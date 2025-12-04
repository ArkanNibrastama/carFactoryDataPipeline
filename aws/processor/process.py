import os
import json
import boto3
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
from typing import Dict, Any, List
import io
import logging
import time
from urllib.parse import unquote_plus

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Environment Variables ---
RAW_BUCKET = os.environ.get("RAW_BUCKET_NAME")
CLEANED_BUCKET = os.environ.get("CLEANED_BUCKET_NAME")
ERROR_BUCKET = os.environ.get("ERROR_BUCKET_NAME")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")

# --- AWS Clients ---
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")

# --- 1. Schemas for Normalized Data ---



class FactServiceRecordSchema(pa.DataFrameModel):

    service_record_id: Series[str] = pa.Field(nullable=False)

    dealer_id: Series[str] = pa.Field(nullable=False)

    vin: Series[str] = pa.Field(nullable=False)

    customer_id: Series[str] = pa.Field(nullable=True) # Customer might be optional/unknown

    service_datetime_utc: Series[pd.Timestamp] = pa.Field(nullable=False)

    service_datetime_wib: Series[pd.Timestamp] = pa.Field(nullable=False)

    total_cost: Series[float] = pa.Field(ge=0)

    class Config: strict = True; coerce = True



class DimDealerSchema(pa.DataFrameModel):

    dealer_id: Series[str] = pa.Field(nullable=False)

    dealer_name: Series[str] = pa.Field(nullable=True)

    dealer_location: Series[str] = pa.Field(nullable=True)

    class Config: strict = True; coerce = True



class DimVehicleSchema(pa.DataFrameModel):

    vin: Series[str] = pa.Field(nullable=False)

    model: Series[str] = pa.Field(nullable=True)

    year: Series[int] = pa.Field(nullable=True)

    odometer_reading: Series[int] = pa.Field(nullable=False)

    class Config: strict = True; coerce = True



class DimCustomerSchema(pa.DataFrameModel):

    customer_id: Series[str] = pa.Field(nullable=False)

    customer_name: Series[str] = pa.Field(nullable=True)

    class Config: strict = True; coerce = True



class DimServicesPerformedSchema(pa.DataFrameModel):

    service_record_id: Series[str] = pa.Field(nullable=False)

    service_name: Series[str]

    service_cost: Series[float] = pa.Field(ge=0)

    # Added service_code and advisor as they belong to service details

    service_code: Series[str] = pa.Field(nullable=True)

    service_advisor_name: Series[str] = pa.Field(nullable=True) 

    class Config: strict = False; coerce = True 



class DimPartsUsedSchema(pa.DataFrameModel):

    service_record_id: Series[str] = pa.Field(nullable=False)

    part_name: Series[str]

    quantity: Series[int] = pa.Field(ge=1)

    unit_cost: Series[float] = pa.Field(ge=0)

    part_code: Series[str] = pa.Field(nullable=True)

    class Config: strict = False; coerce = True

# --- 2. Transformation and Normalization Function ---

def transform_and_normalize(raw_data: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
    """Normalizes the nested JSON data into Star Schema DataFrames and validates them."""
    
    # 1. Flatten the main data to get all fields available
    flat_df = pd.json_normalize(raw_data)
    
    # --- Timezone Transformation (for Fact Table) ---
    flat_df['service_datetime_utc'] = pd.to_datetime(flat_df['service_details.service_date'], utc=True)
    flat_df['service_datetime_wib'] = flat_df['service_datetime_utc'].dt.tz_convert('Asia/Jakarta')

    # --- Create Dimension: Dealer ---
    dealer_cols = {
        'dealer.dealer_id': 'dealer_id',
        'dealer.dealer_name': 'dealer_name', 
        'dealer.dealer_location': 'dealer_location'
    }
    # Select columns, rename, drop duplicates to get unique dealers
    dim_dealer = flat_df[list(dealer_cols.keys())].rename(columns=dealer_cols).drop_duplicates(subset=['dealer_id'])
    validated_dealer = DimDealerSchema.validate(dim_dealer)

    # --- Create Dimension: Vehicle ---
    vehicle_cols = {
        'vehicle.vin': 'vin',
        'vehicle.model': 'model',
        'vehicle.year': 'year',
        'vehicle.odometer_reading': 'odometer_reading'
    }
    dim_vehicle = flat_df[list(vehicle_cols.keys())].rename(columns=vehicle_cols).drop_duplicates(subset=['vin'])
    validated_vehicle = DimVehicleSchema.validate(dim_vehicle)

    # --- Create Dimension: Customer ---
    customer_cols = {
        'customer.customer_id': 'customer_id',
        'customer.customer_name': 'customer_name'
    }
    dim_customer = flat_df[list(customer_cols.keys())].rename(columns=customer_cols).drop_duplicates(subset=['customer_id'])
    validated_customer = DimCustomerSchema.validate(dim_customer)

    # --- Create Fact Table ---
    fact_cols = {
        'service_record_id': 'service_record_id',
        'dealer.dealer_id': 'dealer_id',
        'vehicle.vin': 'vin',
        'customer.customer_id': 'customer_id',
        'service_details.total_cost': 'total_cost',
        'service_datetime_utc': 'service_datetime_utc',
        'service_datetime_wib': 'service_datetime_wib'
    }
    fact_df = flat_df[list(fact_cols.keys())].rename(columns=fact_cols)
    validated_facts = FactServiceRecordSchema.validate(fact_df)

    # --- Create Dimension: Services Performed ---
    # We need to include 'service_details.service_advisor_name' in the record_path context or meta
    services_df = pd.json_normalize(
        raw_data, 
        record_path=['service_details', 'services_performed'], 
        meta=['service_record_id', ['service_details', 'service_advisor_name']]
    ).rename(columns={'service_details.service_advisor_name': 'service_advisor_name'})
    
    validated_services = DimServicesPerformedSchema.validate(services_df) if not services_df.empty else services_df

    # --- Create Dimension: Parts Used ---
    parts_df = pd.json_normalize(
        raw_data, 
        record_path=['service_details', 'parts_used'], 
        meta=['service_record_id']
    )
    validated_parts = DimPartsUsedSchema.validate(parts_df) if not parts_df.empty else parts_df

    return {
        "fact_service_records": validated_facts,
        "dim_dealer": validated_dealer,
        "dim_vehicle": validated_vehicle,
        "dim_customer": validated_customer,
        "dim_services_performed": validated_services,
        "dim_parts_used": validated_parts
    }

# --- 3. Main Handler for a single S3 file ---

def process_s3_file(source_bucket: str, source_key: str):
    """Processes a single S3 file: download, transform, validate, and route."""
    logging.info(f"Processing file: s3://{source_bucket}/{source_key}")
    try:
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        raw_data_bytes = response['Body'].read()
        raw_data = json.loads(raw_data_bytes.decode('utf-8'))
        
        normalized_dataframes = transform_and_normalize(raw_data)
        logging.info("Validation successful. Writing to cleaned bucket in dual formats.")

        # Write Parquet files for Analytics
        for name, df in normalized_dataframes.items():
            if not df.empty:
                # source_key example: service-records/date=2025-12-01/filename.json
                # We want: parquet_for_analytics/{table_name}/date=2025-12-01/filename.parquet
                relative_path = source_key.replace("service-records/", "", 1)
                parquet_key = f"parquet_for_analytics/{name}/{relative_path}".replace(".json", ".parquet")
                
                with io.BytesIO() as buffer:
                    df.to_parquet(buffer, index=False)
                    s3_client.put_object(Bucket=CLEANED_BUCKET, Key=parquet_key, Body=buffer.getvalue())
                logging.info(f"  - Uploaded Parquet: s3://{CLEANED_BUCKET}/{parquet_key}")

        # Write JSON file for DynamoDB loading
        json_key = source_key.replace("service-records/", "json_for_dynamodb/")
        s3_client.put_object(Bucket=CLEANED_BUCKET, Key=json_key, Body=raw_data_bytes)
        logging.info(f"  - Uploaded JSON: s3://{CLEANED_BUCKET}/{json_key}")

    except (pa.errors.SchemaError, ValueError, KeyError) as e:
        logging.error(f"Validation or transformation failed for s3://{source_bucket}/{source_key}: {e}")
        error_key = source_key.replace("raw/", "error/")
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=ERROR_BUCKET, Key=error_key)
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        logging.info(f"Moved original file to error bucket: s3://{ERROR_BUCKET}/{error_key}")

# --- 4. Main Application Loop ---

if __name__ == '__main__':
    logging.info("Starting Fargate Processor...")
    if not all([RAW_BUCKET, CLEANED_BUCKET, ERROR_BUCKET, SQS_QUEUE_URL]):
        # In a real container, this would cause a crash, which is desired.
        # The container orchestrator (ECS) would report the task as failed.
        raise ValueError("One or more required environment variables are not set.")
    
    logging.info(f"Polling SQS Queue: {SQS_QUEUE_URL}")
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20
            )

            messages = response.get('Messages', [])
            if not messages:
                logging.info("No messages in queue. Waiting...")
                continue

            for message in messages:
                receipt_handle = message['ReceiptHandle']
                try:
                    s3_event = json.loads(message['Body'])
                    source_bucket = s3_event['Records'][0]['s3']['bucket']['name']
                    source_key = unquote_plus(s3_event['Records'][0]['s3']['object']['key'])
                    
                    process_s3_file(source_bucket, source_key)
                    
                    sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
                    logging.info(f"Successfully processed and deleted message for {source_key}")

                except Exception as e:
                    logging.error(f"Failed to process message for {source_key}. It will be retried. Error: {e}")
        
        except Exception as e:
            logging.critical(f"An unexpected error occurred in the main loop: {e}")
            time.sleep(10)