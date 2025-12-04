import os
import json
import boto3
import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, Field
from mangum import Mangum

# --- Configuration ---
# The S3 bucket name is expected to be set as an environment variable in the Lambda function
RAW_BUCKET_NAME = os.environ.get("RAW_BUCKET_NAME")

# --- Pydantic Models for Data Validation ---
# These models enforce the structure of the incoming API data.

class Dealer(BaseModel):
    dealer_id: str
    dealer_name: str
    dealer_location: str

class Vehicle(BaseModel):
    vin: str
    model: str
    year: int
    odometer_reading: int

class ServicePerformed(BaseModel):
    service_code: Optional[str] = None
    service_name: str
    service_cost: float
    technician_notes: Optional[str] = None

class PartUsed(BaseModel):
    part_code: Optional[str] = None
    part_name: str
    quantity: int
    unit_cost: float

class ServiceDetails(BaseModel):
    service_date: datetime
    service_advisor_name: str
    total_cost: float
    services_performed: List[ServicePerformed] = []
    parts_used: List[PartUsed] = []

class Customer(BaseModel):
    customer_id: str
    customer_name: str

class ServiceRecord(BaseModel):
    service_record_id: str
    dealer: Dealer
    vehicle: Vehicle
    service_details: ServiceDetails
    customer: Customer

# --- FastAPI Application ---

app = FastAPI(title="Car Service Data Ingestion API")
s3_client = boto3.client("s3")

@app.post("/service-records", status_code=202)
async def create_service_records(records: List[ServiceRecord]):
    """
    Receives a batch of service records, validates them, and uploads them
    as a single JSON file to the raw S3 bucket.
    """
    if not RAW_BUCKET_NAME:
        raise HTTPException(status_code=500, detail="RAW_BUCKET_NAME environment variable not set.")

    # Generate a unique file key using Hive-style partitioning
    now = datetime.utcnow()
    date_partition = now.strftime('date=%Y-%m-%d')
    time_uuid = f"{now.strftime('%H-%M-%S')}_{str(uuid.uuid4())}"
    file_key = f"service-records/{date_partition}/{time_uuid}.json"

    # Convert Pydantic models back to a list of dicts
    records_as_dict = [record.model_dump(mode='json') for record in records]
    
    # Serialize the list of dicts to a JSON string
    json_payload = json.dumps(records_as_dict, indent=2)

    try:
        # Upload the JSON file to S3
        s3_client.put_object(
            Bucket=RAW_BUCKET_NAME,
            Key=file_key,
            Body=json_payload,
            ContentType="application/json"
        )
    except Exception as e:
        # In a real-world app, you'd add more specific error logging here
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")

    return {
        "status": "accepted",
        "file_key": file_key,
        "record_count": len(records)
    }

# --- Mangum Handler ---
# This handler adapts the FastAPI app to be executed by AWS Lambda
handler = Mangum(app)
