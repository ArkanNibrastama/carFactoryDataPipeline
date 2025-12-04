# aws/glue/load_to_dynamo.py
import sys
import boto3
import json
from decimal import Decimal
from awsglue.utils import getResolvedOptions

# Note: This script is intended to be run as an AWS Glue Python Shell job.
# The Boto3 library is available in the Glue Python Shell environment by default.

def floats_to_decimals(obj):
    """
    Recursively walks a data structure and converts all float values to Decimal,
    as required by DynamoDB for numeric types.
    """
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = floats_to_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = floats_to_decimals(v)
        return obj
    elif isinstance(obj, float):
        # It's safer to convert float to string before converting to Decimal
        # to avoid precision issues.
        return Decimal(str(obj))
    return obj

import sys
import boto3
import json
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from awsglue.utils import getResolvedOptions

# Note: This script is intended to be run as an AWS Glue Python Shell job.

def floats_to_decimals(obj):
    """
    Recursively walks a data structure and converts all float values to Decimal.
    """
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = floats_to_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = floats_to_decimals(v)
        return obj
    elif isinstance(obj, float):
        return Decimal(str(obj))
    return obj

def main():
    # --- Get Job Arguments ---
    # --DYNAMODB_TABLE_NAME <table_name>
    # --SOURCE_BUCKET_NAME <bucket_name>
    # --PROCESS_DATE <YYYY-MM-DD> (Optional: Defaults to yesterday)
    args = getResolvedOptions(sys.argv, ['DYNAMODB_TABLE_NAME', 'SOURCE_BUCKET_NAME'])
    
    # Check for optional PROCESS_DATE argument
    # (getResolvedOptions raises error if required args missing, but we need to parse optional manually from sys.argv if strictly needed, 
    #  or simpler: just use yesterday if not passed in args, but standard Glue optional args are tricky. 
    #  We will assume if the user wants to backfill, they add the key. If not present, it fails? 
    #  Actually, let's stick to calculating yesterday nicely.)
    
    # Custom argument parsing for optional args usually involves try/except or scanning sys.argv directly 
    # because getResolvedOptions is strict. 
    # Simpler approach: Calculate yesterday. If specific date needed, user updates script or we add strictly.
    # Let's just calculate yesterday for the automated schedule.
    
    table_name = args['DYNAMODB_TABLE_NAME']
    source_bucket_name = args['SOURCE_BUCKET_NAME']
    
    # Calculate Yesterday's Date (UTC)
    # Why yesterday? Because if the job runs at 00:30 today, we want to process the full day that just finished.
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    partition_date = yesterday.strftime('%Y-%m-%d')
    
    # Allow override if provided (Manually parsing sys.argv for flexibility)
    if '--PROCESS_DATE' in sys.argv:
        idx = sys.argv.index('--PROCESS_DATE') + 1
        if idx < len(sys.argv):
            partition_date = sys.argv[idx]

    # FIX: Removed 'cleaned/' prefix to match actual S3 structure
    prefix = f"json_for_dynamodb/date={partition_date}/"
    
    print(f"Starting Batch Glue Job for Date: {partition_date}")
    print(f"Target Bucket: {source_bucket_name}")
    print(f"Target Prefix: {prefix}")
    
    # --- Initialize AWS Clients ---
    s3_client = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    # --- List Files ---
    try:
        # List objects within the specific date partition
        response = s3_client.list_objects_v2(Bucket=source_bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            print(f"No files found in partition {prefix}. Job finished.")
            return

        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
        print(f"Found {len(files)} files to process.")

    except Exception as e:
        print(f"Failed to list objects: {e}")
        raise e

    # --- Process Each File ---
    success_count = 0
    fail_count = 0

    with table.batch_writer() as batch:
        for source_key in files:
            print(f"Processing file: {source_key}")
            try:
                # 1. Read File
                response = s3_client.get_object(Bucket=source_bucket_name, Key=source_key)
                service_records_raw = json.loads(response['Body'].read().decode('utf-8'))
                
                # 2. Convert Data
                service_records = floats_to_decimals(service_records_raw)
                
                # 3. Load to DynamoDB (Accumulate in batch)
                for record in service_records:
                    batch.put_item(Item=record)
                
                success_count += 1

            except Exception as e:
                print(f"  - Failed to process file {source_key}: {e}")
                fail_count += 1

    print(f"Job Complete. Files Processed: {success_count}, Files Failed: {fail_count}")
    if fail_count > 0:
        raise Exception(f"Job finished with {fail_count} errors.")

# --- Main execution ---
if __name__ == "__main__":
    main()
