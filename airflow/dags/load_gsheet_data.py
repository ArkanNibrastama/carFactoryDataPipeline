import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from io import BytesIO
from datetime import datetime

GSHEET_ID = "1fwszOXg_BiUuC8AROd8itKPNhSXf70fZKidJTHLJ-oQ"
SHEET_NAME = "Sheet1"
MINIO_BUCKET = "datalake"
S3_ENDPOINT = "http://minio:9001"
S3_KEY = "minioadmin"
S3_SECRET = "minioadmin"

def gsheet_to_minio():
    # 1. Extract CRM data from Google Sheet
    sheet_url = f"https://docs.google.com/spreadsheets/d/{GSHEET_ID}/export?format=csv"
    df = pd.read_csv(sheet_url)

    df_filter = df.loc[df['Tanggal'] == datetime.now().strftime("%d-%m-%Y")]

    # 2. Convert to Parquet
    table = pa.Table.from_pandas(df_filter)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # 3. Upload to MinIO using s3fs
    fs = s3fs.S3FileSystem(
        key=S3_KEY,
        secret=S3_SECRET,
        client_kwargs={'endpoint_url': S3_ENDPOINT}
    )

    datenow = datetime.now().strftime("%Y%m%d")

    with fs.open(f"{MINIO_BUCKET}/CRM/{datenow}.parquet", "wb") as f:
        f.write(buffer.read())

    print(f"Uploaded to MinIO: s3://{MINIO_BUCKET}/CRM/{datenow}.parquet")
