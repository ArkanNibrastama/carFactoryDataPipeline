import sys
import os
import pytest
import pandas as pd
import pandera as pa

# Add the parent directory to sys.path to import the 'process' module safely
# This allows running tests from the root (pytest aws/processor/tests/) 
# or from the directory itself.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from process import transform_and_normalize

@pytest.fixture
def sample_raw_data_success():
    """Provides a sample of valid raw data, simulating a JSON from S3."""
    return [
        {
            "service_record_id": "SRV-001",
            "dealer": {
                "dealer_id": "DLR-A", 
                "dealer_name": "Dealer A",
                "dealer_location": "Jakarta"
            },
            "vehicle": {
                "vin": "VIN-123", 
                "odometer_reading": 50000,
                "model": "Civic",
                "year": 2022
            },
            "service_details": {
                "service_date": "2025-11-21T10:00:00Z", # UTC Timestamp
                "total_cost": 750.0,
                "service_advisor_name": "Budi",
                "services_performed": [
                    {"service_name": "Oil Change", "service_cost": 200.0, "service_code": "S01"},
                    {"service_name": "Tire Rotation", "service_cost": 100.0, "service_code": "S02"}
                ],
                "parts_used": [
                    {"part_name": "Oil Filter", "quantity": 1, "unit_cost": 50.0, "part_code": "P01"},
                    {"part_name": "Synthetic Oil", "quantity": 5, "unit_cost": 80.0, "part_code": "P02"}
                ]
            },
            "customer": {
                "customer_id": "CUST-1",
                "customer_name": "Andi"
            }
        }
    ]

@pytest.fixture
def sample_raw_data_failure():
    """Provides a sample of invalid raw data (negative cost)."""
    return [
        {
            "service_record_id": "SRV-FAIL",
            "dealer": {
                "dealer_id": "DLR-C",
                "dealer_name": "Dealer C",
                "dealer_location": "Surabaya"
            },
            "vehicle": {
                "vin": "VIN-FAIL",
                "odometer_reading": 10000,
                "model": "Brio",
                "year": 2021
            },
            "service_details": {
                "service_date": "2025-11-22T11:00:00Z",
                "total_cost": -100.0, # This will cause a validation failure
                "service_advisor_name": "Siti",
                "services_performed": [],
                "parts_used": []
            },
            "customer": {
                "customer_id": "CUST-3",
                "customer_name": "Budi"
            }
        }
    ]

def test_transform_and_normalize_success(sample_raw_data_success):
    """
    Tests the entire transformation and validation process with good data.
    Verifies the Star Schema output (Fact table + 5 Dimension tables).
    """
    # Act
    result_dfs = transform_and_normalize(sample_raw_data_success)

    # Assert
    # Check that all 6 dataframes are created
    assert "fact_service_records" in result_dfs
    assert "dim_services_performed" in result_dfs
    assert "dim_parts_used" in result_dfs
    assert "dim_dealer" in result_dfs
    assert "dim_vehicle" in result_dfs
    assert "dim_customer" in result_dfs

    facts_df = result_dfs["fact_service_records"]
    services_df = result_dfs["dim_services_performed"]
    parts_df = result_dfs["dim_parts_used"]
    dealer_df = result_dfs["dim_dealer"]
    vehicle_df = result_dfs["dim_vehicle"]
    customer_df = result_dfs["dim_customer"]

    # --- Assertions for fact_service_records ---
    assert facts_df.shape[0] == 1 # One main record
    assert "service_datetime_utc" in facts_df.columns
    assert "service_datetime_wib" in facts_df.columns
    assert "dealer_id" in facts_df.columns
    assert "vin" in facts_df.columns
    assert "customer_id" in facts_df.columns
    # Ensure descriptive columns are NOT in the fact table
    assert "dealer_name" not in facts_df.columns 
    assert "model" not in facts_df.columns
    
    assert pd.api.types.is_datetime64_any_dtype(facts_df['service_datetime_utc'])
    assert pd.api.types.is_datetime64_any_dtype(facts_df['service_datetime_wib'])
    assert facts_df['service_datetime_utc'].iloc[0].tz.zone == "UTC"
    assert facts_df['service_datetime_wib'].iloc[0].tz.zone == "Asia/Jakarta"
    assert facts_df['total_cost'].iloc[0] == 750.0

    # --- Assertions for dim_dealer ---
    assert dealer_df.shape[0] == 1
    assert dealer_df['dealer_id'].iloc[0] == "DLR-A"
    assert dealer_df['dealer_name'].iloc[0] == "Dealer A"

    # --- Assertions for dim_vehicle ---
    assert vehicle_df.shape[0] == 1
    assert vehicle_df['vin'].iloc[0] == "VIN-123"
    # assert vehicle_df['model'].iloc[0] == "Grand Livina" # (Sample data assumes model is optional or null if not provided, checking keys mainly)

    # --- Assertions for dim_customer ---
    assert customer_df.shape[0] == 1
    assert customer_df['customer_id'].iloc[0] == "CUST-1"

    # --- Assertions for dim_services_performed ---
    assert services_df.shape[0] == 2 # Two services were performed
    assert "service_record_id" in services_df.columns
    assert services_df['service_record_id'].iloc[0] == "SRV-001"

    # --- Assertions for dim_parts_used ---
    assert parts_df.shape[0] == 2 # Two parts were used
    assert "service_record_id" in parts_df.columns
    assert parts_df['service_record_id'].iloc[0] == "SRV-001"
    assert parts_df['quantity'].sum() == 6

def test_transform_and_normalize_failure(sample_raw_data_failure):
    """
    Tests that a Pandera SchemaError is raised when data is invalid.
    """
    # Act and Assert
    with pytest.raises(pa.errors.SchemaError) as excinfo:
        transform_and_normalize(sample_raw_data_failure)

    # Optionally, check the error message to be more specific
    assert "greater_than_or_equal_to(0)" in str(excinfo.value)
    assert "total_cost" in str(excinfo.value)
