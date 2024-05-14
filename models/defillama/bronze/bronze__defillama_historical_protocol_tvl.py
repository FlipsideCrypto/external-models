import pandas as pd
from datetime import datetime
import requests
import time
from snowflake.snowpark import Session, Row
from snowflake.snowpark.types import StructType, StructField, TimestampType, StringType
import logging

logger = logging.getLogger("python_logger")

def get_protocol_data(protocol_slug):
    url = f'https://api.llama.fi/protocol/{protocol_slug}'
    retries = 2
    backoff_factor = 0.5
    
    for i in range(retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logger.info(f"Successfully fetched data for protocol_slug: {protocol_slug}")
                return response.json()
            else:
                response.raise_for_status()
        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
            if i == retries - 1:
                raise
            else:
                wait_time = backoff_factor * (2 ** i)
                time.sleep(wait_time)
                continue

def model(dbt, session: Session):
    dbt.config(
        materialized="incremental",
        unique_key="defillama_historical_protocol_tvl",
        tags=["100"],
        python_version="3.11",  # Specify the Python runtime version
        packages=["requests", "pandas", "snowflake-snowpark-python"]
    )

    # Load the bronze table and get distinct protocol_slug
    bronze_df = dbt.ref("bronze__defillama_protocols").to_pandas()
    bronze_df = bronze_df.head(50)

    # Ensure the DataFrame contains the expected columns
    if 'PROTOCOL_SLUG' not in bronze_df.columns:
        raise ValueError("Column 'protocol_slug' not found in the DataFrame")

    # Get distinct protocol_slug
    protocol_slugs = bronze_df['PROTOCOL_SLUG'].drop_duplicates().tolist()

    results = []

    # Loop over the protocol_slugs and fetch data
    for protocol_slug in protocol_slugs:
        try:
            protocol_data = get_protocol_data(protocol_slug)
            if protocol_data:
                results.append(Row(
                    _inserted_timestamp=datetime.utcnow(),
                    json_data=str(protocol_data)  # Convert JSON object to string for Snowpark
                ))
        except Exception as e:
            # Continue to the next protocol_slug if an error occurs
            continue

    # Create a DataFrame from the results
    if results:
        df = session.create_dataframe(results)
    else:
        # Return an empty DataFrame with the correct schema
        schema = StructType([
            StructField("_inserted_timestamp", TimestampType(), True),
            StructField("json_data", VariantType(), True)  # Use VariantType for JSON
        ])
        df = session.create_dataframe([], schema=schema)

    return df