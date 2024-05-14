import pandas as pd
from datetime import datetime
import requests
from snowflake.snowpark import Session, Row
from snowflake.snowpark.functions import col, lit, to_timestamp, flatten

def get_protocol_data(protocol_slug):
    url = f'https://api.llama.fi/protocol/{protocol_slug}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def model(dbt, session: Session):
    dbt.config(
        materialized="incremental",
        unique_key="defillama_historical_protocol_tvl",
        tags=["100"]
    )

    # Load the bronze table
    bronze_df = dbt.ref("bronze__defillama_protocols").to_pandas()
    
    # Prepare data in chunks
    chunk_size = 10
    chunks = [bronze_df[i:i + chunk_size] for i in range(0, len(bronze_df), chunk_size)]
    
    results = []

    for chunk in chunks:
        for index, row in chunk.iterrows():
            protocol_data = get_protocol_data(row['protocol_slug'])
            if protocol_data:
                for tvl_record in protocol_data['tvl']:
                    results.append(Row(
                        protocol_id=row['protocol_id'],
                        protocol=row['protocol'],
                        protocol_slug=row['protocol_slug'],
                        date=tvl_record['date'],
                        tvl=float(tvl_record['totalLiquidityUSD']),
                        _inserted_timestamp=datetime.utcnow()
                    ))

    if results:
        df = session.create_dataframe(results)
        
        if dbt.is_incremental():
            max_timestamp = df.select(col("protocol_slug"), col("_inserted_timestamp").cast("DATE").alias("max_timestamp")) \
                              .group_by(col("protocol_slug")) \
                              .agg({"max_timestamp": "max"}) \
                              .filter(col("max_timestamp") == datetime.utcnow().date())

            df = df.filter(~df["protocol_slug"].isin(max_timestamp["protocol_slug"]))

        df = df.with_column("defillama_historical_protocol_tvl", lit(dbt.utils.generate_surrogate_key(["protocol_id", "date"])))
        df = df.with_column("inserted_timestamp", lit(datetime.utcnow()))
        df = df.with_column("modified_timestamp", lit(datetime.utcnow()))
        df = df.with_column("_invocation_id", lit(dbt.invocation_id))

        return df
    else:
        return session.create_dataframe([])

