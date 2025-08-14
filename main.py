"""Spanner update serial number"""
import os
import logging
from google.cloud import bigquery, spanner
from flask import Request, jsonify

logging.basicConfig(level=logging.INFO)
PROJECT_ID= os.getenv('PROJECT_ID', "us")
INSTANCE_ID = os.getenv('INSTANCE_ID', "dev")
DATABASE_ID = os.getenv('DATABASE_ID', "developer")
CONNECTION_ID = os.getenv('CONNECTION_ID', "your-connection-id")

# Initialize clients
bigquery_client = bigquery.Client()
spanner_client = spanner.Client()

# Chunk size for inserts
BATCH_SIZE = 5000

# Table configuration (query + column mapping)
TABLE_CONFIG = {
    "Your_Table_name_1": {
        "query": """
           SELECT Name,Country_code,Recordstamp FROM EXTERNAL_QUERY("{CONNECTION_ID}", "select * from operations_table where flag = 'Y'")
        """,
        "columns": ["Name", "Country_code", "Recordstamp"]
    },
    "Your_Table_name_2": {
        "query": """
        SELECT Name,Country_code,Recordstamp FROM EXTERNAL_QUERY("{CONNECTION_ID}", "select * from operations_table where flag = 'Y'")
  """,
  "columns": ["Name", "Country_code", "Recordstamp"]
    },
    
#You can add many more tables

}

# Helper function to split rows into chunks
def chunked(iterable, chunk_size):
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i + chunk_size]

# Perform batched inserts into Spanner
def insert_rows_with_batch(database, table_name, columns, rows_to_insert):
    if not rows_to_insert:
        logging.info(f" No data to insert into {table_name}")
        return

    chunks = list(chunked(rows_to_insert, BATCH_SIZE))
    total_chunks = len(chunks)
    logging.info(f" Inserting into Spanner: {total_chunks} batches of up to {BATCH_SIZE} rows for table '{table_name}'.")

    for batch_num, chunk in enumerate(chunks, start=1):
        try:
            with database.batch() as batch:
                batch.insert_or_update(
                    table=table_name,
                    columns=columns,
                    values=chunk
                )
            logging.info(f" Batch {batch_num}/{total_chunks} inserted successfully ({len(chunk)} rows).")
        except Exception as e:
            logging.error(f" Error in batch {batch_num}/{total_chunks} for table {table_name}: {str(e)}")

# Process one table: fetch from BigQuery and insert into Spanner
def process_table_with_batch(table_name, config):
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)

    query = config["query"]
    columns = config["columns"]

    logging.info(f" Starting sync for table: {table_name}")

    try:
        query_job = bigquery_client.query(query)
        rows_to_insert = [tuple(row[col] for col in columns) for row in query_job]
    except Exception as e:
        error_msg = f" Failed to query BigQuery for table {table_name}: {str(e)}"
        logging.error(error_msg)
        return error_msg

    if not rows_to_insert:
        msg = f" No data returned from BigQuery for table: {table_name}"
        logging.warning(msg)
        return msg

    insert_rows_with_batch(database, table_name, columns, rows_to_insert)

    msg = f" Total inserted into {table_name}: {len(rows_to_insert)} rows"
    logging.info(msg)
    return msg

# Main entry point
def main(request: Request):
    try:
        results = {}

        for table_name, config in TABLE_CONFIG.items():
            try:
                result = process_table_with_batch(table_name, config)
                results[table_name] = {"status": "success", "message": result}
            except Exception as e:
                error_msg = f" Failed to sync table {table_name}: {str(e)}"
                logging.error(error_msg)
                results[table_name] = {"status": "error", "message": error_msg}
                break

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return f"An error occurred {e}"
main("request")
