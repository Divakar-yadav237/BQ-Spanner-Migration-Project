# Migration-projects

Migration of a petabyte-scale BigQuery database to Cloud Spanner using Python code deployed via Cloud Run, optimized for cost-efficiency.

## GCP Services Used

1. BigQuery Client API  
2. Spanner Client API  
3. Cloud Run (with Python Function Framework)

## Why Is It Cost-Efficient?

This Python-based solution uses only a few GCP services, making it significantly more cost-effective compared to alternatives like Dataflow or Cloud Composer.

Key optimizations:
- **BigQuery Federated Queries**: Enables querying Spanner tables directly from BigQuery, reducing data movement and cost [1].
- **Batch Processing**: Data is divided into chunks during export to Spanner, allowing efficient handling of petabyte-scale datasets.
- **Error Handling**: Avoids mutation errors in Spanner by using batch operations, which are more reliable for large-scale data ingestion.

## How to use this code
**Create a Spanner Connection ID**
First, create a Spanner connection ID as described in this documentation. This allows Spanner tables to be accessed from BigQuery.

**Write Your Federated Query**
Next, write your BigQuery federated query and specify the column names in the TABLE_CONFIG variable. Refer to this guide for examples of federated queries.

**Modify and Save the Code**
Adjust the Python code according to your requirements and save it locally on your system.

**Deploy to Cloud Run**
Finally, deploy the project to Cloud Run using the following gcloud command. This will run the main.py file using the dependencies listed in requirements.txt.

Links:
[1] https://cloud.google.com/bigquery/docs/federated-queries

[2] https://cloud.google.com/bigquery/docs/connect-to-spanner

[3] https://cloud.google.com/bigquery/docs/federated-queries-intro#work_with_collations_in_external_data_sources

[4] gcloud beta run deploy BQ-Spanner \
      --source . \
      --function BQ-Spanner \
      --base-image python312 \
      --region us-central1 \
      --allow-unauthenticated \
      --update-env-vars PROJECT_ID="us",INSTANCE_ID="dev",DATABASE_ID="developer",CONNECTION_ID="your-connection-id"
