# 🛠️ PySpark Data Pipeline for Dmart Analytics

This project implements a modular and scalable data pipeline using **Apache Spark** (PySpark) to process and analyze sales data from **Dmart**. It ingests CSV data from **AWS S3**, performs transformations to answer specific business questions, and persists the results back to S3 or a specified output path.

---

## 📁 Project Structure
.
├── datapipeline.py        # Main pipeline script to orchestrate the entire ETL flow
├── ingest.py              # Handles data ingestion from S3
├── transform.py           # Contains transformation logic to answer analytical questions
├── persist.py             # Writes processed data to S3 (or other storage)
├── pipeline_items.py      # Contains schema definitions and S3 paths
├── resources/
│   ├── configs/
│   │   └── logging.conf   # Logging configuration
│   └── credentials.py     # AWS credentials (access key, secret key, region)


⚙️ Features
🔌 AWS S3 Integration using hadoop-aws connector

📥 Schema-based ingestion of customer, product, and sales data

🔄 Transformations to answer 10 specific business questions

📤 Persistence of transformed data to separate folders (e.g., Q1/, Q2/, ...)

🪵 Structured logging for all steps




📊 Business Questions Answered
Total sales for each product category

Customer with the highest number of purchases

Average discount given on products

Unique products sold per region

Total profit by state

Top sub-category by sales

Average customer age per segment

Orders shipped per shipping mode

Total quantity sold per city

Most profitable customer segment




🚀 Getting Started
1. Install Required Libraries
Ensure you have Python 3.7+ and PySpark installed.
pip install pyspark

If working with AWS S3, also install:
pip install boto3

2. Set Up AWS Credentials
Edit resources/credentials.py:
aws_cred = {
    "access_key": "<your-access-key>",
    "session_key": "<your-secret-key>",
    "region_name": "<your-region>"
}

3. Define Schemas & Paths
In pipeline_items.py, define:

Data schemas (StructType)

S3 CSV paths

Output destination path

4. Run the Pipeline
Run the main file to execute the full ETL pipeline:

python datapipeline.py
📁 Output
Each result is written to the destination_path provided in pipeline_items.py, organized like:

s3://your-bucket/dmart-outputs/
├── Q1/
├── Q2/
├── Q3/
...
Each folder contains a CSV output for that question.

🛡️ Logging
Logging is configured via resources/configs/logging.conf. Logs include:

Data ingestion steps

Transformation steps per question

Write operation status

Error tracking

🧱 Technologies Used
Python 3.x

Apache Spark (PySpark)

AWS S3

Hadoop AWS Connector

Structured Logging

