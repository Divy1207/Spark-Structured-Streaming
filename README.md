# Spark-Structured-Streaming

### Spark-Structured-Streaming Project
This project demonstrates Apache Spark Structured Streaming for real-time and batch data processing, leveraging different output modes and Databricks Autoloader for automatic data ingestion.

### Features
Complete Mode: Processes and writes the entire dataset with each micro-batch.
Update Mode: Writes only the updated rows since the last processing.
Append Mode (Batch Mode): Writes newly appended rows.
Databricks Autoloader: Automates ingestion of both batch and streaming data from cloud storage like AWS S3, Azure Blob, etc.

### Prerequisites
Apache Spark 3.x
Databricks environment or local Spark setup
Python or Scala development environment
