![Screenshot 2024-11-09 at 14 41 36](https://github.com/user-attachments/assets/e2c654d1-a2d6-46a1-9c96-7fda08256711)


Movie Data ETL Pipeline using AWS

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline for analyzing movie data using AWS services. The pipeline extracts data from an S3 bucket, performs transformations and data quality checks with AWS Glue, and loads filtered data into an Amazon Redshift table. Additionally, the pipeline handles bad records and triggers notifications through Amazon SNS based on the job's success or failure.

Project Overview

The goal of this ETL pipeline is to:

Extract movie data from a flat file stored in an Amazon S3 bucket.
Transform the data to filter movies with ratings between 8.5 and 10, perform data quality checks, and return invalid records to S3.
Load the transformed data into an Amazon Redshift table for analytics.


Workflow

Source Data in S3: The source data (movie data) is stored in a flat file format in an S3 bucket.
AWS Glue Crawler: A Glue Crawler runs on the source data in S3 to detect and create metadata in the Glue Data Catalog.
Data Transformation with Glue ETL:
An AWS Glue job performs data transformations, including filtering movies with ratings between 8.5 and 10.
Invalid records are returned to a specified location in the S3 bucket.
Data Quality Check: AWS Glue Data Quality performs checks to ensure data meets defined standards.
Data Loading to Redshift: The transformed data is loaded into a Redshift table (imdb_movies_rating) within the movies schema.
EventBridge and CloudWatch Monitoring: AWS EventBridge is configured to monitor the Glue job's status (success or failure).
Notifications with SNS: Amazon SNS sends notifications based on the job status, alerting about job success or failure.


AWS Services Used

Amazon S3: Storage for source and bad records data.
AWS Glue: ETL service for data transformation and metadata cataloging.
AWS Glue Data Catalog: Metadata repository for the data schema.
Amazon Redshift: Data warehouse for storing transformed data.
AWS EventBridge: Event monitoring for Glue job status.
Amazon CloudWatch: Monitoring and logging for Glue and Redshift.
Amazon SNS: Notification service for alerting job status.
Prerequisites
An AWS account with access to S3, Glue, Redshift, EventBridge, CloudWatch, and SNS.
AWS CLI and configured credentials.
Basic knowledge of SQL and Python (for optional custom transformations).


Setup Instructions

S3 Setup:

Upload the movie data file to an S3 bucket.
Create a folder for storing bad records if needed.
Redshift Setup:

Create a Redshift cluster and configure a schema (movies) and table (imdb_movies_rating) to store the data.
Expose the port for all incoming traffic and set up VPC endpoints for S3, Glue, and CloudWatch.
AWS Glue Setup:

Create a Glue Crawler for the source data in S3 and another for the destination Redshift table.
Define an ETL job in AWS Glue with transformations and data quality checks.
Use Glue Data Catalog for metadata management.
EventBridge and SNS Setup:

Configure an EventBridge rule to trigger on Glue job success/failure.
Set up SNS notifications to receive alerts.




How It Works

Data Ingestion: Data is ingested from the source file in S3.
Data Transformation and Quality Checks:
The Glue job filters out records with ratings between 8.5 and 10.
Data quality checks are performed, and any bad records are sent back to the designated S3 location.
Data Loading: Filtered and validated data is loaded into the Redshift table.
Monitoring and Alerting:
EventBridge monitors job status and triggers alerts via SNS.
CloudWatch provides logs and monitoring for the entire process.
