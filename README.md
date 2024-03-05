# Snowflake Snowpipe Pipeline Project

This repository contains a Snowflake pipeline project that leverages Snowpipe, tasks, and streams to handle data ingestion, transformation, and loading. Below are the setup instructions:

## Architecture

![Project Architecture]()

## Prerequisites

Before running the script, ensure the following prerequisites are met:

1. **AWS Role and Access Key:**
   - Create a role in AWS with the necessary permissions.
   - Download the AWS access key.

2. **S3 Bucket and Event Notification:**
   - Create an S3 bucket.
   - Enable event notification on the S3 bucket.
   - Copy the SQS ARN of Snowpipe to the S3 bucket event notification settings.

## Setup

1. **Database and Schemas:**
   - Create a Snowflake database named `DB1`.
   - Create a schema named `TS1` within the `DB1` database.

2. **External Stage:**
   - Create an external stage named `S3_STAGE` with your S3 bucket credentials.

3. **Initial Table:**
   - Create a table named `PERSON_NESTED` within the `TS1` schema.

4. **JSON File Format:**
   - Create a file format named `JSON` of type JSON and compression AUTO.

5. **Snowpipe:**
   - Create a Snowpipe named `PERSON_PIPE` with auto_ingest enabled.
   - Point the Snowpipe to the `PERSON_NESTED` table and use the `JSON` file format.
   - Refer to the code available in the repo for the detailed Snowpipe setup.

6. **Stream and Target Tables:**
   - Create a stream named `PERSON_NESTED_STREAM` on the `PERSON_NESTED` table.
   - Create target tables `PERSON_MASTER` and `PERSON_LOCATION` within the `TS1` schema.

7. **Procedure and Task:**
   - Create a stored procedure named `PERSON_PROC` within the `TS1` schema.
   - Create a task named `PERSON_TASK1` to schedule the procedure when the stream has data.

8. **Run Task:**
   - Suspend the task using `ALTER TASK DB1.TS1.PERSON_TASK1 SUSPEND` for testing.

## Test the Pipeline

1. **Check Pipeline Status:**
   - Execute `SELECT system$pipe_status('DB1.TS1.PERSON_PIPE');`.

2. **Check Contents of Target Tables:**
   - Execute `SELECT * FROM DB1.TS1.PERSON_MASTER;`, `SELECT * FROM DB1.TS1.PERSON_LOCATION;`, and `SELECT * FROM DB1.TS1.PERSON_NESTED_STREAM;`.

3. **File for Testing:**
   - Use the file `person_intl_1.json` for testing Snowpipe functionality.
   - Upload the rest of the files to the S3 bucket.
   - Wait for a minute and check if the data has been ingested into the target tables.
     

Feel free to refer to the code available in the repository for detailed implementation steps.
