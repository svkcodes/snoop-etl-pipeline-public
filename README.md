# Snoop ETL Pipeline

This is a Python-based ETL pipeline designed for processing Open Banking transaction data as part of the Snoop Data Engineer Technical Assessment.

## Features Implemented

-  Ingests JSON files from the local file system
-  Performs validation checks:
  - Validates allowed currencies (`EUR`, `USD`, `GBP`)
  - Detects invalid `transactionDate` values
  - Flags duplicate `transactionId` entries
-  Transforms data by:
  - Removing personally identifiable information (PII)
  - Extracting the most recent transaction date per customer
-  Loads clean data into:
  - `transactions` table with composite primary key (`customerId`, `transactionId`)
  - `customers` table with latest transaction date
-  Logs invalid records into a dedicated `error_log` table
-  Adds metadata fields: `created_at`, `updated_at`
-  Dockerized for easy local setup

## Folder Structure

```bash
snoop_etl_project/
├── etl/                  # Core ETL modules
│   ├── ingest.py
│   ├── validate.py
│   ├── transform.py
│   └── load.py
├── ddl/                  # SQL DDL scripts
│   └── create_tables.sql
├── data/                 # Input JSON files
├── logs/                 # Output logs
├── Dockerfile            # Dockerfile for app container
├── docker-compose.yml    # Runs PostgreSQL + ETL container
├── run_pipeline.py       # Main script
├── requirements.txt
└── .gitignore

## How to Run
  1. Clone the repository
     
     COMMAND : git clone https://github.com/svkcodes/snoop-etl-pipeline-public.git
     COMMAND : cd snoop-etl-pipeline-public

  2. Set up the .env file
     (Shared in the email)

  3. Build and run the pipeline

     COMMAND : docker-compose up --build

     The ETL pipeline will:

      -Ingest data from data/

      -Validate and transform it

      -Insert clean data into PostgreSQL

      -Log invalid records to error_log table

   4. To shut down

     COMMAND : docker-compose down


## Tech Stack
  Python 3.10 (fully compatible with 3.13)

  PostgreSQL

  Docker & Docker Compose

  GitHub Actions (CI/CD config in progress)

## Notes
  The .env file is excluded from Git using .gitignore

  The CI/CD pipeline was configured; however, the service container failed to initialize due to GitHub-hosted runners' environment limitations. With more time, I would containerize the DB layer or mock it for successful GitHub Action validation.