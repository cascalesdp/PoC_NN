# PoC_NN

## Overview

This project implements a scalable and modular data ingestion pipeline designed to manage metadata, raw data ingestion, and structured data storage using Databricks and PySpark.

## Project Structure

* config.py: Global configurations, including Databricks catalogs, schemas, and storage paths.

* ingest_job.py: Main orchestrator script for data ingestion tasks.

* metadata_db.py: Manages metadata databases and tables.

* raw_zone.py: Handles ingestion and management of raw data.

* data_hub.py: Manages structured data storage in a data hub environment, supporting incremental and full loads.

* utils.py: Utility functions.

* requirements.txt: Python dependencies.

* sources.yaml: YAML configuration for source data and ingestion parameters.
