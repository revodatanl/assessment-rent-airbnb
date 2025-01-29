# Amsterdam Rental Data Pipeline

## Overview
This data pipeline analyzes rental data from Kamernet and Airbnb to identify suitable postal codes for property investment in Amsterdam. It processes raw data stored in zip files, applies necessary transformations, and outputs average revenue per postcode for both datasets.

## Why This Project?
The project was created to automate the process of analyzing rental data, providing property investors with a reliable and scalable method for identifying high-potential investment areas in Amsterdam. The pipeline leverages the Medallion Architecture to ensure the data is processed in stages, providing a clean and aggregated dataset for decision-making.

## Key Features
- **Data Ingestion:** Ingests raw rental data from zip files storing CSV and JSON files.
- **Data Cleaning:** Cleans and standardizes data, including handling inconsistencies and missing zipcodes for the Airbnb data.
- **Aggregation:** Computes average revenue per postcode, broken down by long-term (Kamernet) and short-term (Airbnb) rental data.
- **Scalable and Efficient:** Uses PySpark for processing large datasets and stores results in Parquet format for fast access.

## Pipeline Flow
1. **Ingest Raw Data:** Collects data from Kamernet and Airbnb.
2. **Clean the Data:** Performs data cleaning for both datasets to ensure consistency.
3. **Transform & Aggregate:** Calculates average revenue per postcode.
4. **Output:** The results of every layer (bronze, silver, gold) are saved in Parquet format and used for further analysis.

## Why Use This Pipeline?
- **Efficiency:** Automates data processing, saving time.
- **Scalability:** Can handle larger datasets in the future.
- **Insights:** Provides actionable insights for property investors in The Netherlands.
