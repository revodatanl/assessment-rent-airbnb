# Project rental-airbnb-assessment

## Overview

This project revolves around extracting, transforming, and loading rental houses and Airbnb rent price data using Databricks. The ETL process follows a Medallion architecture, progressing through bronze, silver, and gold stages. The main objectives include extracting JSON and CSV data, cleaning and transforming it, calculating key performance indicators (KPIs), and automating the workflow.

## Project Structure

The project is organized into several components:

1. **Python Notebooks**:
   - `extract_data.ipynb`: Extracts JSON data from rental houses and Airbnb rent price zip files and loads it into Databricks File System (DBFS).
   - `transform_data.ipynb`: Cleans and transforms the extracted datasets and saves them as Parquet files in DBFS.
   - `KPI_calculation.ipynb`: Calculates KPIs such as potential average revenue per house and per postcode, and adds a column with cumulative count percentage.
   - `paths.ipynb`: Stores variables for different paths used in the project.
   - `utils.ipynb`: common functions used by the main scripts.
   - `exploratory_data_analysis`: exploratory analysis.
   
2. **Resources**:
   - `resource.yaml`: YAML file containing project resources.
   - Three JSON files `job_create.json`,`job_get.json`,`job_reset.json` produced during the creation of the workflow job in Databricks.
  
3. **Pre-commit hooks**:
   - `.pre-commit-config.yaml`: YAML file for pre-commit hooks containing some basic checks.

4. **Tests**:
   - `test_bronze_data_ingestion.py`: Contains tests to validate data at stage bronze (checks for correct file extraction)
   - `test_silver_transformation_extraction.py`: Contains tests to validate data at stage silver (checks for correct file extraction and duplicates)
   - `test_silver_transformation_extraction.py`: Contains tests to validate data at stage gold (checks for correct file extraction and final output existance)

5. **Output**:
   - `data/output` are located two parquet files (`airbnb_final.parquet`,`rent_final.parquet`) as the final output of the pipeline output.

6. **Output**:
   - `docs` folder contains relevant documentation regarding the pipeline process.
    
## Workflow

1. **Data Extraction (Bronze)**:
   - Data is extracted from rental houses and Airbnb rent price zip files.
   - Extracted JSON data is stored in DBFS.

2. **Data Transformation (Silver)**:
   - Extracted data is cleaned and transformed to prepare it for analysis.
   - Transformed data is saved as Parquet files in DBFS.

3. **KPI Calculation (Gold)**:
   - Key performance indicators are calculated, such as potential average revenue per house and per postcode.
   - Cumulative count percentage column is added to the dataset.

4. **Automation**:
   - Workflow is automated using Databricks' job scheduling functionality.
   - Pre-commit hooks are implemented for version control and code quality assurance.

## Usage

1. Clone the repository to your local machine.
2. Set up Databricks environment and configure necessary connections.
3. Execute the notebooks or scripts in the specified order to perform ETL and KPI calculation.
4. Use tests in file `src/tests` to validate the files generated at different stages.
5. Refer to `resources` folder for project resources and configurations.

## Dependencies

- Python
- Databricks environment
- Necessary Python packages (specified in each notebook/script)
