# Documentation for Project rental-airbnb-assessment

## Introduction
This project aims to the development of an pipeline in databricks for potential house revenue based on kamernet rental and airbnb data.Additionally the automation using databricks workflows.


## Workflow
- Detailed explanation of the ETL process:
  - `.pre-commit-config.yaml` script is added with simple simple checks before the individual commits. 
  - `paths` script is created in `src` file to store the different file paths into variables to avoid using hardcoded values in the various processing and testing scripts. in case a change has to be made it can implemented in one location and instead of replacing in each operation that this file path is used.
  - Data Extraction (Bronze): Steps involved in extracting data from source files and storing it in DBFS storage. In this stage the data is being extracted from the zip files as json and csv files for rental and airbnb data repsectively. The data are on they raw form.For data storage the `DBFS` storage system was chosen due to the simplicity and cost effectivess in comparison to the delta lake or the convention database tables.

  - Data Transformation (Silver): Details on cleansing and transforming the data to prepare it for analysis.In this stage the cleansing process takes place such as dropping duplicates,fixing data types, removing records with invalid zipcodes and converting fields to the appropriate format for further analysis.Extensive usage of the `utils` scirpt is beign done for data transformation. Common used functions are placed there for higher modularity and easier debuging in case of an error. Needs to be mentioned that for airbnb data the grammatical part of the zipcodes (normal `zipcode 1111RS --> modified  1111`) has been removed since a lot of them were missing. The solution is better than the alternatives.However a possible drawback is that the accuracy of the calculation is lower in regard to the area specification.
    - Alternatives:
      - 1. Removing all the zipcodes that the letters are missing would cause significant data loss
      - 2. calculate the KPIs with a mix of complete and incomplete codes would produce inaccurate results.

    - After the the transformation data is stored in DBFS as parquet files. At this stage the files can be used for various puproses and analysis.
    
  - KPI Calculation (Gold): Only used fields that are necessary for this specific KPI calculation, the output is two tables with the average house revenue per house per zipcode. An additional field has been added presenting the cummulative count distribution for better desicion making (utilization of pareto distribution theory possibly). It might be the case that the average revenue is significantly high for certain zipcodes but this value is based only in a small amount of records which is not representative of the actual figures.For instance, if the calculation is derived based only in one house that is present in the specific area the revenue may be high but not representing the real value since the the sample is too small. The final files are stored both in DBFS (gold level) as parquet files and in the following folder `resources`
  
  - Automation: The pipeline can run in a databricks workflow based on the files placed in `resources`

## Exploratory analysis
- Several finding about rent prices can be found after an exploratory analysis of the data.

## Test
- Simple test have been added to check the file extractionand transformation in the 3 different stages using the unittest python library.


## Output
- 2 parqet files `data/output/airbnb_final.parquet` and `data/output/rent_final.parquet`

## Documentation
- Additional `README.md` has been added for this project.