# Project Architecture

## Why Use the Medallion Architecture?

The **Medallion Architecture** is a layered approach to building data pipelines, particularly suited for large-scale data processing and analytics. This project leverages this architecture to ensure a clean, organized, and efficient flow of data, which is key for providing accurate and insightful results for property investors.

### Three Layers of the Medallion Architecture:

1. **Bronze Layer (Raw Data):**
   - This layer contains raw, unprocessed data; rental listings from Kamernet and Airbnb.
   - **Why This Layer?**
     - The Bronze layer serves as the foundation of the data pipeline. It stores data in its original form so that it can be revisited if needed for validation or troubleshooting. Keeping this raw data ensures that we don’t lose any information in case of issues during transformations.
   
2. **Silver Layer (Cleaned Data):**
   - The Silver layer focuses on cleaning and transforming the data. This includes handling missing values, standardizing formats, and performing any necessary data manipulation.
   - **Why This Layer?**
     - This layer prepares the data for analysis. Cleaned data ensures that downstream processes (like aggregating revenues) are based on high-quality information. Without a Silver layer, the final results could be inaccurate due to inconsistencies in the raw data.

3. **Gold Layer (Aggregated Insights):**
   - The Gold layer contains the final output of the pipeline; the aggregated insights of the average revenue per postcode.
   - **Why This Layer?**
     - The Gold layer is where the most valuable insights are produced. This aggregated data provides the information needed for decision-making (in this case, for property investors). This layer is optimized for fast querying and efficient storage, ensuring that stakeholders can access the results quickly.

### Why This Architecture?

The Medallion Architecture ensures that data is processed in stages, each of which adds value. By separating raw data from clean data and final insights, it allows for better error handling, transparency, and scalability. This architecture also supports easier maintenance, as each layer can be updated or rebuilt independently if necessary.
