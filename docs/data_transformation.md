# Data Transformation

## Why Perform Data Cleaning?

Data cleaning is one of the most critical steps in building a robust data pipeline. The data we receive from sources like Kamernet and Airbnb is often inconsistent, incomplete, or contains errors. Without proper cleaning, any analysis or aggregation would result in misleading or incorrect insights.

### Key Data Cleaning Steps:

1. **Handling Missing Values:**
   - Missing data is a common issue in real-world datasets. We apply strategies to handle missing values appropriately, such as filling missing postal codes with available geospatial information.
   - **Why This Step?**
     - Missing data can skew results, especially in calculations like average revenue per postcode. Ensuring completeness of essential data fields improves the reliability of the final insights.

2. **Standardizing Data Formats:**
   - Different data sources may use different formats for similar data. For example, rent prices may be expressed as strings or numbers. We standardize these to ensure uniformity.
   - **Why This Step?**
     - Standardization simplifies downstream transformations and analysis, reducing complexity and the risk of errors.

3. **Feature Selection and Engineering:**
   - We select and engineer features that are most relevant for the analysis, such as property types, rent, and room numbers.
   - **Why This Step?**
     - Focusing on the most relevant features improves the efficiency and effectiveness of analysis. Irrelevant data can complicate processing and dilute the focus on what matters most. The chosen features are in this case arbitrary 😁, but can be easily changed in the yaml configuration file.

By performing these transformations, we ensure that the data is in a format suitable for analysis and decision-making.
