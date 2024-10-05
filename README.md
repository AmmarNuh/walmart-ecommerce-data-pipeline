# Walmart E-commerce Data Pipeline

## Description
This project implements a data pipeline for analyzing supply and demand around holidays using Walmart's grocery sales data and complementary data. The pipeline includes functions for data extraction, transformation, aggregation, and loading. Additionally, it provides visualizations to showcase insights from the data.

## Features
- Extracts data from a PostgreSQL database and a Parquet file.
- Cleans and transforms the data, filling missing values and adding new columns.
- Aggregates average weekly sales per month.
- Saves cleaned and aggregated data to CSV files.
- Validates the existence of the saved files.
- Unit tests to ensure the functionality of the pipeline.

# Code Process and Workflow
## Overview
The data pipeline follows a structured process that includes the following stages:

- Extract: Load the main grocery sales DataFrame and additional data from a Parquet file.
- Transform: Clean and preprocess the data; fill missing values and create new columns.
- Aggregate: Calculate average weekly sales per month.
- Load: Save the cleaned and aggregated data to CSV files.
- Validate: Check if the CSV files have been successfully created.
- Visualize: Generate visualizations to present key insights.


## Code Structure

The project consists of the following files:

1. **pipeline_units/**: Contains the core functions for the data pipeline.
   - `walmart-data-pipeline.py`
2. **main_pipeline.py**: Executes the data pipeline and showcases visualizations.
3. **test/**: Contains unit tests for the data pipeline functions.
   - `test_walmart-data-pipeline.py`
4. **README.md**: Documentation for the project.

## Usage
1. Place your grocery_sales DataFrame and extra_data.parquet file in the project directory.
2. Run the pipeline:
 ```bash
   pip walmart-data-pipeline.py
```

## Running Tests
To run the unit tests, use the following command:
 ```bash
  python -m unittest discover -s test
```


## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/AmmarNuh/walmart-ecommerce-data-pipeline
   cd walmart-ecommerce-data-pipeline
   ```

2. Install dependencies:
   ```bash
   pip install pandas matplotlib
   ```
