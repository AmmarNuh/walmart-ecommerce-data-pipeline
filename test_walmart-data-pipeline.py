import unittest
import pandas as pd
import os
from pipeline_units.data_pipeline import extract, transform, avg_weekly_sales_per_month, load, validation

# Sample grocery sales DataFrame
grocery_sales = pd.DataFrame({
    'index': [1, 2, 3],
    'Store_ID': [1, 2, 1],
    'Date': ['2021-01-01', '2021-02-01', '2021-03-01'],
    'Weekly_Sales': [15000, 20000, 25000]
})

# Sample extra data DataFrame
extra_data = pd.DataFrame({
    'IsHoliday': [0, 1, 0],
    'Temperature': [70, 65, 75],
    'Fuel_Price': [2.5, 2.7, 2.4],
    'CPI': [100, 102, 101],
    'Unemployment': [5.0, 5.2, 5.1],
    'Dept': [1, 2, 1],
    'Size': [1000, 1500, 1000],
    'Date': ['2021-01-01', '2021-02-01', '2021-03-01']
})

# Save extra_data to a Parquet file for testing
extra_data.to_parquet("extra_data.parquet")

def test_extract():
    merged_df = extract(grocery_sales, "extra_data.parquet")
    assert merged_df.shape[0] == 3

def test_transform():
    merged_df = extract(grocery_sales, "extra_data.parquet")
    clean_data = transform(merged_df)
    assert 'Month' in clean_data.columns
    assert clean_data["Weekly_Sales"].max()>10000  

def test_avg_weekly_sales_per_month():
    merged_df = extract(grocery_sales, "extra_data.parquet")
    clean_data = transform(merged_df)
    agg_data = avg_weekly_sales_per_month(clean_data)
    assert 'Month' in agg_data.columns
    assert 'Weekly_Sales' in agg_data.columns
    assert agg_data.shape[0] == 3  # Should return 3 months

def test_load():
    merged_df = extract(grocery_sales, "extra_data.parquet")
    clean_data = transform(merged_df)
    agg_data = avg_weekly_sales_per_month(clean_data)
    load(clean_data, agg_data)
    
    assert os.path.exists('clean_data.csv')
    assert os.path.exists('agg_data.csv')

