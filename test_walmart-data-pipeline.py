import unittest
import pandas as pd
import os
from pipeline_units import extract, transform, avg_weekly_sales_per_month, load
#from pipeline_units import validation


class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        self.grocery_sales = pd.DataFrame({
            'index': [1, 2, 3],
            'Store_ID': [1, 2, 1],
            'Date': ['2021-01-01', '2021-02-01', '2021-03-01'],
            'Weekly_Sales': [15000, 20000, 25000]
        })

        self.extra_data = pd.DataFrame({
            'IsHoliday': [0, 1, 0],
            'Temperature': [70, 65, 75],
            'Fuel_Price': [2.5, 2.7, 2.4],
            'CPI': [100, 102, 101],
            'Unemployment': [5.0, 5.2, 5.1],
            'Dept': [1, 2, 1],
            'Size': [1000, 1500, 1000],
            'Date': ['2021-01-01', '2021-02-01', '2021-03-01']
        })

    def test_extract(self):
        merged_df = extract(self.grocery_sales, "extra_data.parquet")
        self.assertEqual(merged_df.shape[0], 3)

    def test_transform(self):
        merged_df = extract(self.grocery_sales, "extra_data.parquet")
        clean_data = transform(merged_df)
        self.assertTrue('Month' in clean_data.columns)
        self.assertEqual(clean_data.shape[0], 3)  # Assuming no filtering applied for sales > 10,000

    def test_avg_weekly_sales_per_month(self):
        merged_df = extract(self.grocery_sales, "extra_data.parquet")
        clean_data = transform(merged_df)
        agg_data = avg_weekly_sales_per_month(clean_data)
        self.assertTrue('Month' in agg_data.columns)
        self.assertTrue('Weekly_Sales' in agg_data.columns)
        self.assertEqual(agg_data.shape[0], 3)  # Should return 3 months

    def test_load(self):
        merged_df = extract(self.grocery_sales, "extra_data.parquet")
        clean_data = transform(merged_df)
        agg_data = avg_weekly_sales_per_month(clean_data)
        load(clean_data, agg_data)
        self.assertTrue(os.path.exists('clean_data.csv'))
        self.assertTrue(os.path.exists('agg_data.csv'))

    def test_validation(self):
        load(self.grocery_sales, self.extra_data)
        self.assertTrue(validation('clean_data.csv'))
        self.assertTrue(validation('agg_data.csv'))

    def tearDown(self):
        if os.path.exists('clean_data.csv'):
            os.remove('clean_data.csv')
        if os.path.exists('agg_data.csv'):
            os.remove('agg_data.csv')
