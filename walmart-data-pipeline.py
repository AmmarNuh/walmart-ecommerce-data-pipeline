import pandas as pd
#import matplotlib.pyplot as plt
from pipeline_units.data_pipeline import extract, transform, avg_weekly_sales_per_month, load, validation

# Load your grocery sales DataFrame (replace with actual data loading)
grocery_sales = pd.DataFrame({
    'index': [1, 2, 3],
    'Store_ID': [1, 2, 1],
    'Date': ['2021-01-01', '2021-02-01', '2021-03-01'],
    'Weekly_Sales': [15000, 20000, 25000]
})

# Execute the pipeline
merged_df = extract(grocery_sales, "extra_data.parquet")
clean_data = transform(merged_df)
avg_per_month = avg_weekly_sales_per_month(clean_data)
load(clean_data, avg_per_month)

# Validate file saving
clean_data_path = 'clean_data.csv'
agg_data_path = 'agg_data.csv'
print("Clean data file exists:", validation(clean_data_path))
print("Aggregated data file exists:", validation(agg_data_path))

# # Visualization
# plt.figure(figsize=(10, 6))
# plt.bar(avg_per_month['Month'], avg_per_month['Weekly_Sales'], color='skyblue')
# plt.title('Average Weekly Sales per Month')
# plt.xlabel('Month')
# plt.ylabel('Average Weekly Sales')
# plt.xticks(avg_per_month['Month'])
# plt.grid(axis='y')
# plt.show()
