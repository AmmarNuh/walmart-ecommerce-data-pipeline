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

# Call the extract() function and store it as the "merged_df" variable
merged_df = extract(grocery_sales, "extra_data.parquet")

# Call the transform() function and pass the merged DataFrame
clean_data = transform(merged_df)

# Call the avg_weekly_sales_per_month() function and pass the cleaned DataFrame
avg_per_month = avg_weekly_sales_per_month(clean_data)

# Call the load() function and pass the cleaned and aggregated DataFrames with their paths    
load(clean_data, avg_per_month)

# Validate file saving
# Call the validation() function and pass first, the cleaned DataFrame path, and then the aggregated DataFrame path
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
