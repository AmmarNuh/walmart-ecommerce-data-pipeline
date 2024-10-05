import pandas as pd
import os

# Extract function.
def extract(store_data, extra_data):
    """
    Extract and merge store data with additional data.

    Parameters:
    store_data (DataFrame): The main DataFrame containing store sales data.
    extra_data (str): The file path to the Parquet file containing additional data.

    Returns:
    DataFrame: A merged DataFrame combining the store data and additional data, indexed by the 'index' column.

    This function reads the additional data from a Parquet file, and merges it with the provided store data 
    on the 'index' column to create a comprehensive dataset for further analysis.
    """
    extra_df = pd.read_parquet(extra_data)
    merged_df = store_data.merge(extra_df, on="index")
    return merged_df


# Create the transform() function with one parameter.
def transform(raw_data):
    clean_data = raw_data.loc[:, :]
    
    # Fill missing numerical values (implement your chosen method)
    clean_data.fillna({
        'CPI': clean_data['CPI'].mean(),
        'Unemployment': clean_data['Unemployment'].mean(),
        'Weekly_Sales': 0
    }, inplace=True)
    
    # Convert the 'Date' column to datetime and add the 'Month' column
    clean_data['Date'] = pd.to_datetime(clean_data['Date'])
    clean_data['Month'] = clean_data['Date'].dt.month
    #keeping the rows where the weekly sales are over $10,000 and drops the unnecessary columns. 
    clean_data = clean_data.loc[clean_data['Weekly_Sales'] > 10000, 
                                 ["Store_ID", 'Month', "Dept", "IsHoliday", "Weekly_Sales", "CPI", "Unemployment"]]
    return clean_data
  

# Create the avg_weekly_sales_per_month function that takes in the cleaned data from the last step
def avg_weekly_sales_per_month(clean_data):
    """
    Calculate the average weekly sales per month.

    Parameters:
    clean_data (DataFrame): The cleaned DataFrame containing sales data.

    Returns:
    DataFrame: A DataFrame with average monthly sales.
    """
    agg_data = (
        clean_data[['Month', 'Weekly_Sales']]  # Select necessary columns
        .groupby('Month')                       # Group by the 'Month' column
        .agg({'Weekly_Sales': 'mean'})          # Calculate average weekly sales
        .reset_index()                          # Reset index
        .round(2)                               # Round the results to two decimal places
    )
    return agg_data
  

# Create the load() function that takes in the cleaned DataFrame and the aggregated one with the paths where they are going to be stored
def load(clean_data_frame, agg_data_frame, clean_data_path='clean_data.csv', agg_data_path='agg_data.csv'):
    
    # Save the cleaned data to CSV without index
    clean_data_frame.to_csv(clean_data_path, index=False)
    
    # Save the aggregated data to CSV without index
    agg_data_frame.to_csv(agg_data_path, index=False)
    
    print("Files saved successfully!")
  

# Create the validation() function with one parameter: file_path - to check whether the previous function was correctly executed
def validation(file_path):
        """
    Check if the cleaned and aggregated CSV files exist in the current working directory.

    Parameters:
    clean_data_path (str): The file path for the cleaned data CSV.
    agg_data_path (str): The file path for the aggregated data CSV.

    Returns:
    bool: True if both files exist, False otherwise.
    """
    return os.path.exists(file_path)
