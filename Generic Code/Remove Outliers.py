import mysql.connector
import configparser
import pandas as pd
from scipy import stats

config = configparser.ConfigParser()
config.read('config.ini')

# Database connection configuration
db_config = {
    'user': config['database']['user'],
    'password': config['database']['password'],
    'host': config['database']['host'],
    'database': config['database']['database']
}

original_table_name = config['original_table']['table_name']
server_column = config['original_table']['server_col_name']
time_column = config['original_table']['time_col_name']
value_column = config['original_table']['value_col_name']


# Create a database connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Define the query to fetch data from the original table
fetch_query = f"SELECT {server_column}, {time_column}, {value_column} FROM {original_table_name}"
cursor.execute(fetch_query)

# Fetch all rows into a DataFrame
df = pd.DataFrame(cursor.fetchall(), columns=[server_column, time_column, value_column])

# Group by 'Server' and calculate Z-scores within each group
df['z_score'] = df.groupby(server_column)[value_column].transform(lambda x: stats.zscore(x))

# Define Z-score threshold
threshold = 1  # or another value like 2

# Identify outliers
outliers = df[abs(df['z_score']) > threshold]

# Prepare a list of outlier records for deletion
outlier_records = list(zip(outliers[server_column], outliers[time_column]))

# Convert list to a format suitable for SQL IN clause
format_strings = ','.join(['(%s, %s)'] * len(outlier_records))
delete_query = f"DELETE FROM {original_table_name} WHERE ({server_column}, {time_column}) IN ({format_strings})"

# Flatten the list of tuples into a single tuple for execution
flattened_values = [item for sublist in outlier_records for item in sublist]

# Execute the delete query
cursor.execute(delete_query, tuple(flattened_values))
conn.commit()

# Close the connection
cursor.close()
conn.close()

print(f"Removed {len(outlier_records)} outliers.")