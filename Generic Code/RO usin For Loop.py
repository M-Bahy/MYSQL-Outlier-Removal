import mysql.connector
import pandas as pd
from scipy import stats
import configparser

# Read configuration from config.ini
config = configparser.ConfigParser()
config.read('Generic Code/config.ini')

db_config = {
    'user': config['database']['user'],
    'password': config['database']['password'],
    'host': config['database']['host'],
    'database': config['database']['database']
}

table_name = config['original_table']['table_name']
server_col_name = config['original_table']['server_col_name']
time_col_name = config['original_table']['time_col_name']
value_col_name = config['original_table']['value_col_name']

# Create a database connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Define the query to fetch data from the original table
fetch_query = f"SELECT {server_col_name}, {time_col_name}, {value_col_name} FROM {table_name}"
cursor.execute(fetch_query)

# Fetch all rows into a DataFrame
df = pd.DataFrame(cursor.fetchall(), columns=[server_col_name, time_col_name, value_col_name])

# Get distinct servers
servers = df[server_col_name].unique()

# Define Z-score threshold
threshold = 1  # or another value like 2

outlier_records = []

# Iterate over each server
for server in servers:
    server_data = df[df[server_col_name] == server].copy()
    server_data.loc[:, 'z_score'] = stats.zscore(server_data[value_col_name])
    server_outliers = server_data[abs(server_data['z_score']) > threshold]
    outlier_records.extend(list(zip(server_outliers[server_col_name], server_outliers[time_col_name])))

# Convert list to a format suitable for SQL IN clause
format_strings = ','.join(['(%s, %s)'] * len(outlier_records))
delete_query = f"DELETE FROM {table_name} WHERE ({server_col_name}, {time_col_name}) IN ({format_strings})"

# Flatten the list of tuples into a single tuple for execution
flattened_values = [item for sublist in outlier_records for item in sublist]

# Execute the delete query
cursor.execute(delete_query, tuple(flattened_values))
conn.commit()

# Close the connection
cursor.close()
conn.close()

print(f"Removed {len(outlier_records)} outliers.")