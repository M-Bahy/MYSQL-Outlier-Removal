import mysql.connector
import pandas as pd
from scipy import stats

# Database connection configuration
config = {
    'user': 'root',
    'password': 'Bahy$2942002',
    'host': 'localhost',
    'database': 'bahy'
}

# Create a database connection
conn = mysql.connector.connect(**config)
cursor = conn.cursor()

# Define the query to fetch data from the original table
fetch_query = "SELECT Server, Time, Value FROM original"
cursor.execute(fetch_query)

# Fetch all rows into a DataFrame
df = pd.DataFrame(cursor.fetchall(), columns=['Server', 'Time', 'Value'])

# Get distinct servers
servers = df['Server'].unique()

# Define Z-score threshold
threshold = 1  # or another value like 2

outlier_records = []

# Iterate over each server
for server in servers:
    server_data = df[df['Server'] == server].copy()
    server_data.loc[:, 'z_score'] = stats.zscore(server_data['Value'])
    server_outliers = server_data[abs(server_data['z_score']) > threshold]
    outlier_records.extend(list(zip(server_outliers['Server'], server_outliers['Time'])))

# Convert list to a format suitable for SQL IN clause
format_strings = ','.join(['(%s, %s)'] * len(outlier_records))
delete_query = f"DELETE FROM original WHERE (Server, Time) IN ({format_strings})"

# Flatten the list of tuples into a single tuple for execution
flattened_values = [item for sublist in outlier_records for item in sublist]

# Execute the delete query
cursor.execute(delete_query, tuple(flattened_values))
conn.commit()

# Close the connection
cursor.close()
conn.close()

print(f"Removed {len(outlier_records)} outliers.")