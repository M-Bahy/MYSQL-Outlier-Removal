import mysql.connector
from datetime import datetime, timedelta
import configparser

print("Starting the data processing...")

# Read configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini')
print("Configuration file read successfully.")

db_config = {
    'user': config['database']['user'],
    'password': config['database']['password'],
    'host': config['database']['host'],
    'database': config['database']['database']
}

# Original table configuration
original_table_name = config['original_table']['table_name']
server_col_name = config['original_table']['server_col_name']
time_col_name = config['original_table']['time_col_name']
value_col_name = config['original_table']['value_col_name']

# Compact table configuration
compact_table_name = config['compact_table']['table_name']
compact_server_col_name = config['compact_table']['server_col_name']
compact_time_col_name = config['compact_table']['time_col_name']
compact_min_col_name = config['compact_table']['min_col_name']
compact_max_col_name = config['compact_table']['max_col_name']
compact_avg_col_name = config['compact_table']['avg_col_name']
compact_total_col_name = config['compact_table']['total_col_name']
compact_count_col_name = config['compact_table']['count_col_name']

print("Database configuration loaded.")

# Create a database connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()
print("Database connection established.")

# Create the compact table if not exists
cursor.execute(
    f"""
CREATE TABLE IF NOT EXISTS {compact_table_name} (
    {compact_server_col_name} INT,
    {compact_time_col_name} DATETIME,
    {compact_min_col_name} INT,
    {compact_max_col_name} INT,
    {compact_avg_col_name} FLOAT,
    {compact_total_col_name} INT,
    {compact_count_col_name} INT,
    PRIMARY KEY ({compact_server_col_name}, {compact_time_col_name})
)
"""
)
print(f"Compact table '{compact_table_name}' ensured to exist.")

# Define a query to get data from the original table
query = f"""
SELECT {server_col_name}, 
       DATE({time_col_name}) AS Date,
       HOUR({time_col_name}) AS Hour,
       MIN({value_col_name}) AS Minimum,
       MAX({value_col_name}) AS Maximum,
       AVG({value_col_name}) AS Average,
       SUM({value_col_name}) AS Total,
       COUNT({value_col_name}) AS Count
FROM {original_table_name}
GROUP BY {server_col_name}, DATE({time_col_name}), HOUR({time_col_name})
"""
print("Data aggregation query defined.")

# Define a query to insert aggregated data into the compact table
insert_query = f"""
INSERT INTO {compact_table_name} ({compact_server_col_name}, {compact_time_col_name}, {compact_min_col_name}, {compact_max_col_name}, {compact_avg_col_name}, {compact_total_col_name}, {compact_count_col_name})
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    {compact_min_col_name} = VALUES({compact_min_col_name}),
    {compact_max_col_name} = VALUES({compact_max_col_name}),
    {compact_avg_col_name} = VALUES({compact_avg_col_name}),
    {compact_total_col_name} = VALUES({compact_total_col_name}),
    {compact_count_col_name} = VALUES({compact_count_col_name})
"""
print("Insert query for compact table defined.")

# Execute the query and insert data into the compact table
cursor.execute(query)
rows = cursor.fetchall()
print(f"Fetched {len(rows)} rows from the original table.")

for row in rows:
    server, date, hour, minimum, maximum, average, total, count = row
    # Create a datetime object with the correct hour
    time = datetime.combine(date, datetime.min.time()).replace(hour=hour)
    cursor.execute(
        insert_query, (server, time, minimum, maximum, average, total, count)
    )
print("Data inserted into the compact table.")

# Commit the transaction
conn.commit()
print("Transaction committed.")

# Close the connection
cursor.close()
conn.close()
print("Database connection closed.")