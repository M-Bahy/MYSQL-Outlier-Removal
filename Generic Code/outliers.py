import mysql.connector
import configparser
import pandas as pd
from scipy import stats

print("Starting the outlier removal process...")

config = configparser.ConfigParser()
config.read("config.ini")
print("Configuration file read successfully.")

# Database connection configuration
db_config = {
    "user": config["database"]["user"],
    "password": config["database"]["password"],
    "host": config["database"]["host"],
    "database": config["database"]["database"],
}

original_table_name = config["original_table"]["table_name"]
server_column = config["original_table"]["server_col_name"]
time_column = config["original_table"]["time_col_name"]
value_column = config["original_table"]["value_col_name"]

print("Database configuration loaded.")

# Create a database connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()
print("Database connection established.")

# Define the query to fetch data from the original table
fetch_query = (
    f"SELECT {server_column}, {time_column}, {value_column} FROM {original_table_name}"
)
cursor.execute(fetch_query)
print("Data fetch query executed.")

# Fetch all rows into a DataFrame
df = pd.DataFrame(cursor.fetchall(), columns=[server_column, time_column, value_column])
print(f"Fetched {len(df)} rows from the database.")

# Group by 'Server' and calculate Z-scores within each group
df["z_score"] = df.groupby(server_column)[value_column].transform(
    lambda x: stats.zscore(x)
)
print("Z-scores calculated.")

# Define Z-score threshold
threshold = int(config["z_score"]["threshold"])  # or another value like 2

# Identify outliers
outliers = df[abs(df["z_score"]) > threshold]
print(f"Identified {len(outliers)} outliers in total.")

# Count outliers per server
outliers_per_server = outliers[server_column].value_counts()
for server, count in outliers_per_server.items():
    print(f"Identified {count} outliers in server '{server}'.")

# Prepare a list of outlier records for deletion
outlier_records = list(zip(outliers[server_column], outliers[time_column]))

# Convert list to a format suitable for SQL IN clause
format_strings = ",".join(["(%s, %s)"] * len(outlier_records))
delete_query = f"DELETE FROM {original_table_name} WHERE ({server_column}, {time_column}) IN ({format_strings})"

# Flatten the list of tuples into a single tuple for execution
flattened_values = [item for sublist in outlier_records for item in sublist]

# Execute the delete query
cursor.execute(delete_query, tuple(flattened_values))
conn.commit()
print(f"Deleted {len(outlier_records)} outliers from the database.")

# Close the connection
cursor.close()
conn.close()
print("Database connection closed.")

print(f"Removed {len(outlier_records)} outliers.")

# Wait for Enter key before closing
while True:
    if input("Press Enter to exit...") == "":
        break
