import mysql.connector
import configparser

# Read database configuration from the .ini file
config = configparser.ConfigParser()
config.read("config.ini")

db_config = {
    "user": config["database"]["user"],
    "password": config["database"]["password"],
    "host": config["database"]["host"],
    "database": config["database"]["database"],
}

# Read table name from the .ini file
phase2_table_name = config["tables"]["phase2"]

# Create a database connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()
print("Database connection established.")

# Drop the phase2 table if it exists
cursor.execute(f"DROP TABLE IF EXISTS {phase2_table_name}")
print(f"Dropped existing {phase2_table_name} table if it existed.")

# Create the phase2 table
cursor.execute(
    f"""
CREATE TABLE {phase2_table_name} (
    Hours INT,
    average_value FLOAT,
    Server INT
)
"""
)
print(f"Created {phase2_table_name} table.")

# Define the query to calculate the accumulated average
query = f"""
SELECT 
    HOUR({config["compact_table"]["time_col_name"]}) AS Hours,
    SUM({config["compact_table"]["total_col_name"]}) / SUM({config["compact_table"]["count_col_name"]}) AS average_value,
    {config["compact_table"]["server_col_name"]} AS Server
FROM 
    {config["compact_table"]["table_name"]}
GROUP BY 
    Hours, 
    Server
"""
print("Accumulated average query defined.")

# Execute the query
cursor.execute(query)
results = cursor.fetchall()
print(f"Fetched {len(results)} rows from the compact table.")

# Insert the results into the phase2 table
insert_query = f"""
INSERT INTO {phase2_table_name} (Hours, average_value, Server)
VALUES (%s, %s, %s)
"""
for row in results:
    cursor.execute(insert_query, row)
print(f"Data inserted into the {phase2_table_name} table.")

# Commit the transaction
conn.commit()
print("Transaction committed.")

# Close the connection
cursor.close()
conn.close()
print("Database connection closed.")

# Wait for Enter key before closing
while True:
    if input("Press Enter to exit...") == "":
        break
