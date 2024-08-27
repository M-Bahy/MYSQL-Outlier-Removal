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

# Create a database connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

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

# Execute the query
cursor.execute(query)
results = cursor.fetchall()

# Close the connection
cursor.close()
conn.close()
