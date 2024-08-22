import mysql.connector
from datetime import datetime, timedelta
import random
import configparser

# Read configuration from config.ini
config = configparser.ConfigParser()
config.read("config.ini")

# Database connection configuration
db_config = {
    "user": config["database"]["user"],
    "password": config["database"]["password"],
    "host": config["database"]["host"],
    "database": config["database"]["database"],
}

# Table and column names from config.ini
original_table_name = config["original_table"]["table_name"]
server_col_name = config["original_table"]["server_col_name"]
time_col_name = config["original_table"]["time_col_name"]
value_col_name = config["original_table"]["value_col_name"]

# Create a database connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Create the original table with a composite primary key
cursor.execute(
    f"""
CREATE TABLE IF NOT EXISTS {original_table_name} (
    {server_col_name} INT,
    {value_col_name} INT,
    {time_col_name} DATETIME,
    PRIMARY KEY ({server_col_name}, {time_col_name})
)
"""
)
print(f"Table {original_table_name} created or already exists.")

# Prepare to insert data
server_start = 1
server_end = 3
interval = timedelta(minutes=5)
start_time = datetime(2024, 1, 1)
end_time = datetime(2024, 12, 31, 23, 59, 59)

insert_query = f"""
INSERT INTO {original_table_name} ({server_col_name}, {value_col_name}, {time_col_name}) 
VALUES (%s, %s, %s)
"""

row_count = 0
for server in range(server_start, server_end + 1):
    current_time = start_time
    while current_time <= end_time:
        # Generate a random number to determine the range of the value
        if random.random() <= 0.8:
            # 80% of the time, generate a value between 30 and 50
            value = random.randint(30, 50)
        else:
            # 20% of the time, generate a value between 0 and 100
            value = random.randint(0, 100)

        # Insert the data
        cursor.execute(insert_query, (server, value, current_time))
        row_count += 1

        # Print feedback every 50,000 rows
        if row_count % 50000 == 0:
            print(f"{row_count} rows inserted...")

        # Increment the current time by 5 minutes
        current_time += interval

print("Data insertion completed.")

# Commit the transaction
conn.commit()

# Check the number of rows inserted
cursor.execute(f"SELECT COUNT(*) FROM {original_table_name}")
count = cursor.fetchone()[0]
print(f"Total rows inserted: {count}")

# Close the connection
cursor.close()
conn.close()
print("Database connection closed.")

# Wait for Enter key before closing
while True:
    if input("Press Enter to exit...") == "":
        break
