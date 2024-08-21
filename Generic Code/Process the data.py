import mysql.connector
from datetime import datetime, timedelta
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

# Create the compact table if not exists
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS compact (
    Server INT,
    Time DATETIME,
    Minimum INT,
    Maximum INT,
    Average FLOAT,
    Total INT,
    Count INT,
    PRIMARY KEY (Server, Time)
)
"""
)

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
FROM {table_name}
GROUP BY {server_col_name}, DATE({time_col_name}), HOUR({time_col_name})
"""

# Define a query to insert aggregated data into the compact table
insert_query = """
INSERT INTO compact (Server, Time, Minimum, Maximum, Average, Total, Count)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    Minimum = VALUES(Minimum),
    Maximum = VALUES(Maximum),
    Average = VALUES(Average),
    Total = VALUES(Total),
    Count = VALUES(Count)
"""

# Execute the query and insert data into the compact table
cursor.execute(query)
rows = cursor.fetchall()

for row in rows:
    server, date, hour, minimum, maximum, average, total, count = row
    # Create a datetime object with the correct hour
    time = datetime.combine(date, datetime.min.time()).replace(hour=hour)
    cursor.execute(
        insert_query, (server, time, minimum, maximum, average, total, count)
    )

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()