import mysql.connector
from datetime import datetime, timedelta

# Database connection configuration
config = {
    "user": "root",
    "password": "Bahy$2942002",
    "host": "localhost",
    "database": "bahy",
}

# Create a database connection
conn = mysql.connector.connect(**config)
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
query = """
SELECT Server, 
       DATE(Time) AS Date,
       HOUR(Time) AS Hour,
       MIN(Value) AS Minimum,
       MAX(Value) AS Maximum,
       AVG(Value) AS Average,
       SUM(Value) AS Total,
       COUNT(Value) AS Count
FROM original
GROUP BY Server, DATE(Time), HOUR(Time)
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
