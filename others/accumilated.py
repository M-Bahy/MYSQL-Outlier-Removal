import mysql.connector
from datetime import datetime

# Database connection configuration
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

# Define a query to get aggregated data from the original table
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

# Define a query to get the previous cumulative data for a server and hour
previous_data_query = """
SELECT Total, Count
FROM compact
WHERE Server = %s AND Time = %s
"""

# Define a query to insert or update aggregated data into the compact table
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

# Execute the aggregation query
cursor.execute(query)
rows = cursor.fetchall()

# Initialize cumulative values dictionary
cumulative_data = {}

for row in rows:
    server, date, hour, minimum, maximum, average, total, count = row
    time = datetime.combine(date, datetime.min.time()).replace(hour=hour)

    # Check for previous cumulative data
    cursor.execute(previous_data_query, (server, time))
    result = cursor.fetchone()

    if result:
        prev_total, prev_count = result
        # Update cumulative totals
        cumulative_total = prev_total + total
        cumulative_count = prev_count + count
    else:
        # Initialize cumulative totals
        cumulative_total = total
        cumulative_count = count

    # Calculate average
    if cumulative_count > 0:
        cumulative_average = cumulative_total / cumulative_count
    else:
        cumulative_average = 0

    # Insert or update the compact table
    cursor.execute(
        insert_query,
        (
            server,
            time,
            minimum,
            maximum,
            cumulative_average,
            cumulative_total,
            cumulative_count,
        ),
    )

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()
