import mysql.connector
from datetime import datetime, timedelta
import random

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

# Create the original table
cursor.execute("""
CREATE TABLE IF NOT EXISTS original (
    Server INT,
    Value INT,
    Time DATETIME
)
""")

# Prepare to insert data
server_start = 1
server_end = 3
interval = timedelta(minutes=5)
start_time = datetime(2024, 1, 1)
end_time = datetime(2024, 12, 31, 23, 59, 59)

insert_query = """
INSERT INTO original (Server, Value, Time) 
VALUES (%s, %s, %s)
"""

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

        # Increment the current time by 5 minutes
        current_time += interval

# Commit the transaction
conn.commit()

# Check the number of rows inserted
cursor.execute("SELECT COUNT(*) FROM original")
count = cursor.fetchone()[0]
print(f"Total rows inserted: {count}")

# Close the connection
cursor.close()
conn.close()
