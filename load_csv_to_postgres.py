import psycopg2   # pip install psycopg2
import csv
import os

# Database connection parameters
DB_HOST = 'localhost'
DB_NAME = 'service_data'
DB_USER = 'postgres'
DB_PASSWORD = 'Rajath@123'
DB_PORT = '5432'

# Function to connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

# Main function to ingest data
def ingest_data():
    # Connect to PostgreSQL
    conn = connect_to_db()
    cur = conn.cursor()

    # File path
    file_path = r'C:\Users\rajat\ticket_dump\Sample Data file for Analysis_Jan 25.csv'
    
    # Print the current working directory
    print(f"Current working directory: {os.getcwd()}")
    
    # List files in the directory
    print(f"Files in directory: {os.listdir(os.path.dirname(file_path))}")
    
    # Print the file path
    print(f"File path: {file_path}")
    
    # Print the absolute file path
    abs_file_path = os.path.abspath(file_path)
    print(f"Absolute file path: {abs_file_path}")
    
    # Check if the file exists
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        return

    # Open the CSV file
    with open(file_path, 'r') as file:
        data_reader = csv.reader(file)
        next(data_reader)  # Skip the header row

        # Insert data into the database
        for row in data_reader:
            cur.execute(
                "INSERT INTO your_table_name (column1, column2, column3) VALUES (%s, %s, %s)",
                row
            )

    # Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("Data ingested successfully")

if __name__ == "__main__":
    ingest_data()