import pandas as pd
import psycopg2
from psycopg2 import sql

# Configuration
CSV_FILE_PATH = 'c:/Users/rajat/Psql_Sample/data/ticket_dump.csv'  # Update with the actual CSV file path
DB_CONFIG = {
    'host': 'localhost',
    'database': 'service_Data',
    'user': 'postgres',
    'password': 'Rajath@123',
    'port': 5432
}

# Load CSV into a DataFrame
def load_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"CSV loaded successfully with {len(df)} rows.")
        return df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        raise

# Load DataFrame into PostgreSQL
def load_to_postgres(df, table_name, db_config):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                ticket_id SERIAL PRIMARY KEY,
                created_date TIMESTAMP,
                category TEXT,
                priority TEXT,
                assigned_group TEXT,
                resolution_time FLOAT,
                status TEXT
            )
        """).format(sql.Identifier(table_name)))

        # Insert data
        for _, row in df.iterrows():
            cursor.execute(sql.SQL("""
                INSERT INTO {} (ticket_id, created_date, category, priority, assigned_group, resolution_time, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """).format(sql.Identifier(table_name)), tuple(row))

        conn.commit()
        print(f"Data loaded successfully into table '{table_name}'.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    try:
        # Load CSV
        df = load_csv('C:\Users\rajat\ticket_dump')

        # Load data into PostgreSQL
        load_to_postgres(df, 'ticket_dump', DB_CONFIG)
    except Exception as e:
        print(f"Script failed: {e}")
