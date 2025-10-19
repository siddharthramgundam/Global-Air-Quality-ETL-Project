# connection.py
import psycopg2

def get_connection():
    """
    Returns a PostgreSQL connection object.
    """
    try:
        conn = psycopg2.connect(
            dbname="airquality_db",
            user="postgres",
            password="Siddu@2005",
            host="localhost",
            port=5432
        )
        return conn
    except Exception as e:
        print("❌ Error connecting to PostgreSQL:", e)
        return None


from connection import get_connection

conn = get_connection()
if conn:
    print("Connection successful!")
    conn.close()
else:
    print("Connection failed!")
