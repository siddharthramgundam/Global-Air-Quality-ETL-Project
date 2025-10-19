# load_air_quality.py
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import sys
import os

# Add current folder to path to find connection.py
sys.path.append(os.path.dirname(__file__))
from connection import get_connection  # your DB connection function

# -----------------------------
# Step 1: Load CSV
# -----------------------------
csv_path = r"C:\Users\Siddharth Ramgundam\Downloads\Global Air Quality ETL Project\Data\final_merged_features.csv"
df = pd.read_csv(csv_path)

# -----------------------------
# Step 2: Fix timestamps
# -----------------------------
# Convert 'aqi_datetime' safely (UTC)
df['aqi_datetime'] = pd.to_datetime(df['aqi_datetime'], errors='coerce', utc=True)

# Combine 'date' + 'aqi_datetime' safely
df['aqi_datetime'] = pd.to_datetime(
    df['date'].astype(str) + ' ' + df['aqi_datetime'].astype(str),
    errors='coerce', utc=True
)

# Drop rows with invalid timestamps
# Keep only rows where 'aqi_datetime' is valid
df = df.dropna(subset=['aqi_datetime'])

# -----------------------------
# Step 3: Fix Population column
# -----------------------------
df['Population'] = pd.to_numeric(df['Population'], errors='coerce').fillna(0)

# -----------------------------
# Step 4: Connect to PostgreSQL
# -----------------------------
conn = get_connection()
if conn is None:
    raise Exception("Database connection failed!")
cur = conn.cursor()

# -----------------------------
# Step 5: Create air_quality table
# -----------------------------
cur.execute("""
CREATE TABLE IF NOT EXISTS air_quality (
    aqi_datetime TIMESTAMP,
    city VARCHAR(100),
    aqi INT,
    pm25 FLOAT,
    pm10 FLOAT,
    no2 FLOAT,
    so2 FLOAT,
    o3 FLOAT,
    co FLOAT,
    date DATE,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    wind_speed FLOAT,
    clouds FLOAT,
    weather_description VARCHAR(255),
    weather_datetime TIMESTAMP,
    country VARCHAR(100),
    year INT,
    population NUMERIC,
    pollution_severity VARCHAR(50),
    health_risk VARCHAR(50),
    pm25_rolling_24h FLOAT,
    aqi_rolling_24h FLOAT,
    hour INT,
    day INT,
    month INT,
    weekday INT,
    unhealthy_pm25 BOOLEAN
);
""")
conn.commit()

# -----------------------------
# Step 6: Bulk insert into table
# -----------------------------
# Replace Python column names with DataFrame names
df_columns = [
    'aqi_datetime', 'city', 'aqi', 'pm25', 'pm10', 'no2', 'so2', 'o3', 'co',
    'date', 'temperature', 'humidity', 'pressure', 'wind_speed', 'clouds',
    'weather_description', 'weather_datetime', 'country', 'year', 'Population',
    'pollution_severity', 'health_risk', 'pm25_rolling_24h', 'aqi_rolling_24h',
    'hour', 'day', 'month', 'weekday', 'unhealthy_pm25'
]

# Convert DataFrame to list of tuples for execute_values
values = [tuple(x) for x in df[df_columns].to_numpy()]

insert_query = f"""
INSERT INTO air_quality ({','.join(df_columns)}) VALUES %s
"""

execute_values(cur, insert_query, values)
conn.commit()

# -----------------------------
# Step 7: Close connection
# -----------------------------
cur.close()
conn.close()

print(f"✅ Air Quality table loaded successfully! Total rows inserted: {len(values)}")