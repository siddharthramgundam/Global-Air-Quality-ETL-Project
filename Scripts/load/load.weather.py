# load_weather.py
import pandas as pd
import psycopg2
import os
import sys
from datetime import datetimeq  1     

# Add current folder to path if needed to import connection.py
sys.path.append(os.path.dirname(__file__))
from connection import get_connection  # Your DB connection function

# -------------------------------
# Step 1: Load transformed CSV
# -------------------------------
csv_path = r"C:\Users\Siddharth Ramgundam\Downloads\Global Air Quality ETL Project\Data\final_merged_features.csv"


if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found: {csv_path}")

df = pd.read_csv(csv_path)

# -------------------------------
# Step 2: Parse 'weather_datetime' safely
# -------------------------------
# Handles microseconds and missing values
df['weather_datetime'] = pd.to_datetime(df['weather_datetime'], errors='coerce')

# Check and drop invalid timestamps
invalid_rows = df[df['weather_datetime'].isna()]
if not invalid_rows.empty:
    print(f"⚠ Dropping {len(invalid_rows)} rows due to invalid timestamps")
    print(invalid_rows)

df = df[df['weather_datetime'].notna()]

# -------------------------------
# Step 3: Connect to PostgreSQL
# -------------------------------
conn = get_connection()
cur = conn.cursor()

# -------------------------------
# Step 4: Delete existing rows for today
# -------------------------------
today_str = datetime.now().strftime('%Y-%m-%d')
cur.execute("DELETE FROM weather WHERE weather_datetime::date = %s;", (today_str,))
print(f"Deleted existing rows for {today_str}")

# -------------------------------
# Step 5: Insert all rows from dataframe with UPSERT
# -------------------------------
insert_query = """
INSERT INTO weather (weather_datetime, city, country, temperature, humidity, pressure, wind_speed, clouds, weather_description)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (weather_datetime, city)
DO UPDATE SET
    country = EXCLUDED.country,
    temperature = EXCLUDED.temperature,
    humidity = EXCLUDED.humidity,
    pressure = EXCLUDED.pressure,
    wind_speed = EXCLUDED.wind_speed,
    clouds = EXCLUDED.clouds,
    weather_description = EXCLUDED.weather_description;
"""

rows_inserted = 0
for idx, row in df.iterrows():
    cur.execute(insert_query, (
        row['weather_datetime'], row['city'], row['country'], row['temperature'], 
        row['humidity'], row['pressure'], row['wind_speed'], row['clouds'], 
        row['weather_description']
    ))
    rows_inserted += 1

# -------------------------------
# Step 6: Commit and close
# -------------------------------
conn.commit()
cur.close()
conn.close()

print(f"✅ Weather table loaded successfully! Total rows inserted: {rows_inserted}")
