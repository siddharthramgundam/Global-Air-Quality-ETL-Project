# load_population.py
import pandas as pd
import psycopg2
import sys
import os

sys.path.append(os.path.dirname(__file__))
from connection import get_connection  # your DB connection function


# Step 1: Load CSV robustly
csv_path = r"C:\Users\Siddharth Ramgundam\Downloads\Global Air Quality ETL Project\Data\population_cleaned.csv"
df = pd.read_csv(csv_path)

# Try different separators
separators = [',', ';', '\t']
for sep in separators:
    try:
        df = pd.read_csv(csv_path, sep=sep, on_bad_lines='skip')
        if df.shape[1] > 1:  # if more than 1 column, we likely found correct separator
            break
    except Exception as e:
        continue
else:
    raise Exception("Could not read CSV: check file format or separators")

# Step 2: Clean column names
df.columns = df.columns.str.strip()


# Step 3: Keep only required columns
required_cols = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code', 'Year', 'Population']

# Map existing columns to required names if necessary
existing_cols = [col for col in required_cols if col in df.columns]
missing_cols = set(required_cols) - set(existing_cols)

if missing_cols:
    print(f" Warning: Missing columns in CSV: {missing_cols}")
    # Optional: you can create mapping here if your cleaned CSV uses different names

df = df[existing_cols]


# Step 4: Convert types to native Python
if 'Population' in df.columns:
    df['Population'] = pd.to_numeric(df['Population'], errors='coerce').fillna(0)
if 'Year' in df.columns:
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce').fillna(0)

# Step 5: Connect to PostgreSQL
conn = get_connection()
if conn is None:
    raise Exception("Database connection failed!")
cur = conn.cursor()


# Step 6: Create population table
cur.execute("""
CREATE TABLE IF NOT EXISTS population (
    country VARCHAR(100),
    country_code VARCHAR(10),
    indicator_name VARCHAR(255),
    indicator_code VARCHAR(50),
    year INT,
    population NUMERIC
);
""")
conn.commit()


# Step 7: Insert data into table
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO population (
            country, country_code, indicator_name, indicator_code, year, population
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        str(row.get('Country Name', '')),
        str(row.get('Country Code', '')),
        str(row.get('Indicator Name', '')),
        str(row.get('Indicator Code', '')),
        int(row.get('Year', 0)),       # native int
        float(row.get('Population', 0)) # native float
    ))


# Step 8: Close connection
conn.commit()
cur.close()
conn.close()

print("Population table loaded successfully!")
