# main.py
import requests
import pandas as pd
import os
from time import sleep
import pycountry  # to convert country codes to full country names.

# ------------------ API Keys ------------------
WAQI_TOKEN = "83a71689c7360dffa6fdf77677e667c7676a2cef"
OPENWEATHER_KEY = "8a8bce175124301dfc27408b407be80a"

# ------------------ Global Cities List ------------------
cities = [
    # Asia
    "Delhi", "Mumbai", "Bengaluru", "Chennai", "Kolkata", "Beijing", "Shanghai", "Tokyo", "Seoul", "Bangkok", "Jakarta",
    # Europe
    "London", "Paris", "Berlin", "Madrid", "Milan", "Moscow", "Rome", "Vienna", "Warsaw", "Amsterdam",
    # North America
    "New York", "Los Angeles", "Chicago", "Toronto", "Mexico City", "Houston", "Vancouver", "San Francisco", "Montreal",
    # South America
    "São Paulo", "Rio de Janeiro", "Buenos Aires", "Bogotá", "Lima",
    # Africa
    "Cairo", "Lagos", "Johannesburg", "Nairobi", "Casablanca",
    # Oceania
    "Sydney", "Melbourne", "Auckland", "Brisbane"
]

# ------------------ Ensure data folder exists ------------------
if not os.path.exists("data"):
    os.makedirs("data")

# ------------------ Helper: Convert country code to full name ------------------
def get_country_name(code):
    if not code:
        return "Unknown"
    try:
        country = pycountry.countries.get(alpha_2=code.upper())
        return country.name if country else "Unknown"
    except:
        return "Unknown"

# ------------------ Function: Fetch Air Quality from WAQI ------------------
def fetch_air_quality(city):
    url = f"https://api.waqi.info/feed/{city}/?token={WAQI_TOKEN}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
    except Exception as e:
        print(f"Error fetching AQI for {city}: {e}")
        return pd.DataFrame()
    
    if data.get("status") != "ok":
        print(f"No AQI data for {city}")
        return pd.DataFrame()
    
    iaqi = data["data"].get("iaqi", {})
    country_code = data["data"].get("city", {}).get("country")  # may be None
    country_name = get_country_name(country_code)
    
    record = {
        "city": city,
        "country": country_name,
        "aqi": data["data"].get("aqi"),
        "pm25": iaqi.get("pm25", {}).get("v"),
        "pm10": iaqi.get("pm10", {}).get("v"),
        "no2": iaqi.get("no2", {}).get("v"),
        "so2": iaqi.get("so2", {}).get("v"),
        "o3": iaqi.get("o3", {}).get("v"),
        "co": iaqi.get("co", {}).get("v"),
        "datetime": data["data"].get("time", {}).get("s")
    }
    
    df = pd.DataFrame([record])
    print(f"Fetched AQI for {city}: {record}")
    return df

# ------------------ Function: Fetch Weather from OpenWeather ------------------
def fetch_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
    except Exception as e:
        print(f"Error fetching weather for {city}: {e}")
        return pd.DataFrame()
    
    if response.status_code != 200:
        print(f"No weather data for {city}: {data.get('message')}")
        return pd.DataFrame()
    
    country_code = data.get("sys", {}).get("country")  # ISO code
    country_name = get_country_name(country_code)
    
    record = {
        "city": city,
        "country": country_name,
        "temperature": data.get("main", {}).get("temp"),
        "humidity": data.get("main", {}).get("humidity"),
        "pressure": data.get("main", {}).get("pressure"),
        "wind_speed": data.get("wind", {}).get("speed"),
        "clouds": data.get("clouds", {}).get("all"),
        "weather_description": data.get("weather", [{}])[0].get("description"),
        "datetime": pd.Timestamp.now()
    }
    
    df = pd.DataFrame([record])
    print(f"Fetched weather for {city}: {record}")
    return df

# ------------------ Main Function: Extract All Data ------------------
def extract_all_data():
    all_aq = []
    all_weather = []
    
    for city in cities:
        print(f"\nProcessing city: {city}")
        
        # Fetch air quality
        aq_df = fetch_air_quality(city)
        if not aq_df.empty:
            all_aq.append(aq_df)
        
        # Fetch weather
        weather_df = fetch_weather(city)
        if not weather_df.empty:
            all_weather.append(weather_df)
        
        sleep(1)  # avoid hitting API rate limits
    
    # Combine and save CSVs
    if all_aq:
        final_aq = pd.concat(all_aq, ignore_index=True)
        final_aq.to_csv("data/air_quality.csv", index=False, encoding='utf-8-sig')
        print("\nAir quality data saved to data/air_quality.csv")
    else:
        print("\nNo air quality data fetched")
    
    if all_weather:
        final_weather = pd.concat(all_weather, ignore_index=True)
        final_weather.to_csv("data/weather.csv", index=False, encoding='utf-8-sig')
        print("Weather data saved to data/weather.csv")
    else:
        print("No weather data fetched")

# ------------------ Run ETL ------------------
if __name__ == "__main__":
    extract_all_data()


# ------------------ Transform ------------------

# feature_engineering.py
# Air Quality + Weather + Population Feature Engineering
# Saves final_merged_features.csv in the data folder

import pandas as pd
import os


# Paths
# Get project root folder
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
input_csv = r"C:\Users\Siddharth Ramgundam\Downloads\Global Air Quality ETL Project\data\final_merged_dataset.csv"


# Output CSV
output_csv = os.path.join(project_root, "data", "final_merged_features.csv")

# Load data
df = pd.read_csv(input_csv, parse_dates=['aqi_datetime'])

# Set datetime as index
df.set_index('aqi_datetime', inplace=True)

# Handle missing values
numeric_cols = ['aqi', 'pm25', 'pm10', 'no2', 'so2', 'o3', 'co', 
                'temperature', 'humidity', 'pressure', 'wind_speed', 
                'population_density']

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col].fillna(df[col].median(), inplace=True)


# Pollution Severity Index
# Weighted sum of pollutants
df['pollution_severity'] = (df.get('pm25', 0)*0.5 + 
                            df.get('pm10', 0)*0.3 + 
                            df.get('no2', 0)*0.2)


# Health Risk Score
def health_risk(severity):
    if severity > 50:
        return "High"
    elif severity > 25:
        return "Medium"
    else:
        return "Low"

df['health_risk'] = df['pollution_severity'].apply(health_risk)


# Rolling Features (24-hour rolling average)

if 'pm25' in df.columns:
    df['pm25_rolling_24h'] = df.groupby('city')['pm25'].transform(
        lambda x: x.rolling(24, min_periods=1).mean()
    )

if 'aqi' in df.columns:
    df['aqi_rolling_24h'] = df.groupby('city')['aqi'].transform(
        lambda x: x.rolling(24, min_periods=1).mean()
    )


# Time-based Features
df['hour'] = df.index.hour
df['day'] = df.index.day
df['month'] = df.index.month
df['weekday'] = df.index.weekday  # Monday=0, Sunday=6


# WHO Compliance / Unhealthy Days
if 'pm25' in df.columns:
    df['unhealthy_pm25'] = df['pm25'] > 25


# Population Exposure (optional)
if 'pm25' in df.columns and 'population_density' in df.columns:
    df['pm25_exposure'] = df['pm25'] * df['population_density']


# Save processed dataset
df.to_csv(output_csv, index=True)
print(f"Feature-engineered dataset saved to: {output_csv}")

import pandas as pd

# Load CSVs
air = pd.read_csv("data/air_quality.csv")
weather = pd.read_csv("data/weather.csv")

# Convert datetime columns
air["datetime"] = pd.to_datetime(air["datetime"], errors="coerce")
weather["datetime"] = pd.to_datetime(weather["datetime"], errors="coerce")

# Extract only the date for merging
air["date"] = air["datetime"].dt.date
weather["date"] = weather["datetime"].dt.date

# Merge datasets by city and date
merged = pd.merge(
    air,
    weather,
    on=["city", "date"],
    how="inner",
    suffixes=('_aqi', '_weather')  # avoid duplicate column names
)

# Fill or create a unified 'country' column
if 'country_weather' in merged.columns:
    merged['country'] = merged['country_weather']
elif 'country_aqi' in merged.columns:
    merged['country'] = merged['country_aqi']
else:
    merged['country'] = "Unknown"

# Drop old country columns
for col in ['country_aqi', 'country_weather']:
    if col in merged.columns:
        merged.drop(columns=col, inplace=True)

# Rename datetime columns for clarity
merged.rename(columns={
    "datetime_aqi": "aqi_datetime",
    "datetime_weather": "weather_datetime"
}, inplace=True)

# Save the cleaned merged CSV
merged.to_csv("data/merged_data.csv", index=False, encoding='utf-8-sig')

print(f"Merge successful! {len(merged)} rows")
print("Merged file saved as data/merged_data.csv")

import pandas as pd
import os

# Step 1: Define file paths
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
data_dir = os.path.join(base_dir, "data")
processed_dir = os.path.join(base_dir, "processed_data")

# Input files
air_weather_file = os.path.join(data_dir, "merged_data.csv")  # Your Air+Weather data
population_file = os.path.join(data_dir, "population_cleaned.csv")  # World Bank population

# Output file
final_output_file = os.path.join(processed_dir, "final_merged_dataset.csv")

# Mapping file for country name mismatches
mapping_file = os.path.join(base_dir, "scripts", "transform", "country_mapping.csv")

# Step 2: Load datasets
print("Loading datasets...")
air_weather = pd.read_csv(air_weather_file)
population = pd.read_csv(population_file)

print("Air+Weather shape:", air_weather.shape)
print("Population shape:", population.shape)

# Step 3: Preprocess population dataset
# Rename columns for clarity
population.rename(columns={"Country Name": "country_name", "Year": "year"}, inplace=True)

mapping_file = os.path.join(base_dir, "scripts", "transform", "country_mapping.csv")

if os.path.exists(mapping_file):
    mapping_df = pd.read_csv(mapping_file)
    mapping_dict = dict(zip(mapping_df["country"], mapping_df["country_name"]))
    air_weather["country"] = air_weather["country"].replace(mapping_dict)

# Convert date/year columns to numeric for merging
air_weather["year"] = pd.to_datetime(air_weather["date"], errors="coerce").dt.year
population["year"] = pd.to_numeric(population["year"], errors="coerce")


# Step 4: Fill Population for Air+Weather
# Sort population by country and year
population = population.sort_values(["country_name", "year"])

# Function to get latest available population for each country
def get_population(row):
    country = row["country"]
    year = row["year"]
    df = population[(population["country_name"] == country) & (population["year"] <= year)]
    if not df.empty:
        return df.iloc[-1]["Population"]
    return None

# Apply to Air+Weather dataset
air_weather["Population"] = air_weather.apply(get_population, axis=1)

print("Population filled for Air+Weather dataset.")
print("Final dataset shape:", air_weather.shape)

# Step 5: Save merged dataset in data folder
final_output_file = r"C:\Users\Siddharth Ramgundam\Downloads\Global Air Quality ETL Project\data\final_merged_dataset.csv"
os.makedirs(os.path.dirname(final_output_file), exist_ok=True)
air_weather.to_csv(final_output_file, index=False)
print("✅ Final merged dataset saved at:", final_output_file)


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

# Combine 'date' + 'weather_datetime' safely
df['weather_datetime'] = pd.to_datetime(
    df['date'].astype(str) + ' ' + df['weather_datetime'].astype(str),
    errors='coerce', utc=True
)

# Drop rows with invalid timestamps
df = df.dropna(subset=['aqi_datetime', 'weather_datetime'])

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


# load_weather.py
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import sys
import os

# Add current folder to path to find connection.py
sys.path.append(os.path.dirname(__file__))
from connection import get_connection  # your DB connection function

# Load CSV
csv_path = r"C:\Users\Siddharth Ramgundam\Downloads\Global Air Quality ETL Project\Data\final_merged_features.csv"
df = pd.read_csv(csv_path)

# -----------------------------
# Step 1: Handle timestamps safely
# -----------------------------
df['weather_datetime'] = pd.to_datetime(
    df['date'].astype(str) + ' ' + df['weather_datetime'].astype(str),
    errors='coerce', utc=True
)

# Drop rows with invalid timestamps
invalid_rows = df[df['weather_datetime'].isna()]
if len(invalid_rows) > 0:
    print(f"⚠️ Dropping {len(invalid_rows)} rows due to invalid timestamps")
df = df.dropna(subset=['weather_datetime'])

# -----------------------------
# Step 2: Connect to PostgreSQL
# -----------------------------
conn = get_connection()
if conn is None:
    raise Exception("Database connection failed!")
cur = conn.cursor()

# -----------------------------
# Step 3: Create table if not exists
# -----------------------------
cur.execute("""
CREATE TABLE IF NOT EXISTS weather (
    weather_datetime TIMESTAMP,
    city VARCHAR(100),
    country VARCHAR(100),
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    wind_speed FLOAT,
    clouds FLOAT,
    weather_description VARCHAR(255)
);
""")
conn.commit()

# -----------------------------
# Step 4: Bulk insert data
# -----------------------------
columns = [
    'weather_datetime', 'city', 'country', 'temperature', 'humidity',
    'pressure', 'wind_speed', 'clouds', 'weather_description'
]

values = [tuple(x) for x in df[columns].to_numpy()]
insert_query = f"INSERT INTO weather ({','.join(columns)}) VALUES %s"

execute_values(cur, insert_query, values)
conn.commit()

# -----------------------------
# Step 5: Close connection
# -----------------------------
cur.close()
conn.close()

print(f"✅ Weather table loaded successfully! Total rows inserted: {len(values)}")
