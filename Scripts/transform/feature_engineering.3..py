# feature_engineering.py
# Air Quality + Weather + Population Feature Engineering
# Saves final_merged_features.csv in the data folder

import pandas as pd
import os


# Paths
# Get project root folder
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

# Input CSV
input_csv = os.path.join(project_root, "data", "final_merged_dataset.csv")

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
