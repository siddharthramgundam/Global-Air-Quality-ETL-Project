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
