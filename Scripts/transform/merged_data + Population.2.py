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


