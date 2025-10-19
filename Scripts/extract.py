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
