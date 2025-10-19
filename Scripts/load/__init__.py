import requests
import datetime
import psycopg2
from connection import get_connection  # your DB function

# Your token
WAQI_TOKEN = "83a71689c7360dffa6fdf77677e667c7676a2cef"

API_URL = "https://api.openaq.org/v3/measurements"

# Always use UTC-aware date
today = datetime.datetime.now(datetime.UTC).date()

params = {
    "date_from": str(today),
    "date_to": str(today),
    "limit": 1000,   # keep small first, we can paginate later
    "page": 1,
    "sort": "desc",
    "order_by": "datetime"
}

headers = {"Authorization": f"Bearer {WAQI_TOKEN}"}

print("🔄 Fetching data from OpenAQ...")
response = requests.get(API_URL, params=params, headers=headers, timeout=30)
response.raise_for_status()
data = response.json()
print("✅ Got records:", len(data.get("results", [])))
