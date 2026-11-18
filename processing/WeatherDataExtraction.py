from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
)
import requests

# Set Hadoop home for local Windows execution
os.environ["HADOOP_HOME"] = "C:\\hadoop"

# 1. Initialize Spark Session
spark = (
    SparkSession.builder.appName("OpenMeteoHourlyExtraction")
    .master("local[*]")
    .getOrCreate()
)

# 2. Read the coordinates CSV file using Spark
df_coords = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("D:/Projects/WeatherAnalysis/pyspark/constants/latitude_longitude (1).csv")
)

rows = df_coords.collect()

# Define target date (e.g., yesterday or a specific past/current date)
# Open-Meteo forecast API provides recent/current days; use archive-api for older dates
target_date = datetime.now().strftime("%Y-%m-%d")

results = []

for row in rows:
  district = row["District"]
  lat = row["lat"]
  lon = row["lon"]

  # Updated URL: requesting hourly data for temperature, relative humidity, and wind speed for a specific date
  url = (
      f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
      f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
      f"&start_date={target_date}&end_date={target_date}"
  )

  try:
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
      data = response.json()
      hourly = data.get("hourly", {})
      times = hourly.get("time", [])
      temps = hourly.get("temperature_2m", [])
      humidities = hourly.get("relative_humidity_2m", [])
      winds = hourly.get("wind_speed_10m", [])

      # Loop through the 24 hourly data points for this district
      for t, temp, hum, wind in zip(times, temps, humidities, winds):
        results.append((
            str(district),
            float(lat),
            float(lon),
            str(t),  # Timestamp (e.g., '2026-10-12T00:00')
            float(temp) if temp is not None else 0.0,
            float(hum) if hum is not None else 0.0,
            float(wind) if wind is not None else 0.0,
        ))
    else:
      print(f"API Error for {district}: Status {response.status_code}")
  except Exception as e:
    print(f"Failed for {district}: {e}")

# 3. Define explicit Spark schema with timestamp/hour included
schema = StructType([
    StructField("District", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature_2m", FloatType(), True),
    StructField("relative_humidity_2m", FloatType(), True),
    StructField("wind_speed_10m", FloatType(), True),
])

# 4. Convert results into a native Spark DataFrame
df_weather_final = spark.createDataFrame(results, schema)

# 5. Display results and persist
df_weather_final.show(24, truncate=False)

# Write output to Parquet
# df_weather_final.write.mode("overwrite").parquet("D:/Projects/WeatherAnalysis/output/weather_hourly_parquet")