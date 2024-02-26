from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder.appName("RestaurantWeatherDataProcessing").getOrCreate()

# Read restaurant data from CSV
restaurant_data = spark.read.format("csv").option("header", "true").load("part-00004-1232ade3-2393-4b9e-b216-12afe2a74cf8-c000.csv")

# Filter out rows with non-null latitude and longitude
filtered_data = restaurant_data.filter(col("latitude").isNotNull() & col("longitude").isNotNull())

# Define a UDF to generate Geohash
def generate_geohash(latitude, longitude):
    # Custom geohash generation logic (you can replace this)
    return f"{latitude:.4f}_{longitude:.4f}"

generate_geohash_udf = udf(generate_geohash, StringType())
data_with_geohash = filtered_data.withColumn("Geohash", generate_geohash_udf(col("latitude"), col("longitude")))

# Read weather data from CSV
weather_data = spark.read.format("csv").option("header", "true").load("part-00004-f74bfd93-8481-453b-8f31-2031b1d198af-c000.csv")

# Join data using Geohash
joined_data = data_with_geohash.join(weather_data, data_with_geohash.Geohash == weather_data.Geohash, "left")

# Save data in Parquet format with partitioning
joined_data.write.partitionBy("country", "city").format("parquet").save("output_path")

# Stop the Spark session
spark.stop()
