from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import unidecode

from utils import compute_locations, normalize_text, normalize_string_column, haversine_distance
from utils import weather_path, session_path, weather_code_path

dim_local_path = "../data/output/common/dim_local.parquet"
dim_date_path = "../data/output/common/dim_date.parquet"
dim_time_path = "../data/output/common/dim_time.parquet"
dim_weather_code_path = "../data/output/weather/dim_weather_code.parquet"
fact_weather_path = "../data/output/weather/fact_weather.parquet"

def generate_weather_code(weather_code_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("delimiter", ",").csv(weather_code_path, header=True, inferSchema=False)

    df = df.withColumn("code", col("code").cast("int"))
    df = df.withColumn("description", col("description").cast("string"))

    window = Window.orderBy("code")

    df = df.withColumn("sk_weather_code", row_number().over(window))

    df = df.dropDuplicates()

    df = df.select(
        col("sk_weather_code"),
        col("code"),
        col("description"),
    )

    null_row = spark.createDataFrame([(-1, -1, "Unknown")], ["sk_weather_code", "code", "description"])
    df = null_row.union(df)
    df = df.orderBy("sk_weather_code")

    df.write.parquet(dim_weather_code_path, mode="overwrite")

    return dim_weather_code_path


def generate_fact_weather(weather_path: str, session_path: str):
    spark = SparkSession.builder.getOrCreate()

    df_weather = spark.read.option("delimiter", ",").csv(weather_path, header=True)
    df_session = spark.read.option("delimiter", ";").csv(session_path, header=True)

    df_session = df_session.select("Session ID", "Start Date", "City", "Country", "Latitude", "Longitude", "Elevation")

    dim_local = spark.read.parquet(dim_local_path)
    dim_weather_code = spark.read.parquet(dim_weather_code_path)
    dim_time = spark.read.parquet(dim_time_path)
    dim_time = dim_time.withColumn("hour", col("hour").cast("int"))
    dim_time = dim_time.withColumn("minute", col("minute").cast("int"))
    dim_time = dim_time.withColumn("second", col("second").cast("int"))

    # Junta com dim_date
    df = df_session.withColumn("pk_date",
        sf.concat_ws("", 
                     sf.year(col('Start Date')).cast('string'),
                     sf.lpad(sf.month(col('Start Date')).cast('string'), 2, '0'),
                     sf.lpad(sf.day(col('Start Date')).cast('string'), 2, '0')).cast('int')
    )

    # Junta com dim_tempo
    df = df.join(dim_time, (sf.hour(df["Start Date"]) == dim_time["hour"]) & 
                             (lit(0) == dim_time["minute"]) & 
                             (lit(0) == dim_time["second"]), "left").select(df['*'], dim_time['sk_time'].alias("sk_time"))
    df = df.withColumn("sk_time", sf.coalesce(col("sk_time"), lit(-1)))

    # Junta com dim_local
    df = df.join(dim_local, \
                          (df['Latitude'] == dim_local['latitude']) & \
                          (df['Longitude'] == dim_local['longitude']) & \
                          (df['Elevation'] == dim_local['elevation_m']), \
                          how = "left").select(df['*'], dim_local['sk_local'])
    df = df.withColumn("sk_local", sf.coalesce(col("sk_local"), lit(-1))) 
    
    # Junta com dim_weather_code
    df_weather = df_weather.withColumn("weather_code", col("weather_code").cast("double").cast("int"))
    df_weather = df_weather.join(dim_weather_code, df_weather['weather_code'] == dim_weather_code['code'], "left").select(df_weather['*'], dim_weather_code['sk_weather_code'])
    df_weather = df_weather.withColumn("sk_weather_code", sf.coalesce(col("sk_weather_code"), lit(-1)))


    df = df.join(df_weather, "Session ID", "left")

    df = df.select(
        col("pk_date").alias("fk_date"),
        col("sk_time").alias("fk_time"),
        col("sk_weather_code").alias("fk_weather_code"),
        col("sk_local").alias("fk_local"),
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "cloud_cover",
        "cloud_cover_low",
        "cloud_cover_mid",
        "cloud_cover_high",
    ).dropDuplicates()

    df = df.orderBy("fk_date", "fk_time", "fk_local", "fk_weather_code")

    df.write.parquet(fact_weather_path, mode="overwrite")



generate_weather_code(weather_code_path)
generate_fact_weather(weather_path, session_path)