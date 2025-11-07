from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import unidecode

from utils import compute_locations, normalize_text, normalize_string_column
from utils import weather_path, session_path, weather_code_path, locations_path

dim_local_path = "../data/output/weather/dim_local.parquet"
dim_date_path = "../data/output/weather/dim_date.parquet"
dim_time_path = "../data/output/weather/dim_time.parquet"
dim_weather_code_path = "../data/output/weather/dim_weather_code.parquet"
fact_weather_path = "../data/output/weather/fact_weather.parquet"


def generate_dim_local(session_path: str, locations_path: str):

    compute_locations(locations_path, session_path)
    spark = SparkSession.builder.appName("GenerateLocalDim").getOrCreate()
    df = spark.read.option("delimiter", ";").csv(session_path, header=True, inferSchema=True)
    df_locations = spark.read.option("delimiter", ",").csv(locations_path, header=True, inferSchema=True)

    location_columns = df_locations.columns
    
    df_locations_renamed = df_locations
    for c in location_columns:
        df_locations_renamed = df_locations_renamed.withColumnRenamed(c, f"loc_{c}")
    
    df_cross = df.crossJoin(df_locations_renamed).filter(
        (sf.abs(sf.col("Latitude") - sf.col("loc_Latitude")) < 0.1) &
        (sf.abs(sf.col("Longitude") - sf.col("loc_Longitude")) < 0.1)
    )
    
    dLat = sf.radians(sf.col("Latitude") - sf.col("loc_Latitude"))
    dLon = sf.radians(sf.col("Longitude") - sf.col("loc_Longitude"))

    tmp = (
        sf.pow(sf.sin(dLat / 2), 2) +
        sf.cos(sf.radians(sf.col("loc_Latitude"))) * sf.cos(sf.radians(sf.col("Latitude"))) *
        sf.pow(sf.sin(dLon / 2), 2)
    )
    
    df_cross = df_cross.withColumn(
        "distance",
        (6371 * 2 * sf.asin(sf.sqrt(tmp)))
    )

    window = Window.partitionBy(col("Session ID")).orderBy("distance")
    df_nearest = (df_cross.withColumn("rank", sf.row_number().over(window))
                  .filter(sf.col("rank") == 1)
                  .drop("rank"))
    

    df_temp = df_nearest.withColumn("Latitude", col("Latitude").cast("double")) \
                        .withColumn("Longitude", col("Longitude").cast("double"))

    df_dim_source = df_temp.select(
        col("loc_City").alias("city"),
        col("loc_Country").alias("country"),
        col('loc_Village_or_hamlet').alias("village_or_hamlet"), 
        col("loc_County").alias("county"),
        col("loc_State").alias("state"),
        col("loc_CountryCode").alias("country_code"),
        col("Latitude").alias("latitude"),
        col("Longitude").alias("longitude"),
    ).dropDuplicates()


    df_dim_source = df_dim_source.orderBy(["country", "city"])

    window_sk = Window.orderBy(lit(1))
    df_with_sk = df_dim_source.withColumn("sk_local", row_number().over(window_sk))

    df_final_dim = df_with_sk.select(
        "sk_local",
        "country",
        "city",
        "village_or_hamlet",
        "county",
        "state",
        "country_code",
        "latitude",
        "longitude",
    )

    df_final_dim = df_final_dim.orderBy('sk_local')
    df_final_dim.write.parquet(dim_local_path, mode="overwrite")
    return dim_local_path



def generate_dim_data(session_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("delimiter", ";").csv(session_path, header=True)

    df = df.select(
        sf.concat_ws("",
                    sf.year(col("Start Date")).cast("string"), 
                    sf.lpad(sf.month(col("Start Date")).cast("string"), 2, '0'),
                    sf.lpad(sf.day(col("Start Date")).cast("string"), 2, '0') 
        ).alias("pk_date"),
        sf.year(col("Start Date")).cast("string").alias("year"),
        sf.monthname(col("Start Date")).cast("string").alias("month_name"),
        sf.month(col("Start Date")).cast("string").alias("month"),
        sf.day(col("Start Date")).cast("string").alias("day"),
        sf.weekday(col("Start Date")).cast("string").alias("week_day"),
        sf.weekofyear(col("Start Date")).cast("string").alias("week_of_year"),
        sf.dayofyear(col('Start Date')).alias('day_of_year'),
        sf.when(sf.month(col('Start Date')) <= 6, 1).otherwise(2).alias('semester'),
        sf.quarter(col('Start Date')).alias('trimester'),
        sf.ceil(sf.month(col('Start Date')) / 2).alias('bimester'),
        sf.when( ( (sf.year(col('Start Date')) % 400 == 0) | ((sf.year(col('Start Date')) % 4 == 0) & (sf.year(col('Start Date')) % 100 != 0))), 1).otherwise(0).alias('is_leap_year')
    ).dropDuplicates()

    df = df.orderBy("pk_date")

    df.write.parquet(dim_date_path, mode="overwrite")
    #df.show()
    return dim_date_path

def generate_dim_time(session_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("delimiter", ";").csv(session_path, header=True)

    df = df.withColumn("label", lit("HH"))

    df = df.select(
        sf.lpad(sf.hour(col("Start Date")).cast("string"), 2, '0').alias("pk_time"),
        sf.hour(col("Start Date")).cast("string").alias("hour"),
        col("label")
    ).dropDuplicates()

    df = df.orderBy("pk_time")

    df.write.parquet(dim_time_path, mode="overwrite")
    return dim_time_path

def generate_weather_code(weather_code_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("delimiter", ",").csv(weather_code_path, header=True)

    window = Window.orderBy("code")

    df = df.withColumn("sk_weather_code", row_number().over(window))

    df = df.orderBy("sk_weather_code").dropDuplicates()

    df = df.select(
        col("sk_weather_code"),
        col("code"),
        col("description")
    )

    df.write.parquet(dim_weather_code_path, mode="overwrite")

    return dim_weather_code_path


def generate_fact_weather(weather_path: str, session_path: str):
    spark = SparkSession.builder.getOrCreate()

    df_weather = spark.read.option("delimiter", ",").csv(weather_path, header=True)
    df_session = spark.read.option("delimiter", ";").csv(session_path, header=True)

    df_session = df_session.select("Session ID", "Start Date", "City", "Country", "Latitude", "Longitude")

    dim_local = spark.read.parquet(dim_local_path)
    dim_weather_code = spark.read.parquet(dim_weather_code_path)

    df = df_session.withColumn("pk_date",
        sf.concat_ws("", 
                     sf.year(col('Start Date')).cast('string'),
                     sf.lpad(sf.month(col('Start Date')).cast('string'), 2, '0'),
                     sf.lpad(sf.day(col('Start Date')).cast('string'), 2, '0')).cast('int')
    )

    df = df.withColumn("pk_time", sf.lpad(sf.hour(col('Start Date')).cast('string'), 2, '0'))

    df = df.join(dim_local, \
                          (df['Latitude'] == dim_local['latitude']) & \
                          (df['Longitude'] == dim_local['longitude']) & \
                          (df['City'] == dim_local['raw_city']), \
                          how = "left").select(df['*'], dim_local['sk_local'])
    
    df_weather = df_weather.withColumn("weather_code", col("weather_code").cast("double").cast("int"))
    df_weather = df_weather.join(dim_weather_code, df_weather['weather_code'] == dim_weather_code['code'], "left").select(df_weather['*'], dim_weather_code['sk_weather_code'])
    
    df = df.join(df_weather, "Session ID", "left")

    df = df.select(
        col("pk_date").alias("fk_date"),
        col("pk_time").alias("fk_time"),
        col("sk_weather_code").alias("fk_weather_code"),
        col("sk_local").alias("fk_local"),
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "snow_depth",
        "pressure_msl",
        "surface_pressure",
        "cloud_cover",
        "cloud_cover_low",
        "cloud_cover_mid",
        "cloud_cover_high",
        "et0_fao_evapotranspiration",
        "vapour_pressure_deficit",
        "wind_speed_10m",
        "wind_speed_100m",
        "wind_direction_10m",
        "wind_direction_100m",
        "wind_gusts_10m",
        "soil_temperature_0_to_7cm",
        "soil_temperature_7_to_28cm",
        "soil_temperature_28_to_100cm",
        "soil_temperature_100_to_255cm",
        "soil_moisture_0_to_7cm",
        "soil_moisture_7_to_28cm",
        "soil_moisture_28_to_100cm",
        "soil_moisture_100_to_255cm",
    ).dropDuplicates()

    df = df.orderBy("fk_date", "fk_time", "fk_local", "fk_weather_code")

    df.write.parquet(fact_weather_path, mode="overwrite")



generate_dim_local(session_path, locations_path)
#generate_dim_data(session_path)
#generate_dim_time(session_path)
#generate_weather_code(weather_code_path)
#generate_fact_weather(weather_path, session_path)