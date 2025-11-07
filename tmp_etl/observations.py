from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import unidecode

from utils import compute_locations, normalize_text, normalize_string_column, getDistanceFromLatLonInKm
from utils import session_path, rates_path, locations_path, meteor_shower_path

dim_local_path = "../data/output/observations/dim_local.parquet"
dim_date_path = "../data/output/observations/dim_date.parquet"
dim_time_path = "../data/output/observations/dim_time.parquet"
dim_user_path = "../data/output/observations/dim_user.parquet"
dim_shower_path = "../data/output/observations/dim_shower.parquet"
dim_junk_path = "../data/output/observations/dim_junk.parquet"
fact_observations_path = "../data/output/observations/fact_observations.parquet"

def generate_local_dim(session_path: str, locations_path: str):

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


    df_temp = df_temp.withColumn("elevation_km", col("Elevation")/1000)

    df_dim_source = df_temp.select(
        col("loc_City").alias("city"),
        col("loc_Country").alias("country"),
        col('loc_Village_or_hamlet').alias("village_or_hamlet"), 
        col("loc_County").alias("county"),
        col("loc_State").alias("state"),
        col("loc_CountryCode").alias("country_code"),
        col("Latitude").alias("latitude"),
        col("Longitude").alias("longitude"),
        col("Elevation").alias("elevation_m"),
        col("elevation_km"),
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
        "elevation_m",
        "elevation_km",
    )

    df_final_dim = df_final_dim.orderBy('sk_local')
    df_final_dim.write.parquet(dim_local_path, mode="overwrite")
    return dim_local_path



def generate_date_dim(rates_path: str):
    spark = SparkSession.builder.getOrCreate()
    df_rates = spark.read.option("delimiter", ";").csv(rates_path, header=True)

    rates_start_dates =  df_rates.select(
        sf.concat_ws("", 
                     sf.year(col('Start Date')).cast('string'), 
                     sf.lpad(sf.month(col('Start Date')).cast('string'), 2, '0'), 
                     sf.lpad(sf.day(col('Start Date')).cast('string'), 2, '0')).cast('int').alias('pk_date'),
        sf.year(col('Start Date')).alias('year'),
        sf.monthname(col('Start Date')).alias('month_name'),
        sf.month(col('Start Date')).alias('month'),
        sf.day(col('Start Date')).alias('day'),
        sf.weekday(col('Start Date')).alias('week_day'),
        sf.weekofyear(col('Start Date')).alias('week_of_year'),
        sf.dayofyear(col('Start Date')).alias('day_of_year'),
        sf.when(sf.month(col('Start Date')) <= 6, 1).otherwise(2).alias('semester'),
        sf.quarter(col('Start Date')).alias('trimester'),
        sf.ceil(sf.month(col('Start Date')) / 2).alias('bimester'),
        sf.when( ( (sf.year(col('Start Date')) % 400 == 0) | ((sf.year(col('Start Date')) % 4 == 0) & (sf.year(col('Start Date')) % 100 != 0))), 1).otherwise(0).alias('is_leap_year')
    )

    rates_end_dates = df_rates.select(
        sf.concat_ws("", 
                     sf.year(col('End Date')).cast('string'), 
                     sf.lpad(sf.month(col('End Date')).cast('string'), 2, '0'), 
                     sf.lpad(sf.day(col('End Date')).cast('string'), 2, '0')).cast('int').alias('pk_date'),
        sf.year(col('End Date')).alias('year'),
        sf.monthname(col('End Date')).alias('month_name'),
        sf.month(col('End Date')).alias('month'),
        sf.day(col('End Date')).alias('day'),
        sf.weekday(col('End Date')).alias('week_day'),
        sf.weekofyear(col('End Date')).alias('week_of_year'),
        sf.dayofyear(col('End Date')).alias('day_of_year'),
        sf.when(sf.month(col('End Date')) <= 6, 1).otherwise(2).alias('semester'),
        sf.quarter(col('End Date')).alias('trimester'),
        sf.ceil(sf.month(col('End Date')) / 2).alias('bimester'),
        sf.when( ( (sf.year(col('End Date')) % 400 == 0) | ((sf.year(col('End Date')) % 4 == 0) & (sf.year(col('End Date')) % 100 != 0))), 1).otherwise(0).alias('is_leap_year')
    )

    df = rates_start_dates.union(rates_end_dates).dropDuplicates()

    df = df.orderBy("pk_date")

    df.write.parquet(dim_date_path, mode="overwrite")
    #df.show()
    return dim_date_path


def generate_time_dim(rates_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("delimiter", ";").csv(rates_path, header=True)

    df = df.withColumn("label", lit("HH:MM:SS"))
    df =  df.select(
        sf.concat_ws("", 
                     sf.lpad(sf.hour(col('Start Date')).cast('string'), 2, '0'), 
                     sf.lpad(sf.minute(col('Start Date')).cast('string'), 2, '0'), 
                     sf.lpad(sf.second(col('Start Date')).cast('string'), 2, '0')).cast('int').alias('pk_time'),
        sf.hour(col('Start Date')).alias('hour'),
        sf.minute(col('Start Date')).alias('minute'),
        sf.second(col('Start Date')).alias('second'),
        "label"
    )

    

    df = df.dropDuplicates().orderBy("pk_time")

    df.write.parquet(dim_time_path, mode="overwrite")
    #df.show()
    return dim_time_path

def generate_user_dim(session_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("delimiter", ";").csv(session_path, header=True)

    # junção de submitters e observers como uma unica entidade "usuário"
    submitters = df.select(col("Submitter ID").alias("user_id"), col("Submitted by").alias("name"))
    observers = df.select(col("Observer ID").alias("user_id"), col("Actual Observer Name").alias("name"))
    users = submitters.union(observers)

    # separação entre first e last name
    users = users.withColumn("first_name", split(col("name"), " ").getItem(0))
    users = users.withColumn("last_name", split(col("name"), " ").getItem(1))

    # Padronização dos nomes
    users = normalize_string_column(users, "first_name")
    users = normalize_string_column(users, "last_name")

    # tratamento de valores nulos
    users = users.withColumn("user_id", sf.when((col("user_id").isNull()) | (col("user_id") == ""), -1).otherwise(col("user_id")))
    users = users.withColumn("first_name", sf.when((col("first_name").isNull()) | (col("first_name") == ""), "Unknown").otherwise(col("first_name")))
    users = users.withColumn("last_name", sf.when((col("last_name").isNull()) | (col("last_name") == ""), "Unknown").otherwise(col("last_name")))

    users = users.dropDuplicates()
    
    # geração das surrogate keys
    window = Window.orderBy("user_id")
    users = users.withColumn("sk_user", row_number().over(window))

    users = users.select(
        "sk_user",
        "user_id",
        "first_name",
        "last_name",
    ).orderBy("sk_user")



    users.write.parquet(dim_user_path, mode="overwrite")
    #df.show()
    return dim_user_path

def generate_shower_dim(rates_path: str, meteor_shower_path: str):
    spark = SparkSession.builder.getOrCreate()

    df_rates = spark.read.option("delimiter", ";").csv(rates_path, header=True)
    df_shower = spark.read.option("delimiter", ",").csv(meteor_shower_path, header=True)

    #df_shower.show()

    df_rates = df_rates.withColumnRenamed("Shower", "IAU_code")

    df = df_rates.join(df_shower, "IAU_code", how="inner")

    df = df.select(
        "IAU_code",
        "name",
    ).dropDuplicates()

    df = normalize_string_column(df, "name")

    # geração das surrogate keys
    window = Window.orderBy(lit(1))
    df = df.withColumn("sk_shower", row_number().over(window))

    df = df.select("sk_shower", "IAU_code", "name").orderBy("sk_shower")

    df.write.parquet(dim_shower_path, mode="overwrite")
    #df.show()
    return dim_shower_path

def generate_junk_dim(rates_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("delimiter", ";").csv(rates_path, header=True)

    df = df.select(col("Method")).dropDuplicates()

    window = Window.orderBy(lit(1))
    df = df.withColumn("sk_junk", row_number().over(window))

    df = df.select("sk_junk", "Method").orderBy("sk_junk")

    df.write.parquet(dim_junk_path, mode="overwrite")
    #df.show()
    return dim_junk_path

def generate_fact_observation(rates_path: str, session_path: str):
    spark = SparkSession.builder.getOrCreate()
    df_rates = spark.read.option("delimiter", ";").csv(rates_path, header=True)
    df_session = spark.read.option("delimiter", ";").csv(session_path, header=True)

    df_rates = df_rates.select("Rate ID", "Obs Session ID", "Start Date", "End Date", "Ra", "Decl", "Teff", "F", "Shower", "Method" , "Number")
    df_session = df_session.select("Session ID", "Observer ID", "Submitter ID", "Actual Observer Name", "Submitted by", "City", "Country", "Latitude", "Longitude", "Elevation")

    df = df_rates.join(df_session, df_rates["Obs Session ID"] == df_session["Session ID"], how="inner")

    df = df.withColumn("pk_date_start",
        sf.concat_ws("", 
                     sf.year(col('Start Date')).cast('string'),
                     sf.lpad(sf.month(col('Start Date')).cast('string'), 2, '0'),
                     sf.lpad(sf.day(col('Start Date')).cast('string'), 2, '0')).cast('int')
    )

    df = df.withColumn("pk_date_end",
        sf.concat_ws("", 
                     sf.year(col('End Date')).cast('string'),
                     sf.lpad(sf.month(col('End Date')).cast('string'), 2, '0'),
                     sf.lpad(sf.day(col('End Date')).cast('string'), 2, '0')).cast('int')
    )

    df = df.withColumn("pk_time_start",
        sf.concat_ws("", 
                     sf.hour(col('Start Date')).cast('string'),
                     sf.lpad(sf.minute(col('Start Date')).cast('string'), 2, '0'),
                     sf.lpad(sf.second(col('Start Date')).cast('string'), 2, '0')).cast('int')
    )

    df = df.withColumn("pk_time_end",
        sf.concat_ws("", 
                     sf.hour(col('End Date')).cast('string'),
                     sf.lpad(sf.minute(col('End Date')).cast('string'), 2, '0'),
                     sf.lpad(sf.second(col('End Date')).cast('string'), 2, '0')).cast('int')
    )


    # separa nomes de observador e submitter
    df = df.withColumn("observer_first_name", split(col("Actual Observer Name"), " ").getItem(0))
    df = df.withColumn("observer_last_name", split(col("Actual Observer Name"), " ").getItem(1))
    df = df.withColumn("submitter_first_name", split(col("Submitted by"), " ").getItem(0))
    df = df.withColumn("submitter_last_name", split(col("Submitted by"), " ").getItem(1))

    # normaliza strings
    df = normalize_string_column(df, "observer_first_name")
    df = normalize_string_column(df, "observer_last_name")
    df = normalize_string_column(df, "submitter_first_name")
    df = normalize_string_column(df, "submitter_last_name")

    # ler as dimensões
    dim_local = spark.read.parquet(dim_local_path)
    dim_user = spark.read.parquet(dim_user_path)
    dim_user_aux = spark.read.parquet(dim_user_path)
    dim_shower = spark.read.parquet(dim_shower_path)
    dim_junk = spark.read.parquet(dim_junk_path)

    
    # Join com observador via nome
    df = df.join(dim_user, col("Observer ID") == dim_user['user_id'],"left").select(df['*'], dim_user['sk_user'].alias("observer_id"))

    # Join com submitter via nome
    df = df.join(dim_user_aux, col("Submitter ID") == dim_user_aux['user_id'], "left").select(df['*'], dim_user_aux['sk_user'].alias("submitter_id"))


    # Junta com dim_local
    df = df.join(dim_local, (df["Latitude"] == dim_local["latitude"]) & (df["Longitude"] == dim_local["longitude"]) & (df["Elevation"] == dim_local["elevation_m"]) & (df["City"] == dim_local["raw_city"]), "left").select(df['*'], dim_local['sk_local'])

    # Junta com dim_shower
    df = df.join(dim_shower, df["Shower"] == dim_shower["IAU_code"], "left").select(df['*'], dim_shower['sk_shower'])

    # Junta com junk
    df = df.join(dim_junk, df["Method"] == dim_junk["Method"], "left").select(df['*'], dim_junk['sk_junk'])

    # selecionar apenas as chaves e métricas
    df = df.select(
        col("submitter_id").alias("fk_user_submitter"),
        col("observer_id").alias("fk_user_observer"),
        col("pk_date_start").alias("fk_start_date"),
        col("pk_time_start").alias("fk_start_time"),
        col("pk_date_end").alias("fk_end_date"),
        col("pk_time_end").alias("fk_end_time"),
        col("sk_local"),
        col("sk_shower"),
        col("sk_junk"),
        col("Obs Session ID").alias("id_session"),
        col("Rate ID").alias("id_observation"),
        col("Ra").alias("right_observations/ascention"),
        col("Decl").alias("declination"),
        col("Teff").alias("effective_time"),
        col("F").alias("correction_factor"),
        col("Number").alias("meteors_counted"),
    ).dropDuplicates()

    df.orderBy("id_observation")

    df.write.parquet(fact_observations_path, mode="overwrite")
    #df.show()
    return fact_observations_path


generate_local_dim(session_path, locations_path)
#generate_date_dim(rates_path)
#generate_time_dim(rates_path)
#generate_user_dim(session_path)
#generate_shower_dim(rates_path, meteor_shower_path)
#generate_junk_dim(rates_path)
#generate_fact_observation(rates_path, session_path)
