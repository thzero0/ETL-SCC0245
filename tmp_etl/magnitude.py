from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import unidecode



session_path = "../data/Session-IMO-VMDB-Year-2022.csv"
rates_path = "../data/Rate-IMO-VMDB-Year-2022.csv"
meteor_shower_path = "../data/IMO_Working_Meteor_Shower_List.csv"
magnitude_path = "../data/2022-Magnitude.csv"

dim_local_path = "../data/output/magnitude/dim_local.parquet"
dim_date_path = "../data/output/magnitude/dim_date.parquet"
dim_time_path = "../data/output/magnitude/dim_time.parquet"
dim_user_path = "../data/output/magnitude/dim_user.parquet"
dim_shower_path = "../data/output/magnitude/dim_shower.parquet"
fact_magnitude_path = "../data/output/magnitude/fact_magnitude.parquet"

# UDF para remover acentos
@udf(returnType=StringType())
def remove_accents(text):
    if text is None:
        return None
    return unidecode.unidecode(text)

def normalize_string_column(df, colname):
    return (
        df.withColumn(f"raw_{colname}", col(colname))  # mantém o valor original
          .withColumn(colname, remove_accents(col(colname)))
          .withColumn(colname, initcap(lower(col(colname))))
          .withColumn(colname, regexp_replace(col(colname), r"\s+", " ")) # Tira vários espaços
    )

def generate_local_dim(csv_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("delimiter", ";").csv(csv_path, header=True)

    # Normaliza as colunas de cidade e país, mas mantém o dado bruto também para eventuais auditorias
    df = normalize_string_column(df, "City")
    df = normalize_string_column(df, "Country")

    # FIltra apenas as cidades “válidas” com regex (apenas letras, espaços e hífen)
    df = df.withColumn(
        "city",
        when(col("City").rlike(r"^[A-Za-zÀ-ÿ\s\-]+$"), col("City"))
        .otherwise(lit("Unknown"))
    )

    # Filtra apenas os países que são "válidos" (apenas letras, espaços e hífen)
    df = df.withColumn(
        "country",
        when(col("Country").rlike(r"^[A-Za-zÀ-ÿ\s\-]+$"), col("Country"))
        .otherwise(lit("Unknown"))
    )

    df = df.withColumn("latitude", col("latitude").cast("double")) \
       .withColumn("longitude", col("longitude").cast("double"))
    
    df = df.withColumn("elevation_km", col("elevation")/1000)


    df = df.select(
        col("City").alias("city"),
        col("Country").alias("country"),
        col("Latitude").alias("latitude"),
        col("Longitude").alias("longitude"),
        col("Elevation").alias("elevation_m"),
        col("elevation_km"),
        col("raw_City").alias("raw_city"),
        col("raw_Country").alias("raw_country")
    ).dropDuplicates()


    df = df.orderBy(["country", "city"])

    window = Window.orderBy(lit(1))
    df = df.withColumn("sk_local", row_number().over(window))

    df = df.select(
        "sk_local",
        "country",
        "city",
        "latitude",
        "longitude",
        "elevation_m",
        "elevation_km",
        "raw_city",
        "raw_country"
    )

    df = df.orderBy('sk_local')
    df.write.parquet(dim_local_path, mode="overwrite")
    return dim_local_path



def generate_date_dim(magnitude_path: str):
    spark = SparkSession.builder.getOrCreate()
    df_magnitude = spark.read.option("delimiter", ";").csv(rates_path, header=True)

    magnitude_start_dates =  df_magnitude.select(
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

    magnitude_end_dates = df_magnitude.select(
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

    df = magnitude_start_dates.union(magnitude_end_dates).dropDuplicates()

    df = df.orderBy("pk_date")

    df.write.parquet(dim_date_path, mode="overwrite")
    #df.show()
    return dim_date_path


def generate_time_dim(magnitude_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("delimiter", ";").csv(magnitude_path, header=True)

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


def generate_shower_dim(magnitude_path: str, meteor_shower_path: str):
    spark = SparkSession.builder.getOrCreate()

    df_mag = spark.read.option("delimiter", ";").csv(magnitude_path, header=True)
    df_shower = spark.read.option("delimiter", ",").csv(meteor_shower_path, header=True)

    #df_shower.show()

    df_mag = df_mag.withColumnRenamed("Shower", "IAU_code")

    df = df_mag.join(df_shower, "IAU_code", how="inner")

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


def generate_fact_observation(magnitude_path: str, session_path: str):
    spark = SparkSession.builder.getOrCreate()
    df_mag = spark.read.option("delimiter", ";").csv(magnitude_path, header=True)
    df_session = spark.read.option("delimiter", ";").csv(session_path, header=True)

    df_session = df_session.select("Session ID", "Observer ID", "Actual Observer Name", "City", "Country", "Latitude", "Longitude", "Elevation")

    df = df_mag.join(df_session, df_mag["Obs Session ID"] == df_session["Session ID"], how="inner")

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

    # normaliza strings
    df = normalize_string_column(df, "observer_first_name")
    df = normalize_string_column(df, "observer_last_name")

    # ler as dimensões
    dim_local = spark.read.parquet(dim_local_path)
    dim_user = spark.read.parquet(dim_user_path)
    dim_shower = spark.read.parquet(dim_shower_path)

    
    # Join com observador via nome
    df = df.join(dim_user, col("Observer ID") == dim_user['user_id'],"left").select(df['*'], dim_user['sk_user'].alias("observer_id"))

    # Junta com dim_local
    df = df.join(dim_local, (df["Latitude"] == dim_local["latitude"]) & (df["Longitude"] == dim_local["longitude"]) & (df["Elevation"] == dim_local["elevation_m"]) & (df["City"] == dim_local["raw_city"]), "left").select(df['*'], dim_local['sk_local'])

    # Junta com dim_shower
    df = df.join(dim_shower, df["Shower"] == dim_shower["IAU_code"], "left").select(df['*'], dim_shower['sk_shower'])

    # selecionar apenas as chaves e métricas
    df = df.select(
        col("observer_id").alias("fk_user_observer"),
        col("pk_date_start").alias("fk_start_date"),
        col("pk_time_start").alias("fk_start_time"),
        col("pk_date_end").alias("fk_end_date"),
        col("pk_time_end").alias("fk_end_time"),
        col("sk_local"),
        col("sk_shower"),
        col("Obs Session ID").alias("id_session"),
        col("Magnitude ID").alias("id_magnitude"),
        col("Mag N6").alias("count_mag_neg_6"),
        col("Mag N5").alias("count_mag_neg_5"),
        col("Mag N4").alias("count_mag_neg_4"),
        col("Mag N3").alias("count_mag_neg_3"),
        col("Mag N2").alias("count_mag_neg_2"),
        col("Mag N1").alias("count_mag_neg_1"),
        col("Mag 0").alias("count_mag_0"),
        col("Mag 1").alias("count_mag_1"),
        col("Mag 2").alias("count_mag_2"),
        col("Mag 3").alias("count_mag_3"),
        col("Mag 4").alias("count_mag_4"),
        col("Mag 5").alias("count_mag_5"),
        col("Mag 6").alias("count_mag_6"),

    ).dropDuplicates()

    df.orderBy("id_magnitude")

    df.write.parquet(fact_magnitude_path, mode="overwrite")
    #df.show()
    return fact_magnitude_path




generate_local_dim(session_path)
generate_date_dim(magnitude_path)
generate_time_dim(magnitude_path)
generate_user_dim(session_path)
generate_shower_dim(magnitude_path, meteor_shower_path)
generate_fact_observation(magnitude_path, session_path)
