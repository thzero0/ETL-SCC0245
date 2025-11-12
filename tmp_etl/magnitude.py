from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import unidecode

from utils import compute_locations, normalize_text, normalize_string_column
from utils import session_path, magnitude_path


dim_local_path = "../data/output/common/dim_local.parquet"
dim_date_path = "../data/output/common/dim_date.parquet"
dim_time_path = "../data/output/common/dim_time.parquet"
dim_user_path = "../data/output/common/dim_user.parquet"
dim_shower_path = "../data/output/common/dim_shower.parquet"
fact_magnitude_path = "../data/output/magnitude/fact_magnitude.parquet"


def generate_fact_magnitude(magnitude_path: str, session_path: str):
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

    # separa nomes de observador e submitter
    df = df.withColumn("observer_first_name", split(col("Actual Observer Name"), " ").getItem(0))
    df = df.withColumn("observer_last_name", split(col("Actual Observer Name"), " ").getItem(1))

    # normaliza strings
    df = normalize_string_column(df, "observer_first_name")
    df = normalize_string_column(df, "observer_last_name")

    # ler as dimensões
    dim_local = spark.read.parquet(dim_local_path)
    dim_user = spark.read.parquet(dim_user_path)
    dim_time = spark.read.parquet(dim_time_path)
    dim_time_aux = spark.read.parquet(dim_time_path)
    dim_shower = spark.read.parquet(dim_shower_path)

    
    # Join com observador via nome
    df = df.join(dim_user, col("Observer ID") == dim_user['user_id'],"left").select(df['*'], dim_user['sk_user'].alias("observer_id"))
    df = df.withColumn("observer_id", sf.coalesce(col("observer_id"), lit(-1))) # caso não encontre, atribui chave -1 (Desconhecido)

    # Junta com dim_time para Start Date
    df = df.join(dim_time, (sf.hour(df["Start Date"]) == dim_time["hour"]) & 
                             (sf.minute(df["Start Date"]) == dim_time["minute"]) & 
                             (sf.second(df["Start Date"]) == dim_time["second"]), "left").select(df['*'], dim_time['sk_time'].alias("pk_time_start"))
    df = df.withColumn("pk_time_start", sf.coalesce(col("pk_time_start"), lit(-1)))

    # Junta com dim_time para End Date
    df = df.join(dim_time_aux, (sf.hour(df["End Date"]) == dim_time_aux["hour"]) & 
                             (sf.minute(df["End Date"]) == dim_time_aux["minute"]) & 
                             (sf.second(df["End Date"]) == dim_time_aux["second"]), "left").select(df['*'], dim_time_aux['sk_time'].alias("pk_time_end"))
    df = df.withColumn("pk_time_end", sf.coalesce(col("pk_time_end"), lit(-1)))


    # Junta com dim_local
    df = df.join(dim_local, (df["Latitude"] == dim_local["latitude"]) & (df["Longitude"] == dim_local["longitude"]) & (df['Elevation'] == dim_local['elevation_m']), "left").select(df['*'], dim_local['sk_local'])
    df = df.withColumn("sk_local", sf.coalesce(col("sk_local"), lit(-1)))

    # Junta com dim_shower
    df = df.join(dim_shower, df["Shower"] == dim_shower["IAU_code"], "left").select(df['*'], dim_shower['sk_shower'])
    df = df.withColumn("sk_shower", sf.coalesce(col("sk_shower"), lit(-1)))

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


generate_fact_magnitude(magnitude_path, session_path)
