import logging
from airflow import DAG
from airflow.decorators import dag, task
from airflow.sdk import Asset, chain, Param, dag, task
from pendulum import datetime, duration
from airflow.hooks.base import BaseHook
from minio import Minio
from datetime import datetime, date

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import unidecode

# logger do Airflow
t_log = logging.getLogger("airflow.task")

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
          .withColumn(colname, regexp_replace(col(colname), r"\s+", " "))
          .withColumn(colname, split(col(colname), ',').getItem(0))  # Casos em que as pessoas armazenaram "<Cidade> , <Bairro>, <Rua> ...." vamos filtrar apenas a cidade mesmo
    )


@dag(
    dag_id="etl_Observations",
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    default_args={
        "owner": "Grupo6",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["Observations", "ETL"],
    is_paused_upon_creation=False,
)
def etl_Observations():
    """
    DAG que cria uma instância do MinIO e faz upload de arquivos locais.
    """
    # Task to verify if the dag can access the minio
    @task
    def verify_minio_is_up():
        conn = BaseHook.get_connection("minio_default")

        # Extrai dados da conexão
        endpoint = conn.host
        access_key = conn.login
        secret_key = conn.password

        # Verifica se o host tem protocolo (por segurança)
        endpoint = endpoint.replace("http://", "").replace("https://", "")

        # Cria cliente MinIO
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        # Verifica se o serviço está ativo
        try:
            client.list_buckets()
            t_log.info(f"MinIO at {endpoint} is reachable!")
        except Exception as e:
            t_log.error(f"Cannot connect to MinIO: {e}")
            raise

    @task 
    def extract_SessionIMO():
        t_log.info("Extracting SessionIMO data...")
        conn = BaseHook.get_connection("minio_default")

        endpoint = conn.host
        access_key = conn.login
        secret_key = conn.password

        endpoint = endpoint.replace("http://", "").replace("https://", "")

        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        filename = "Session-IMO-VMDB-Year-2022.csv"
        bucket_name = "raw-data"

        client.fget_object(bucket_name, filename, f"/tmp/{filename}") # salva o arquivo no diretório /tmp (DENTRO DO CONTAINER!)

        spark = SparkSession.builder.getOrCreate()
        df_sessions = spark.read.option("delimiter", ";").csv(f"/tmp/{filename}", header=True)

        output_path = "/tmp/sessions.parquet"
        df_sessions.write.parquet(output_path, mode="overwrite")

        #t_log.info("\n\nSpark dataframe type: %s\n\n", type(df_sessions))
        return output_path
    

    @task
    def extract_RateIMO():
        t_log.info("Extracting RateIMO data...")
        conn = BaseHook.get_connection("minio_default")

        endpoint = conn.host
        access_key = conn.login
        secret_key = conn.password

        endpoint = endpoint.replace("http://", "").replace("https://", "")

        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        filename = "Rate-IMO-VMDB-Year-2022.csv"
        bucket_name = "raw-data"

        client.fget_object(bucket_name, filename, f"/tmp/{filename}")

        spark = SparkSession.builder.getOrCreate()
        df_rates = spark.read.option("delimiter", ";").csv(f"/tmp/{filename}", header=True)

        output_path = "/tmp/rates.parquet"
        df_rates.write.parquet(output_path, mode="overwrite")

        return output_path
    
    @task
    def extract_MeteoShowerIMO():
        t_log.info("Extracting MeteoShowerIMO data...")
        conn = BaseHook.get_connection("minio_default")

        endpoint = conn.host
        access_key = conn.login
        secret_key = conn.password

        endpoint = endpoint.replace("http://", "").replace("https://", "")

        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        filename = "IMO_Working_Meteor_Shower_List.csv"
        bucket_name = "raw-data"

        client.fget_object(bucket_name, filename, f"/tmp/{filename}")

        spark = SparkSession.builder.getOrCreate()
        df_showers = spark.read.option("delimiter", ";").csv(f"/tmp/{filename}", header=True)

        output_path = "/tmp/showers.parquet"
        df_showers.write.parquet(output_path, mode="overwrite")

        return output_path

    @task
    def translate_to_local_dim(session_path: str):
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.option("delimiter", ";").parquet(session_path)

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

        output_path = "/tmp/dimensions/dim_local.parquet"
        df.write.parquet(output_path, mode="overwrite")

        t_log.info("\n\n\n\nFinal dim_local dataframe: %s\n\n\n\n", df.show())
        return output_path

    @task
    def translate_to_session_dim(sessions_path: str):
        t_log.info("Creating dimension Session...")

    @task
    def translate_to_user_dim(sessions_path: str):
        t_log.info("Creating dimension User...")

    @task
    def translate_to_date_dim(sessions_path: str, rates_path: str):
        t_log.info("Creating dimension Date...")

    @task
    def translate_to_time_dim(sessions_path: str, rates_path: str):
        t_log.info("Creating dimension Time...")

    @task
    def translate_to_meteo_shower_dim(showers_path: str):
        t_log.info("Creating dimension MeteoShower...")

    @task
    def translate_to_junk_dim(sessions_path: str, rates_path: str, showers_path: str):
        t_log.info("Creating dimension Junk...")

    @task
    def translate_to_fact_observation(sessions_path: str, rates_path: str, showers_path: str):
        t_log.info("Creating fact Observation...")

    verify_minio = verify_minio_is_up()
    E_rates = extract_RateIMO()
    E_sessions = extract_SessionIMO()
    E_showers = extract_MeteoShowerIMO()

    verify_minio >> [E_sessions, E_rates, E_showers]

    translate_to_local_dim(E_sessions)
    translate_to_session_dim(E_sessions)
    translate_to_user_dim(E_sessions)
    translate_to_date_dim(E_sessions, E_rates)
    translate_to_time_dim(E_sessions, E_rates)
    translate_to_meteo_shower_dim(E_showers)
    translate_to_junk_dim(E_sessions, E_rates, E_showers)
    translate_to_fact_observation(E_sessions, E_rates, E_showers)



    


etl_Observations()
