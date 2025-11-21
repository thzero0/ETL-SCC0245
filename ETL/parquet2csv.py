import os
import shutil
from pyspark.sql import SparkSession

def parquet_to_csv_batch(input_dir, output_dir):
    """
    Converte todos os arquivos .parquet dentro de input_dir em arquivos .csv no output_dir.
    Cada .parquet vira um .csv único.
    """
    output_tmp = "../data/output/tmp_csv"
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(input_dir)
    df.coalesce(1).write.csv(output_tmp, mode="overwrite", header=True)
    for tmp_file in os.listdir(output_tmp):
        if tmp_file.startswith("part-") and tmp_file.endswith(".csv"):
            shutil.move(os.path.join(output_tmp, tmp_file), output_dir)
            break
    shutil.rmtree(output_tmp)


dim_local = "../data/output/common/dim_local"
dim_date = "../data/output/common/dim_date"
dim_time = "../data/output/common/dim_time"
dim_user = "../data/output/common/dim_user"
dim_shower = "../data/output/common/dim_shower"
dim_junk = "../data/output/observations/dim_junk"

dim_weather_code = "../data/output/weather/dim_weather_code"

fact_observations = "../data/output/observations/fact_observations"
fact_magnitude = "../data/output/magnitude/fact_magnitude"
fact_weather = "../data/output/weather/fact_weather"


parquet_to_csv_batch(f"{dim_local}.parquet", f"{dim_local}.csv")
parquet_to_csv_batch(f"{dim_date}.parquet", f"{dim_date}.csv")
parquet_to_csv_batch(f"{dim_time}.parquet", f"{dim_time}.csv")
parquet_to_csv_batch(f"{dim_user}.parquet", f"{dim_user}.csv")
parquet_to_csv_batch(f"{dim_shower}.parquet", f"{dim_shower}.csv")
parquet_to_csv_batch(f"{dim_junk}.parquet", f"{dim_junk}.csv")
parquet_to_csv_batch(f"{fact_observations}.parquet", f"{fact_observations}.csv")


parquet_to_csv_batch(f"{fact_magnitude}.parquet", f"{fact_magnitude}.csv")

parquet_to_csv_batch(f"{dim_weather_code}.parquet", f"{dim_weather_code}.csv")
parquet_to_csv_batch(f"{fact_weather}.parquet", f"{fact_weather}.csv")