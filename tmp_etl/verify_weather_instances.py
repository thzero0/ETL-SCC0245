import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import pandas

# --- 1. Criar dados de exemplo em um arquivo CSV ---
# (Em um cenário real, você pularia esta etapa e usaria seu arquivo existente)

# --- 2. Iniciar a Sessão Spark ---
spark = SparkSession.builder.appName("GroupAndCountObservations").getOrCreate()

file_path = "../data/Session-IMO-VMDB-Year-2022.csv"

# --- 3. Ler o DataFrame ---
# É crucial configurar as opções corretas para o seu formato
df = spark.read.option("delimiter", ";").csv(file_path,header=True,inferSchema=True)

df_with_timestamp = df.withColumn(
    "Timestamp",
    F.to_timestamp(F.col("Start Date"), "yyyy-MM-dd HH:mm:ss")
)

df_with_parts = df_with_timestamp.withColumn("Ano", F.year("Timestamp")) \
                                 .withColumn("Mês", F.month("Timestamp")) \
                                 .withColumn("Dia", F.dayofmonth("Timestamp")) \
                                 .withColumn("Hora", F.hour("Timestamp"))


grouped_df = df_with_parts.groupBy(
    "Ano",
    "Mês",
    "Dia",
    "Hora",
    "Latitude",
    "Longitude",
    "Elevation"
)

count_df = grouped_df.count()

# --- 6. Exibir Resultado ---
print("--- Resultado Final: Agrupado e Contado ---")
# Ordenamos para ver o grupo duplicado (count=2)
count_df = count_df.orderBy(F.col("count").desc())
count_df.show(truncate=False)

# AVISO: SÓ FAÇA ISSO SE count_df FOR PEQUENO!
try:
    pandas_df = count_df.toPandas()
    
    # Agora usamos o .to_csv() do Pandas, que salva um arquivo único
    pandas_df.to_csv("grouped_data.csv", index=False)
    
    print("Arquivo 'grouped_data.csv' salvo com sucesso!")

except Exception as e:
    print(f"Erro ao coletar para Pandas. O DataFrame é muito grande? {e}")

# --- 7. Limpeza ---
spark.stop()