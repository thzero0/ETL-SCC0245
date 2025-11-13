import pandas as pd

# Caminho do arquivo CSV
arquivo = "../data/output/weather/fact_weather.csv"

# Lê o CSV
df = pd.read_csv(arquivo)

# Define as chaves para verificar duplicatas
chaves = ["fk_date", "fk_time", "fk_weather_code", "fk_local"]

# Encontra duplicatas com base nessas colunas
duplicatas = df[df.duplicated(subset=chaves, keep=False)]

# Mostra duplicatas (se houver)
if duplicatas.empty:
    print("✅ Nenhuma duplicata encontrada.")
else:
    print("⚠️ Duplicatas encontradas:")
    print(duplicatas.sort_values(by=chaves))
