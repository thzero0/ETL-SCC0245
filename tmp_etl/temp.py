import pandas as pd

# Caminho do seu arquivo CSV
csv_path = "../data/output/fact_observations.csv"

# Lê o CSV
df = pd.read_csv(csv_path)

# Verifica duplicatas na coluna 'id_observation'
duplicated_mask = df["id_observation"].duplicated(keep=False)

# Filtra apenas as linhas com valores repetidos
duplicated_rows = df[duplicated_mask]

# Mostra os índices das linhas duplicadas
print("Índices com id_observation repetido:")
print(duplicated_rows.index.tolist())

# (Opcional) mostrar os valores duplicados e seus índices
print("\nDetalhes das duplicatas:")
print(duplicated_rows[["id_observation"]])
