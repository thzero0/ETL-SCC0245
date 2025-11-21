from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import math
import pandas as pd
import time
import unidecode 
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType
import unidecode
import os 
import glob

session_path = "../data/Sessions/"
rates_path = "../data/Observations/"
magnitude_path = "../data/Magnitudes/"
weather_path = "../data/Weather/"

weather_code_path = "../data/weather_description.csv"
locations_path = "../data/locations.csv"
meteor_shower_path = "../data/IMO_Working_Meteor_Shower_List.csv"

geolocator = Nominatim(user_agent="Obtain_Locations_ETL")



def normalize_text(text: str) -> str:
    text = str(text)
    text = unidecode.unidecode(text)
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    text = text.title()

    return text

def getDistanceFromLatLonInKm(lat1: float, lon1: float, lat2: float, lon2: float):
    R = 6371
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)

    tmp = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2) * math.sin(dLon/2)
    tmp2 = 2 * math.atan2(math.sqrt(tmp), math.sqrt(1-tmp))
    distance = R * tmp2 

    return distance

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = sf.radians(lat2 - lat1)
    dlon = sf.radians(lon2 - lon1)
    a = sf.pow(sf.sin(dlat / 2), 2) + sf.cos(sf.radians(lat1)) * sf.cos(sf.radians(lat2)) * sf.pow(sf.sin(dlon / 2), 2)
    c = 2 * sf.atan2(sf.sqrt(a), sf.sqrt(1 - a))
    return R * c

def compute_locations(path_locations: str, path_session: str):

    # Conjunto de localizações já conhecidas
    known_locations = pd.read_csv(path_locations, sep=",")

    known_coords = set(zip(known_locations["Latitude"], known_locations["Longitude"]))

    # Lê todas as localizações a serem descobertas
    to_be_discovered_files = glob.glob(os.path.join(path_session , "*.csv"))
    li = []
    for filename in to_be_discovered_files:
        to_be_discovered = pd.read_csv(filename, index_col=None, header=0, sep=';')
        li.append(to_be_discovered)
    to_be_discovered = pd.concat(li, axis=0, ignore_index=True)

    print(f"Total de localizações a serem descobertas: {len(to_be_discovered)}")


    
    # Filtra apenas Latitude e Longitude
    to_be_discovered = to_be_discovered[['Latitude', 'Longitude']]
    
    new_locations_list = []

    cnt_requests = 0

    for index, row in to_be_discovered.iterrows():
            
        lat_atual, lon_atual = row['Latitude'], row['Longitude']
        
        # Pula valores nulos
        if pd.isna(lat_atual) or pd.isna(lon_atual):
            continue

        # Flag para controlar se encontramos um local próximo
        found_near = False

        # 1. Checa contra as localizações já conhecidas no nosso csv
        if (lat_atual, lon_atual) in known_coords:
            continue

        # 3. Se não encontrou em NENHUM lugar, usa a API
        if not found_near:
            print(f"Chamando API para lat={lat_atual} & lon={lon_atual}.")
            cnt_requests += 1
            try:
                location = geolocator.reverse((lat_atual, lon_atual), timeout=10, exactly_one=True, language='en', zoom=18)
                
                if location and location.raw:
                    address = location.raw['address']
                    print(address)

                    city = normalize_text(address.get('city') or address.get('town') or 'Unknown')
                    village_or_hamlet = normalize_text(address.get('village') or address.get('hamlet') or 'Unknown')
                    county = normalize_text(address.get('county') or 'Unknown')
                    state = normalize_text(address.get('state') or 'Unknown')
                    country = normalize_text(address.get('country', 'Unknown'))
                    country_code = address.get('country_code', '').upper()

                    new_location_data = {
                        'Latitude': lat_atual,
                        'Longitude': lon_atual,
                        'City': city,
                        'Village_or_Hamlet': village_or_hamlet,
                        'County': county,
                        'State': state,
                        'Country': country,
                        'CountryCode': country_code
                    }

                    
                    # Adiciona para a lista de novas localizações
                    known_coords.add((lat_atual, lon_atual))
                    new_locations_list.append(new_location_data)
                else:
                    print(f"Nenhum resultado da API para ({lat_atual}, {lon_atual}).")
                    new_location_data = {
                        'Latitude': lat_atual,
                        'Longitude': lon_atual,
                        'City': 'Unknown',
                        'Village_or_Hamlet': 'Unknown',
                        'County': 'Unknown',
                        'State': 'Unknown',
                        'Country': 'Unknown',
                        'CountryCode': 'Unknown'
                    }
                    known_coords.add((lat_atual, lon_atual))
                    new_locations_list.append(new_location_data)

                
            except GeocoderTimedOut:
                print("GeocoderTimedOut: Pulando esta localização.")
                continue
            except Exception as e:
                print(f"Erro ao processar ({lat_atual}, {lon_atual}): {e}")
                continue
            finally:
                # limite de requisições do nominatim 
                time.sleep(1.1)

        # Escreve resultados parciais a cada 100 requisições
        if cnt_requests > 50:
            print("Escrevendo resultados parciais no arquivo de localizações...")
            if new_locations_list:
                new_locations_df = pd.DataFrame(new_locations_list)
                final_locations = pd.concat([known_locations, new_locations_df], ignore_index=True)
                final_locations.to_csv(locations_path, index=False)
                print(f"Escreveu {len(new_locations_list)} novas localizações no arquivo.")
                new_locations_list = []
                known_locations = pd.read_csv(path_locations, sep=",")
            cnt_requests = 0

    # 4. Atualiza o csv (só se teve mudança)
    if new_locations_list:
        print("Escrevendo resultados finais no arquivo de localizações...")
        new_locations_df = pd.DataFrame(new_locations_list)
        final_locations = pd.concat([known_locations, new_locations_df], ignore_index=True)
        # Ordena por pais, estado, cidade
        try:
            final_locations = final_locations.sort_values(by=['Country', 'State', 'City'])
        except Exception as e:
            print(f"Erro ao ordenar localizações: {e}")
        final_locations.to_csv(locations_path, index=False)

    return None

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