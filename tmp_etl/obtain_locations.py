from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import math
import pandas as pd
import time
import unidecode 
import re

geolocator = Nominatim(user_agent="Obtain_Locations_ETL")

# Threshold da distancia mínima entre dois locais, se a distancia for menor que essa, pega uma localização já existente
# Distância em Km
threshold = 10

locations_path = "../data/locations.csv"
session_path = "../data/Session-IMO-VMDB-Year-2022.csv"

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

def compute_locations():

    known_locations = pd.read_csv(locations_path, sep=",")

    to_be_discovered = pd.read_csv(session_path, sep=";")
    to_be_discovered = to_be_discovered[['Latitude', 'Longitude']]
    
    new_locations_list = []

    for index, row in to_be_discovered.iterrows():
            
        lat_atual, lon_atual = row['Latitude'], row['Longitude']
        
        # Flag para controlar se encontramos um local próximo
        found_near = False

        # 1. Checa contra as localizações já conhecidas no nosso csv
        if not known_locations.empty:
            for _, known_row in known_locations.iterrows():
                distance = getDistanceFromLatLonInKm(lat_atual, lon_atual, known_row['Latitude'], known_row['Longitude'])
                if distance <= threshold:
                    found_near = True
                    break

        # 2. Se não encontrou, checa as localizações DESCOBERTAS nessa chamada de função
        if not found_near and new_locations_list:
            for new_loc in new_locations_list:
                distance = getDistanceFromLatLonInKm(lat_atual, lon_atual, new_loc['Latitude'], new_loc['Longitude'])
                if distance <= threshold:
                    found_near = True
                    break

        # 3. Se não encontrou em NENHUM lugar, usa a API
        if not found_near:
            print(f"Chamando API para lat={lat_atual} & lon={lon_atual}.")
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

    # 4. Atualiza o csv (só se teve mudança)
    if new_locations_list:
        new_locations_df = pd.DataFrame(new_locations_list)
        final_locations = pd.concat([known_locations, new_locations_df], ignore_index=True)
        final_locations.to_csv(locations_path, index=False)

    return None

compute_locations()