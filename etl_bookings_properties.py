import os
import pandas as pd
from google.cloud import bigquery
from pandas_gbq import to_gbq

def extract_data():
    print("==> EXTRACT")
    df_bookings = pd.read_csv('./data_ddv/Bookings.csv')
    df_properties = pd.read_csv('./data_ddv/Properties.csv')
    return df_bookings, df_properties

def transform_data(df_bookings, df_properties):
    print("==> TRANSFORM")
    combined_data = pd.merge(df_bookings, df_properties, on="PropertyId", how="left")

    # Imputación de valores nulos
    combined_data['Channel'] = combined_data['Channel'].fillna('Unknown')  
    combined_data['RoomRate'] = combined_data['RoomRate'].fillna(combined_data['RoomRate'].mean())  
    combined_data['Revenue'] = combined_data['Revenue'].fillna(combined_data['Revenue'].mean())  
    combined_data['ADR'] = combined_data['ADR'].fillna(combined_data['ADR'].mean())  
    combined_data['TouristTax'] = combined_data['TouristTax'].fillna(0)  
    combined_data['RealProperty'] = combined_data['RealProperty'].fillna('Unknown')
    combined_data['Capacity'] = combined_data['Capacity'].fillna(0)
    combined_data['Square'] = combined_data['Square'].fillna(combined_data['Square'].mean())
    combined_data['PropertyType'] = combined_data['PropertyType'].fillna('Unknown')
    combined_data['NumBedrooms'] = combined_data['NumBedrooms'].fillna(0)
    combined_data['ReadyDate'] = combined_data['ReadyDate'].fillna("1900-01-01")
    return combined_data

def load_data(data, output_folder="data_udv", output_filename="combined_data.csv"):
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, output_filename)
    data.to_csv(output_path, index=False)
    print("==> LOAD")

def upload_to_bigquery(dataframe, project_id, dataset_id, table_name, if_exists="replace"):
    # Ruta a las credenciales de Google Cloud
    credentials_path = './credentials/credentials.json'

    # Crear un cliente de BigQuery
    client = bigquery.Client.from_service_account_json(credentials_path)

    # Definir el nombre de la tabla
    table_id = f"{project_id}.{dataset_id}.{table_name}"  

    # Cargar el DataFrame a BigQuery
    job = client.load_table_from_dataframe(dataframe, table_id, job_id=None)

    # Esperar a que el trabajo de carga finalice
    job.result()

    print("==> CARGA DB BIGQUERY")

if __name__ == "__main__":
    df_bookings, df_properties = extract_data()
    combined_data = transform_data(df_bookings, df_properties)
    load_data(combined_data)
    upload_to_bigquery(combined_data, 'esoteric-throne-425604-u5', 'dataset_stayunique', 'bookings_info')
