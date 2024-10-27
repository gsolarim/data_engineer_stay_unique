# Proyecto de Data Engineer

Este proyecto se centra en la recopilación y análisis de datos de propiedades y reservas de Airbnb utilizando técnicas de web scraping. También se muestra un EDA y ETL realizado a 2 archivos de extensión csv. 
A continuación, se presentan los pasos necesarios para configurar el entorno y ejecutar el proyecto.

## 1. Configuración del entorno

Te recomiendo tener instalada la versión de Python 3.12.5.

La puedes buscar aquí: https://www.python.org/downloads/

##### Instalar virtualenv:
```bash
pip install virtualenv
```

##### Crear un entorno virtual
```bash
python -m venv env
```

#### Activar el entorno virtual

##### En Linux/Mac
```bash
source env/bin/activate  
```
##### En Windows
```bash
env\Scripts\activate     
```

##### Instalar las dependencias
```bash
pip install -r requirements.txt
```

## 2. Cómo ejecutar este proyecto

### Estructura de carpetas

- **credentials**: 
  Aquí se encuentran las credenciales de la cuenta de servicio creadas con GCP IAM y con permisos a BigQuery.

- **data_rdv**: 
  Aquí se encuentran los archivos `Bookings.csv` y `Properties.csv` que están sin editar, tal como han sido recibidos, para realizar el EDA y ETL.

- **data_ddv**: 
  Aquí se encuentran los archivos `Bookings.csv` y `Properties.csv` de `data_rdv` después de haber pasado por el EDA (y una pequeña transformación sobre el dataset) en los archivos `eda_bookings.ipynb` y `eda_properties.ipynb`.

- **data_udv**: 
  Aquí se encuentra el merge de los archivos `Bookings.csv` y `Properties.csv` de `data_ddv` en un archivo `combined_data.csv`.

- **imgs**: 
  Aquí puedes alojar imágenes del paso a paso del scrapeo en caso de que las necesites para alguna validación. Puedes utilizar la función `show_screenshot` que se encuentra en `web_scraping_airbnb.py`.

- **web_scraping_outputs**: 
  Aquí podrás encontrar el output del scrapeo realizado a la página de Airbnb en el archivo `airbnb_barcelona.csv`.

### Ejecución de Scripts

#### EDA y ETL

1. **Ejecución del EDA**  
   Primero, se deben ejecutar los archivos **`eda_bookings.ipynb`** y **`eda_properties.ipynb`**.
   Puedes ejecutar directamente todas las celdas de estos notebooks para realizar el Análisis Exploratorio de Datos (EDA) y las transformaciones necesarias.

3. **Ejecución del ETL**  
   Luego, procede a ejecutar el script **`etl_bookings_properties.py`**. Puedes hacerlo utilizando uno de los siguientes comandos en tu terminal:

```bash
python etl_bookings_properties.py
```
o
```bash
python3 etl_bookings_properties.py
```
#### Web Scraping

1. Para realizar pruebas de scrapeo, he proporcionado el archivo **`web_scraping_airbnb_test.ipynb`**. Este archivo te permitirá experimentar y hacer pruebas antes de pasar al archivo principal.

2. Para ejecutar el script de scrapeo, utiliza uno de los siguientes comandos en tu terminal:

```bash
python web_scraping_airbnb.py
```
o
```bash
python3 web_scraping_airbnb.py
```
## 3. Detalles de las decisiones de limpieza de datos.
- Al momento de realizar el EDA en el DataFrame de **Bookings** se observó que el campo **BookingCreatedDate** venía con dos formatos no compatibles entre sí. El primero **03/10/2024** y el segundo **2024-10-03 16:42:13**. Se optó por convertir el primero
  a un formato DateTime ya que es un formato más convencional para trabajar en **DataFrames** y no perder la info sobre las horas del segundo ejemplo.
- Al momento de realizar el EDA en el DataFrame de **Properties** se observó que el campo **PropertyType** venía con dos descripciones muy similares. La primera **Apartment** y la segunda **Apa**. Se infirió que se estaba haciendo referencia a lo mismo
  por lo que se realizó una transformación a la segunda forma para que diga **Apartment**. 

## 4. Descripción del pipeline de ETL implementado.

El proceso ETL (Extract, Transform, Load) en este proyecto se compone de tres etapas principales: extracción de datos, transformación de datos y carga de datos. A continuación se detallan cada una de estas etapas:

### 4.1. Extracción de Datos (Extract)

En esta etapa, se cargan los datos desde los archivos CSV ubicados en la carpeta **data_ddv**. Se utiliza la función **extract_data()** para leer los siguientes archivos:

**1. Bookings.csv**: Contiene información sobre las reservas.

**2. Properties.csv**: Contiene información sobre las propiedades.

Los datos se almacenan en dos DataFrames: **df_bookings** y **df_properties**.

```python
def extract_data():
    print("==> EXTRACT")
    df_bookings = pd.read_csv('./data_ddv/Bookings.csv')
    df_properties = pd.read_csv('./data_ddv/Properties.csv')
    return df_bookings, df_properties
```
### 4.2. Transformación de Datos (Transform)

En esta etapa, se combinan los datos de las reservas y propiedades mediante una unión (merge) en la columna PropertyId. También se realiza la imputación de valores nulos en varias columnas para asegurar la integridad de los datos. 
Se utiliza la función **transform_data(df_bookings, df_properties)** para realizar las siguientes imputaciones:

- Se llena **Channel** con "Unknown" donde hay valores nulos.
- Se sustituye **RoomRate, Revenue, y ADR** con su promedio donde hay valores nulos.
- Se llena **TouristTax** con 0 donde hay valores nulos.
- Se asigna "Unknown" a **RealProperty**, 0 a **Capacity**, y el promedio de **Square** donde hay valores nulos.
- Se asigna "Unknown" a **PropertyType**, 0 a **NumBedrooms**, y "1900-01-01" a **ReadyDate** donde hay valores nulos.

```python
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
```
### 4.3. Carga de Datos (Load) y Upload a BigQuery

En la etapa de carga, los datos transformados se guardan en un archivo CSV en la carpeta **data_udv** utilizando la función **load_data(data)**. El archivo se nombra como **combined_data.csv**.

Además, los datos se cargan en **Google BigQuery** mediante la función **upload_to_bigquery(dataframe, project_id, dataset_id, table_name)**. 
Esta función crea un **cliente de BigQuery** utilizando las credenciales de **Google Cloud** y carga el **DataFrame** combinado en una tabla específica dentro del dataset.

```python
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
```

## 5. Retos

- **Tener que descargar el ChromeDiverManager manualmente:** Para esto se tuvo que investigar una forma de realizarlo de manera automática y se opto por instalarlo en el código.
- **Propiedades con buenas referencias en Airbnb:** Cuando tienen una calificación muy buena dentro de Airbnb se mueven los elementos **XPATH** y no es posible acceder por el mismo que una calificación normal.
  Por lo que se optó por crear dos variables **review_1_booking** y **review_2_booking** para el caso de reviews y luego homologarlo en un campo **review_booking**. Lo mismo se realizó para **raitings**.
- **Datos no completamente limpios al momento del Scrapeo:** Los datos normalmente no estaban tan limpios, se requirió utilizar **RegEx** o expresiones regulares para la limpieza.
- **Elementos del XPATH no tan accesibles:** Los elementos del **XPATH** no son tan accesibles, teniendo cadenas muy largas para la clase u otros atributos. Hubiera sido más sencillo que la página hubiera tenido nombres
  más cortos o un id para los elementos, de todas formas se pudo acceder a los elementos utilizando XPATH y limpiando los campos con **RegEx**.
- **Conexión a BigQuery:** Desconocía el proceso, pero revisando documentación, se logró realizar el llenado de tablas. Se tuvo que configurar primero una cuenta de servicio en IAM de GCP y luego crear un dataset en el proyecto de BigQuery.
  Llevar el archivo credentials.json a mi Mac y setearlo como variable de entorno. Luego ya solo era configurarlo en el código.

  
