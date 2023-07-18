import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import io
import json
import openpyxl
import xlrd
import functions_framework
import os
import re
import datetime


def eliminar_archivo(bucket_nombre, subdir, archivo_nombre):
    # Crea una instancia del cliente de Storage
    client = storage.Client() 
    # Obtén una referencia al bucket
    bucket = client.get_bucket(bucket_nombre) 
    
    blobs = bucket.list_blobs(prefix=subdir)
    
    # Elimina los archivos en el subdirectorio
    for blob in blobs:
        # Elimina el archivo del bucket
        if archivo_nombre in blob.name:
            print(blob.name)
            blob.delete()
    print("hola MUNDO 3")

def eliminar_tuplas_por_palabra(palabra,file_name):
    # Crea una instancia del cliente de BigQuery
    client = bigquery.Client()

    # Especifica el ID del proyecto y el nombre del dataset y la tabla
    proyecto = "bi-main-335421"
    dataset = "dwh"
    tabla = "ICBC_VOUCHER"

    # Construye la consulta para eliminar las tuplas que coincidan con la palabra en la columna "nombre"
    query = f"""
    DELETE FROM `{proyecto}.{dataset}.{tabla}`
    WHERE `Nombre_Archivo_Fuente` LIKE '%{palabra}%'
    """

    # Ejecuta la consulta
    job = client.query(query)
    job.result()  # Espera a que la consulta se complete

    # Imprime un mensaje indicando la cantidad de tuplas eliminadas
    print(f"Se eliminaron {job.num_dml_affected_rows} tuplas.")
    bucket_name_delete= "bi-main-335421-dataset-prod"
    subdir="procesado"
    eliminar_archivo(bucket_name_delete,subdir, file_name)

def move_object(bucket_name, old_object_path, new_object_path):
    # Create a Cloud Storage client instance
    storage_client = storage.Client()

    # Get the source and destination objects
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(old_object_path)
    destination_blob = bucket.blob(new_object_path)

    # Check if the destination object already exists
    if destination_blob.exists():
        print("The destination object already exists: {}/{}".format(bucket_name, new_object_path))
        return

    # Copy the object to the new destination
    bucket.copy_blob(blob, bucket, new_name=new_object_path)

    # Delete the source object
    blob.delete()

    print("Moved the object from {}/{} to {}/{}".format(bucket_name, old_object_path, bucket_name, new_object_path))

def process_file(file_name, bucket, ahora, año_actual, mes_actual, bucket_name):
    
    aux_file_name = file_name.split(".")[0]
    print("hola")
    print(aux_file_name)

    print(f"SE VALIDARA SI EL ARCHIVO {aux_file_name} SE ENCUENTRA DENTRO DE LA TABLA ICBC_VOUCHER")
    eliminar_tuplas_por_palabra(aux_file_name,file_name)
    print(file_name)

    blob = bucket.blob(file_name)
    print("Processing file: {}".format(file_name))

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=";",
        allow_jagged_rows=True,
        write_disposition="WRITE_APPEND",
    )

    table_id = "bi-main-335421.dwh.ICBC_VOUCHER"

    if file_name.endswith(".xls"):
        df = pd.read_excel(blob.download_as_bytes(), sheet_name='sheet1') 
    elif file_name.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(blob.download_as_bytes()), sheet_name='sheet1', engine='openpyxl') 
    else:
        print("Unsupported file format: {}".format(file_name))
        return

    # Convert date format
    df['Fecha de Nacimiento'] = pd.to_datetime(df['Fecha de Nacimiento'], format='%d/%m/%Y', errors='coerce')
    df['anio'] = df['Fecha de Nacimiento'].dt.year.astype('Int64')
    df['mes'] = df['Fecha de Nacimiento'].dt.month.astype('Int64')

    df['Fecha Carga'] = ahora.date()
    df['Fuente'] = file_name

    csv_data = df.to_csv(index=None, sep=';', encoding='utf-8')

    bq_client = bigquery.Client()
    table_ref = bq_client.get_table(table_id)
    load_job = bq_client.load_table_from_file(
        io.BytesIO(csv_data.encode('utf-8')),
        table_ref,
        job_config=job_config,
    )

    try:
        print("Loading data into table: {}".format(table_id))
        load_job.result()  # Wait for the job to complete
        print("Data load finished")
    except Exception as e:
        print("Error loading data into BigQuery. Error: {} - File: {}".format(str(e), file_name))

    new_file_name = "procesado/{}".format(file_name)
    move_object(bucket_name, file_name, new_file_name)
    print()


        

def kickoff(data, context):
    ahora = datetime.datetime.now()
    año_actual = ahora.year
    mes_actual = ahora.month

    bucket_name = data['bucket']
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()
    blobs_list = [blob.name for blob in blobs if '/' not in blob.name]

    expr_reg = r'\d{4}-\d{2}-\d{2}\.xls$'
    expr_reg_2 = r'\d{4}-\d{2}-\d{2}\.xlsx$'

    for file_name in blobs_list:
        if re.match(expr_reg, file_name):
            process_file(file_name, bucket, ahora, año_actual, mes_actual, bucket_name)
        elif re.match(expr_reg_2, file_name):
            process_file(file_name, bucket, ahora, año_actual, mes_actual, bucket_name)
        else:
            print("Unsupported file format: {}".format(file_name))

@functions_framework.http
def main(request):
    data = {'bucket': 'bi-main-335421-dataset-prod'}
    kickoff(data, None)

if __name__ == '__main__':
    dict={'hola'} 
    main(dict)