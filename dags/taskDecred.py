"""
Dag de Pipeline de ETL dos arquivos decred: zip >> BigQuery
"""

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param, ParamsDict
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from google.cloud import bigquery
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud import storage

import pandas as pd

from datetime import datetime
import os 
from zipfile import ZipFile
from io import BytesIO
import logging

import gcsfs




# Defina os parâmetros do DAG (valores padrão podem ser sobrescritos ao iniciar o DAG)
conn_id = "google_cloud_default"
params_dict = {
    "Ano Layout": Param(
            default=" ",
            type="integer",
            description= "Escolher o ano do layout")
    ,"BUCKET_NAME": Param(
            default="k8s-dataita",
            type='string',
            enum=["dataita", "k8s-dataita", "dataita-teste"])
    ,'prefix': Param(
            default='raw/decred/waiting/2020'
            ,type="string"
            ,description="Prefixo do bucket")
    ,'dest_data': Param(
            default='staged/decred/waiting/2020'
            ,type="string"
            ,description="Arquivos parquet")
    ,'dataset': Param(
            default='mds_decred'
            ,type="string"
            ,description= "Dataset destino no BigQuery")
}


params = ParamsDict(params_dict)
hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
credentials = hook.get_credentials()

@dag(
    dag_id='debug_decred_nelio'
    ,start_date=datetime(2025, 2, 1)
    ,schedule= None
    ,doc_md=__doc__
    ,catchup=False
    ,params=params
    ,tags=["prod"]
    ,max_active_tasks=1
)
def decred_etl_duckdb():
    
    start = EmptyOperator(task_id='start')
    

    @task(
        task_id="get_files"
        ,multiple_outputs=True
        )
    def get_files(**kwargs):
        hook = GoogleBaseHook(gcp_conn_id="gcs_default")
        credentials = hook.get_credentials() 
        params : ParamsDict = kwargs["params"]  

        hook = GCSHook(gcp_conn_id=conn_id)
        ## Get all zips in folder e subfolders 
        files = hook.list(params["BUCKET_NAME"], prefix=params["prefix"], delimiter=".zip" )
        list_name_files = [file.split('/')[-1] for file in files]
        logging.info(list_name_files)
        list_period = [f'decred-{file.split("_")[4]}-{file.split("_")[5]}' for file in list_name_files]
        
        metadata_decred = {'file_name': list_name_files, 'period': list_period, 'path_files': files}

        return metadata_decred

    @task
    def extract_path_files(metadata_decred):
        return metadata_decred['path_files']
    
    @task
    def extract_period(metadata_decred):
        return metadata_decred['period']    

    @task(map_index_template='{{ period }}')
    def zip_to_gcs(**kwargs):
        
        context = get_current_context()
        context['decred'] = kwargs['period']


        bucket_name = params["BUCKET_NAME"]


        hook = GCSHook(gcp_conn_id=conn_id)
        files = [kwargs['period']]
          
        path_descompacted_files = f'tmp/{kwargs['period'].split("/")[-1].replace(".zip","")}/'
        # list_dest_paths = list()
        for i in range(len(files)):
            zip_data = BytesIO(hook.download(bucket_name, files[i]))
            with ZipFile(zip_data, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith('.zip'):
                        continue  # Skip directories in the zip file
                    file_data = zip_ref.read(file)
                    print(path_descompacted_files + file)
                    print(logging.info(path_descompacted_files + file))
                    hook.upload(bucket_name,object_name= path_descompacted_files + file, data=file_data)


        return path_descompacted_files


    @task(task_id="get_xcom")    
    def get_xcom(values):
        return list(values)



    @task(map_index_template='{{ source_csv }}',
          )
    def layout_old( **kwargs):

        
        params : ParamsDict = kwargs["params"]
        
        context = get_current_context()
        context['source_csv'] = kwargs['source_csv']


        bucket_name = params["BUCKET_NAME"]
        path_descompacted_files = kwargs['source_csv']
        
        

        colunas2020 = [
        "DS_ORIGEM_INFORMACAO",
        "SQ_REG_0000",
        "SQ_ARQUIVO_ADM_CARTAO",
        "NU_CNPJ_ADMINISTRADORA_ARQUIVO",
        "NO_ADMINISTRADORA_ARQUIVO",
        "NU_CNPJ_LIQUIDANTE_OPERACAO",
        "NO_LIQUIDANTE_OPERACAO",
        "TP_NIVEL_OPERACAO",
        "DS_TIPO_NIVEL",
        "NU_CNPJ_CLIENTE_ORIG",
        "NU_CNPJ_CLIENTE",
        "NU_CPF_CLIENTE",
        "NO_FANTASIA_CLIENTE",
        "NO_RESP_CADASTRO",
        "CO_MCAPT",
        "NU_MCAPT_LOGICO",
        "IN_MCAPT_TERMINAL_PROPRIO",
        "DS_MCAPT_MARCA",
        "NO_MCAPT_TIPO_TECNOLOGIA",
        "DS_OPERACAO_MES",
        "DS_OPERACAO_ANO",
        "DH_OPERACAO",
        "DS_OPERACAO_BANDEIRA_CARTAO",
        "DS_OPERACAO_NSU",
        "DS_OPERACAO_TRANSACAO",
        "CO_OPERACAO_AUT",
        "DS_OPERACAO_TIPO_NATUREZA",
        "CO_NAT_OPERACAO",
        "VL_OPERACAO",
        "VL_CANCELADO",
        "DS_ORIGEM_INFORMACAO.1"
        ]


        
        fs = gcsfs.GCSFileSystem(project='infra-itaborai', token=credentials)
        logging.info(f"lendo arquivos de: gs://{bucket_name}/{path_descompacted_files}*")
        list_files = fs.ls(f"gs://{bucket_name}/{path_descompacted_files}")
        logging.info(f"list_files: {list_files}")
        df_2020 = pd.concat([pd.read_csv(f"gs://{f}", decimal='.',sep=';', encoding="latin1") for f in list_files], ignore_index=True)
        df_2020 = df_2020[colunas2020].rename(columns={"DS_ORIGEM_INFORMACAO.1": "ID_SEFAZ"})
        logging.info(f"df shape: {df_2020.shape}")      

        prefix_dest  = f'{params["dest_data"]}'
        
        
        client = storage.Client(credentials=credentials)
        dest_path_file = os.path.join(prefix_dest, f"{kwargs['source_csv'].split('/')[1]}.parquet")
        logging.info(f'gravando {dest_path_file}')
        bucket = client.bucket(bucket_name)
        # The name assigned to the CSV file on GCS
        blob = bucket.blob(dest_path_file)
        blob.upload_from_string(df_2020.to_parquet(), 'application/octet-stream')

        
        
       
        logging.info(f"Parquet criado em {dest_path_file}")


        return dest_path_file
    @task(map_index_template='{{ source_csv }}',
        )
    def layout_new( **kwargs):


        params : ParamsDict = kwargs["params"]

        context = get_current_context()
        context['source_csv'] = kwargs['source_csv']


        bucket_name = params["BUCKET_NAME"]
        path_descompacted_files = kwargs['source_csv']



        colunas2023 = [
        "DS_ORIGEM_INFORMACAO",
        "SQ_REG_0000",
        "SQ_ARQUIVO_ADM_CARTAO",
        "NU_CNPJ_ADMINISTRADORA_ARQUIVO",
        "NO_ADMINISTRADORA_ARQUIVO",
        "NU_CNPJ_LIQUIDANTE_OPERACAO",
        "NO_LIQUIDANTE_OPERACAO",
        "TP_NIVEL_OPERACAO",
        "DS_TIPO_NIVEL",
        "NU_CNPJ_CLIENTE_ORIG",
        "NU_CNPJ_CLIENTE",
        "NU_CPF_CLIENTE",
        "NO_FANTASIA_CLIENTE",
        "NO_RESP_CADASTRO",
        "CO_MCAPT",
        "NU_MCAPT_LOGICO",
        "IN_MCAPT_TERMINAL_PROPRIO",
        "DS_MCAPT_MARCA",
        "NO_MCAPT_TIPO_TECNOLOGIA",
        "DS_OPERACAO_MES",
        "DS_OPERACAO_ANO",
        "DH_OPERACAO",
        "DS_OPERACAO_BANDEIRA_CARTAO",
        "DS_OPERACAO_NSU",
        "DS_OPERACAO_TRANSACAO",
        "CO_OPERACAO_AUT",
        "DS_OPERACAO_TIPO_NATUREZA",
        "CO_NAT_OPERACAO",
        "VL_OPERACAO",
        "VL_CANCELADO"
        ]

        fs = gcsfs.GCSFileSystem(project='infra-itaborai', token=credentials)
        logging.info(f"lendo arquivos de: gs://{bucket_name}/{path_descompacted_files}*")
        list_files = fs.ls(f"gs://{bucket_name}/{path_descompacted_files}")
        logging.info(f"list_files: {list_files}")
        df_2023 = pd.concat([pd.read_csv(f"gs://{f}", decimal='.',sep=';', encoding="latin1") for f in list_files], ignore_index=True)
        df_2023['SQ_ARQUIVO_ADM_CARTAO'] = 'null'
        df_2023.insert(2, 'SQ_ARQUIVO_ADM_CARTAO', df_2023.pop('SQ_ARQUIVO_ADM_CARTAO'))
        logging.info(f"df shape: {df_2023.shape}")      

        prefix_dest  = f'{params["dest_data"]}'


        client = storage.Client(credentials=credentials)
        dest_path_file = os.path.join(prefix_dest, f"{kwargs['source_csv'].split('/')[1]}.parquet")
        logging.info(f'gravando {dest_path_file}')
        bucket = client.bucket(bucket_name)
        # The name assigned to the CSV file on GCS
        blob = bucket.blob(dest_path_file)
        blob.upload_from_string(df_2023.to_parquet(), 'application/octet-stream')




        logging.info(f"Parquet criado em {dest_path_file}")


        return dest_path_file

    @task(task_id = 'get_xcom2')
    def get_xcom2(values):
        values = [value.replace(f'gs://{params["BUCKET_NAME"]}/', '') for value in values]
        logging.info(f'type values: {type(values)}')
        return values


    @task(task_id='load_parquet_to_bigquery', map_index_template='{{ file_path }}')
    def load_parquet_to_bigquery(file_path):
        logging.info(f'dataset dest {params["dataset"]}')
        logging.info(f'bucket src {params["BUCKET_NAME"]}')
        bq_client = bigquery.Client(credentials=credentials)
        table_ref = f'infra-itaborai.{params["dataset"]}.decred'
        logging.info(f'table_ref: {table_ref}')
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        load_job = bq_client.load_table_from_uri(
            f"gs://{params['BUCKET_NAME']}/{file_path}", table_ref, job_config=job_config
        )
        load_job.result()



    @task(map_index_template='{{ folder_path }}')
    def delete_folder(folder_path):
        
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(params['BUCKET_NAME'])
        blobs = bucket.list_blobs(prefix=folder_path)
        for blob in blobs:
            blob.delete()


    @task.branch(task_id="escolher_ano")
    def escolher_layout():
        context = get_current_context()
        params : ParamsDict = context["params"]
        ano_layout = params["Ano Layout"]
        logging.info(f"ano_layout: {ano_layout}")
        if ano_layout <= 2022:
            return "layout_old"
        else:
            return "layout_new"





    end = EmptyOperator(task_id='end', trigger_rule="none_failed_or_skipped")


    list_files = get_files()
    path_files = extract_path_files(list_files)
    extract_zip = zip_to_gcs.expand(period=path_files)
    get_path_csv_xcom = get_xcom(extract_zip)
    layout_2020 = layout_old()
    layout_2022 = layout_new()
    layout = escolher_layout()
    
    get_path_parquet_xcom = get_xcom2(layout)

    load_tasks= load_parquet_to_bigquery.expand(file_path=get_path_parquet_xcom)

    delete_tmp = delete_folder.expand(folder_path=get_path_csv_xcom)
   
   
    start >> list_files >> path_files >> extract_zip >> get_path_csv_xcom >> layout
    layout >> layout_2022 >> get_path_parquet_xcom >> load_tasks >> delete_tmp >> end
    layout >> layout_2020 >> get_path_parquet_xcom >> load_tasks >> delete_tmp >> end
    



decred_etl_duckdb()