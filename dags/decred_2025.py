from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param, ParamsDict
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from zipfile import ZipFile
from io import BytesIO
import logging
from google.cloud import storage
import pandas as pd
import os
import google.auth
import gcsfs

# Identificador da conexão configurada no Airflow para o GCS
conn_id = "google_cloud_default"

# Definição dos parâmetros do DAG
params_dict = {
    "Usuario": Param(
        default="José Nélio",
        type="string",
        enum=["José Nélio", "Vinicius B. Soares", "Igor Kalon"],
        description="Escolher usuário para download dos arquivos do CNPJ"
    ),
    "Ano Layout": Param(
        default="2020",
        type="string",
        enum=["2020", "2023"],
        description="Escolher o ano do layout: 2020 = (2020 até 2022). 2023 = (a partir de 2023)"
    ),
    "Nome Bucket": Param(
        default="nelio_teste",
        type="string",
        enum=["dataita", "k8s-dataita", "dataita-teste", "nelio_teste"],
        description="Nome do bucket no GCS"
    ),
    "Caminho do Arquivo": Param(
        default="raw/decred/waiting/2020",
        type="string",
        description="Prefixo do bucket onde estão os arquivos"
    ),
    "Destino Arquivo": Param(
        default="staged/decred/waiting/2020",
        type="string",
        description="Destino para os arquivos em formato parquet"
    ),
    "dataset": Param(
        default="mds_decred2",
        type="string",
        description="Dataset de destino no BigQuery"
    )
}

params = ParamsDict(params_dict)
hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
credentials = hook.get_credentials()

@dag(
    dag_id="decrede_2025",
    start_date=datetime(2025, 2, 1),
    schedule=None,
    doc_md=__doc__,
    catchup=False,
    params=params,
    tags=["teste", "decred"],
    max_active_tasks=1
)
def decred_2025():
    
    lista_arquivos = TaskGroup("lista_arquivos", tooltip="Lista Arquivos")
    extrai_metadata = TaskGroup("extrai_metadata", tooltip="Extrai Metadados")
    layout = TaskGroup("layout", tooltip="Layout de Arquivos")

    start = EmptyOperator(task_id="start")
    
    @task(task_id="lista_arquivos", multiple_outputs=True, task_group=lista_arquivos)
    def lista_arquivos():
        context = get_current_context()
        params_context = context["params"]
        nome_bucket = params_context["Nome Bucket"]
        caminho = params_context["Caminho do Arquivo"]
        gcs_hook = GCSHook(gcp_conn_id=conn_id)
        arquivos = gcs_hook.list(bucket_name=nome_bucket, prefix=caminho)
        logging.info("Arquivos encontrados: %s", arquivos)
        return {"arquivos": arquivos}

    @task(task_id="extrai_metadata_arquivos_zip", multiple_outputs=True, task_group=extrai_metadata)
    def extrai_metadata_arquivos_zip(arquivos: dict):
        arquivos_lista = arquivos["arquivos"]
        arquivos_zip = [file for file in arquivos_lista if file.endswith(".zip")]
        nomes_arquivos = [file.split("/")[-1] for file in arquivos_zip]
        list_period = [
            f'decred-{nome.split("_")[4]}-{nome.split("_")[5].replace(".zip", "")}'
            for nome in nomes_arquivos
            if len(nome.split("_")) >= 6
        ]
        logging.info(f"Nomes dos arquivos zip: {nomes_arquivos}")
        logging.info(f"Períodos encontrados: {list_period}")
        return {
            'file_name': nomes_arquivos,
            'period': list_period,
            'path_files': arquivos_zip
        }

    @task(task_id="caminho_arquivos", task_group=extrai_metadata)
    def extract_path_files(metadata_decred):
        return metadata_decred['path_files']

    @task(task_id="periodos", task_group=extrai_metadata)
    def extract_period(metadata_decred):
        return metadata_decred['period']

    @task(task_id="descompacta_arquivo_zip", task_group=extrai_metadata)
    def descompacta_arquivo_zip(path_file: str):
        context = get_current_context()
        params_context = context["params"]
        bucket_name = params_context["Nome Bucket"]

        nome_pasta_periodo = path_file.split("/")[-1].replace(".zip", "")
        caminho_tmp = f"tmp/{nome_pasta_periodo}/"

        gcs_hook = GCSHook(gcp_conn_id=conn_id)

        logging.info(f"Baixando e extraindo: {path_file}")
        zip_data = BytesIO(gcs_hook.download(bucket_name=bucket_name, object_name=path_file))

        with ZipFile(zip_data, 'r') as zip_ref:
            for file in zip_ref.namelist():
                if file.endswith(".zip"):
                    continue
                file_data = zip_ref.read(file)
                caminho_destino = f"{caminho_tmp}{file}"
                gcs_hook.upload(bucket_name=bucket_name, object_name=caminho_destino, data=file_data)
                logging.info(f"Arquivo extraído enviado para: {caminho_destino}")

        return caminho_tmp


    @task(task_id="get_xcom")    
    def get_xcom(values):
        return list(values)




    @task.branch(task_id="escolhe_layout")
    def escolhe_layout():
        context = get_current_context()
        ano_layout = context["params"]["Ano Layout"]
        if ano_layout == "2020":
            return "layout.tarefa_layout_2020"
        elif ano_layout == "2023":
            return "layout.tarefa_layout_2023"
        else:
            return "end" 

    # @task(task_id="tarefa_layout_2020", task_group=layout)
    # def tarefa_layout_2020():
    #     logging.info("Executando lógica para layout de 2020")

    @task(task_id="tarefa_layout_2020", task_group=layout,map_index_template='{{ source_csv }}')
    def tarefa_layout_2020(source_csv: str):


        logging.info("Executando lógica para layout de 2020")
        context = get_current_context()
        params = context["params"]
        caminho_tmp = source_csv
        bucket_name = params["Nome Bucket"]



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

        dtype_dict = {
        "DS_ORIGEM_INFORMACAO": "string",
        "SQ_REG_0000": "string",
        "SQ_ARQUIVO_ADM_CARTAO": "string",
        "NU_CNPJ_ADMINISTRADORA_ARQUIVO": "string",
        "NO_ADMINISTRADORA_ARQUIVO": "string",
        "NU_CNPJ_LIQUIDANTE_OPERACAO": "string",
        "NO_LIQUIDANTE_OPERACAO": "string",
        "TP_NIVEL_OPERACAO": "string",
        "DS_TIPO_NIVEL": "string",
        "NU_CNPJ_CLIENTE_ORIG": "string",
        "NU_CNPJ_CLIENTE": "string",
        "NU_CPF_CLIENTE": "string",
        "NO_FANTASIA_CLIENTE": "string",
        "NO_RESP_CADASTRO": "string",
        "CO_MCAPT": "string",
        "NU_MCAPT_LOGICO": "string",
        "IN_MCAPT_TERMINAL_PROPRIO": "string",
        "DS_MCAPT_MARCA": "string",
        "NO_MCAPT_TIPO_TECNOLOGIA": "string",
        "DS_OPERACAO_MES": "string",
        "DS_OPERACAO_ANO": "string",
        "DH_OPERACAO": "string",
        "DS_OPERACAO_BANDEIRA_CARTAO": "string",
        "DS_OPERACAO_NSU": "string",
        "DS_OPERACAO_TRANSACAO": "string",
        "CO_OPERACAO_AUT": "string",
        "DS_OPERACAO_TIPO_NATUREZA": "string",
        "CO_NAT_OPERACAO": "string",
        "VL_OPERACAO": "float64",  
        "VL_CANCELADO": "float64",
        "DS_ORIGEM_INFORMACAO.1": "string",
        "ID_SEFAZ": "string"
        }


        fs = gcsfs.GCSFileSystem(project='infra-itaborai', token=credentials)
        logging.info(f"lendo arquivos de: gs://{bucket_name}/{caminho_tmp}*")
        list_files = [ f for f in fs.ls(f"gs://{bucket_name}/{caminho_tmp}") if f.endswith('.csv')]
        logging.info(f"list_files: {list_files}")
        df = pd.concat([pd.read_csv(f"gs://{f}", decimal='.',sep=';', encoding="latin1",dtype=dtype_dict) for f in list_files], ignore_index=True)
        logging.info(f"Escolhido ano Layout: {params['Ano Layout']}")
        df = df[colunas2020].rename(columns={"DS_ORIGEM_INFORMACAO.1": "ID_SEFAZ"})
        logging.info(f"df shape: {df.shape}")
        prefix_dest  = f'{params["dest_data"]}'


        client = storage.Client(credentials=credentials)
        dest_path_file = os.path.join(prefix_dest, f"{'source_csv'}.split('/')[1].parquet")
        logging.info(f'gravando {dest_path_file}')
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(dest_path_file)
        blob.upload_from_string(df.to_parquet(), 'application/octet-stream')

        logging.info(f"Parquet criado em {dest_path_file}")

        return dest_path_file




        # credentials = hook.get_credentials()
        # gcs_hook = GCSHook(gcp_conn_id=conn_id)

        # logging.info(f"Lendo arquivos de: gs://{bucket_name}/{caminho_tmp}*")
        

        # credentials = hook.get_credentials()
        # fs = gcsfs.GCSFileSystem(project='infra-itaborai', token=credentials,access='read_write')
        # list_files = gcs_hook.list(bucket_name=bucket_name, prefix=caminho_tmp)

        # logging.info(f"Arquivos CSV encontrados list_files: {list_files}")
        # logging.info(f"Arquivos CSV encontrados csv_files: {list_files}")

        # df = pd.concat([
        #     pd.read_csv(f"gs://{f}", decimal='.', sep=';', encoding="latin1", dtype=dtype_dict) for f in list_files], ignore_index=True)

        # df = df[colunas2020].rename(columns={"DS_ORIGEM_INFORMACAO.1": "ID_SEFAZ"})
        # logging.info(f"DataFrame final possui shape: {df.shape}")

        # prefix_dest = params["Destino Arquivo"]
        # dest_path_file = os.path.join(prefix_dest, f"{source_csv.split('/')[1]}.parquet")

        # with fs.open(f"{bucket_name}/{dest_path_file}", 'wb') as f:
        #     df.to_parquet(f, index=False)



        # logging.info(f"Parquet criado em: gs://{bucket_name}/{dest_path_file}")



        # return dest_path_file
    



    @task(task_id="tarefa_layout_2023", task_group=layout)
    def tarefa_layout_2023():
        logging.info("Executando lógica para layout de 2023")

    end = EmptyOperator(task_id="end",trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)


    # Encadeamento
    arquivos = lista_arquivos()
    metadata = extrai_metadata_arquivos_zip(arquivos)
    path_files = extract_path_files(metadata)
    periodos = extract_period(metadata)
    unzip = descompacta_arquivo_zip.expand(path_file=path_files)
    get_path_csv_xcom = get_xcom(unzip)
    branch = escolhe_layout()
    # layout_2020 = tarefa_layout_2020()
    layout_2020 = tarefa_layout_2020.expand(source_csv=get_path_csv_xcom)

    layout_2023 = tarefa_layout_2023()

    start >> arquivos >> metadata >> [path_files, periodos] >> unzip >> get_path_csv_xcom >> branch
    branch >> layout_2020 >> end
    branch >> layout_2023 >> end

decred_2025_dag = decred_2025()