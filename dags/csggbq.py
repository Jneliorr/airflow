import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

@dag(
    dag_id="csg_gcs_to_bigquery",
    schedule=None,
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=["Teste", "Estudo", "GCS", "BigQuery"],
    params={
        "Escolher_bucket": Param(
            default="dataita",
            type='string',
            enum=["dataita", "k8s-dataita", "dataita-teste"]
        ),
        "Escolher_Tabela": Param(
            "teste",
            type="string",
            title="Nome da Tabela",
            description="Nome da tabela no BigQuery"
        ),
        "Criar_Tabela": Param(
            default="modify",
            type="string",
            title="Nome da Tabela",
            description="Nome da tabela no BigQuery"
        ),
        "Valor_Filtro": Param(
            default=4000,
            type="integer",
            title="Valor do filtro",
            description="Valor numérico para filtrar os dados da coluna VALOR"
        )
    },
    doc_md="Escolher nome da tabela"
)
def main():

    # Grupos de tarefas
    test_conexion = TaskGroup("test_conexion", tooltip="Testar Conexão com o GCS")
    transform = TaskGroup("transforma", tooltip="Transformar os dados")
    criacao_tabela = TaskGroup("criacao_tabela", tooltip="Cria tabela no BigQuery")
    vl_liquido = 4000  # Valor de comparação para o branch

    # Contexto de execução
    # Task de início
    start = EmptyOperator(
        task_id='inicio',
        task_group=test_conexion
    )

    # Validar conexão com GCS
    @task(
        task_id="validar_conexao_gcs",
        doc_md="Valida a conexão com o GCS",
        task_group=test_conexion
    )
    def validar_conexao_gcs():
        try:
            hook = GCSHook(gcp_conn_id='gcs_default')
            client = hook.get_conn()
            buckets = list(client.list_buckets())
            logging.info(f"Conexão validada! Buckets acessíveis: {len(buckets)}")
            return True
        except Exception as e:
            logging.error(f"Erro na conexão GCS: {str(e)}")
            raise

    # Operador de carregamento GCS → BigQuery
    carregar_bigquery = GCSToBigQueryOperator(
        task_id='carregar_para_bigquery',
        bucket="{{ params.Escolher_bucket }}",
        source_objects=['spark/testes_empresa.csv'],
        destination_project_dataset_table="infra-itaborai.teste2.empresas_{{ params.Escolher_Tabela }}",
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        field_delimiter=';',
        skip_leading_rows=1,
        gcp_conn_id='gcs_default'
    )

    # Transformação de dados
    @task(
        task_id="transformar_dados",
        doc_md="Realiza a transformação dos dados",
        task_group=transform
    )
    def transformar_dados():
        context = get_current_context()
        tabela_origem = context["params"]["Escolher_Tabela"]
        tabela_destino = context["params"]["Criar_Tabela"]
        logging.info(f"Transformando dados de {tabela_origem} para {tabela_destino}")

        query = f"""
            CREATE OR REPLACE TABLE infra-itaborai.teste2.empresas_{tabela_destino} AS 
            SELECT CNPJ_BASICO, SUM(CAPITAL_SOCIAL) AS VALOR 
            FROM infra-itaborai.teste2.empresas_{tabela_origem} 
            GROUP BY CNPJ_BASICO
        """

        hook = BigQueryHook(gcp_conn_id='gcs_default')
        client = hook.get_client()
        client.query(query).result()
        logging.info("Tabela transformada com sucesso!")
        return "Transformação concluída"

    # Criação da tabela: VALOR MAIOR
    @task(task_id="criar_tabela_valor_maior", task_group=criacao_tabela)
    def criar_tabela_valor_maior():
        context = get_current_context()
        tabela_origem = context["params"]["Criar_Tabela"]
        valor_filtro = context["params"]["Valor_Filtro"]

        query = f"""
            CREATE OR REPLACE TABLE infra-itaborai.teste2.empresas_valor_maior AS
            SELECT * FROM infra-itaborai.teste2.empresas_{tabela_origem}
            WHERE VALOR <= {valor_filtro}
        """

        hook = BigQueryHook(gcp_conn_id='gcs_default')
        client = hook.get_client()
        client.query(query).result()
        logging.info("Tabela 'empresas_valor_maior' criada.")
        return "Tabela maior criada"

    # Criação da tabela: VALOR MENOR
    @task(task_id="criar_tabela_valor_menor", task_group=criacao_tabela)
    def criar_tabela_valor_menor():
        context = get_current_context()
        tabela_origem = context["params"]["Criar_Tabela"]
        valor_filtro = context["params"]["Valor_Filtro"]

        query = f"""
            CREATE OR REPLACE TABLE infra-itaborai.teste2.empresas_valor_menor AS
            SELECT * FROM infra-itaborai.teste2.empresas_{tabela_origem}
            WHERE VALOR <= {valor_filtro}
        """

        hook = BigQueryHook(gcp_conn_id='gcs_default')
        client = hook.get_client()
        client.query(query).result()
        logging.info("Tabela 'empresas_valor_menor' criada.")
        return "Tabela menor criada"

    # Task de branch (deve vir antes da instância das tasks que ela usa)
    @task.branch(task_id="branch_filter_valor", task_group=transform)
    def branch_filter_valor():
        context = get_current_context()
        valor = context["params"]["Valor_Filtro"]
        logging.info(f"Valor escolhido: {valor}")

        if valor <= vl_liquido:
            return "criacao_tabela.criar_tabela_valor_menor"
        else:
            return "criacao_tabela.criar_tabela_valor_maior"

    # Instanciando as tasks
    validacao = validar_conexao_gcs()
    transformacao = transformar_dados()
    branch = branch_filter_valor()
    maior = criar_tabela_valor_maior()
    menor = criar_tabela_valor_menor()
    fim = EmptyOperator(task_id='end', trigger_rule="none_failed_or_skipped")

    # Orquestração
    start >> validacao >> carregar_bigquery >> transformacao >> branch
    branch >> maior >> fim
    branch >> menor >> fim

dag_instance = main()