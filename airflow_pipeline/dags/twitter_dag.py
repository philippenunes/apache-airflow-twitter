import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timezone
from operators.twitter_operator import TwitterOperator
from os.path import join

# Definição da DAG principal para extração de dados do Twitter
# - Executa diariamente (@daily)
with DAG(dag_id="TwitterDAG", start_date=datetime(2025, 8, 12, tzinfo=timezone.utc), schedule="@daily") as dag:
    
    # Formato de timestamp exigido pela API do Twitter
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    
    # Termo de busca para tweets sobre data science
    query = "data science"
    
    # Operador principal que executa a extração
    # - Salva dados em estrutura organizada por data
    # - Usa variáveis de template do Airflow para nomenclatura dinâmica
    # - data_interval_start/end são fornecidos automaticamente pelo Airflow
    to = TwitterOperator(
        file_path=join("datalake/twitter_datascience",
                       "extract_date={{ ds }}",  # Data no formato YYYY-MM-DD
                       "datascience_{{ ds_nodash }}.json"),  # Data no formato YYYYMMDD
        query=query,
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",  # Início do intervalo
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",      # Fim do intervalo
        task_id="twitter_datascience"
    )