import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path
import json

class TwitterOperator(BaseOperator):
    """
    Operador personalizado para extrair dados do Twitter via API.
    Herda de BaseOperator para integração com o Airflow.
    """
    
    # Campos que podem usar templates do Airflow (variáveis dinâmicas)
    template_fields = ["query", "file_path", "start_time", "end_time"]

    def __init__(self, file_path, end_time, start_time, query, conn_id="twitter-default", **kwargs):
        """
        Inicializa o operador com os parâmetros necessários.
        
        Args:
            file_path: Caminho onde salvar os dados extraídos
            end_time: Timestamp de fim do período de busca
            start_time: Timestamp de início do período de busca
            query: Termo de busca para tweets
            conn_id: ID da conexão configurada no Airflow (padrão: "twitter-default")
            **kwargs: Argumentos adicionais do BaseOperator
        """
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        super().__init__(**kwargs)
    
    def create_parent_folder(self):
        """
        Cria a estrutura de diretórios necessária para salvar o arquivo.
        Usa pathlib para criar diretórios aninhados se não existirem.
        """
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        """
        Método principal executado pelo Airflow.
        
        Args:
            context: Contexto da execução fornecida pelo Airflow
        """
        # Cria a estrutura de diretórios
        self.create_parent_folder()
        
        # Abre arquivo para escrita e processa cada página de resultados
        with open(self.file_path, "w") as output_file:
            for page in TwitterHook(self.end_time, self.start_time, self.query, conn_id=self.conn_id).run():
                # Salva cada página como JSON separado por linha
                json.dump(page, output_file, ensure_ascii=False)   
                output_file.write("\n")

# Código de teste para execução independente
if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    # Configuração de teste: busca tweets do dia anterior
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "data science"

    # Execução de teste da DAG
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            file_path=join("datalake/twitter_datascience",
                           f"extract_date={datetime.now().date()}",
                           f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json"),
            query=query, 
            start_time=start_time, 
            end_time=end_time, 
            task_id="test_run"
        )
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)