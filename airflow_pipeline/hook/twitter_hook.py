from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import requests
import json

class TwitterHook(HttpHook):
    """
    Hook personalizado para conectar com a API do Twitter.
    Herda de HttpHook para gerenciar conexões HTTP.
    """

    def __init__(self, end_time, start_time, query, conn_id=None):
        """
        Inicializa o hook com parâmetros de busca.
        
        Args:
            end_time: Timestamp de fim do período de busca
            start_time: Timestamp de início do período de busca
            query: Termo de busca para tweets
            conn_id: ID da conexão configurada no Airflow (opcional)
        """
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.conn_id = conn_id or "twitter_default"  # Usa conexão padrão se não especificada
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        """
        Cria a URL da API do Twitter com todos os parâmetros necessários.
        
        Returns:
            str: URL completa para a requisição à API
        """
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

        # Campos específicos dos tweets que queremos extrair
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        
        # Campos específicos dos usuários que queremos extrair
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        # URL comentada para referência
        # url_raw = f"https://api.twitter.com/2/tweets/search/recent?query={query}&start_time={start_time}&end_time={end_time}&{tweet_fields}&{user_fields}"

        # URL final usando base_url da conexão configurada
        url_raw = f"{self.base_url}/2/tweets/search/recent?query={self.query}&{tweet_fields}&{user_fields}&start_time={self.start_time}&end_time={self.end_time}"

        return url_raw

    def connect_to_endpoint(self, url, session):
        """
        Conecta ao endpoint da API e executa a requisição.
        
        Args:
            url: URL completa para a requisição
            session: Sessão HTTP configurada
            
        Returns:
            Response: Resposta da API
        """
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")  

        return self.run_and_check(session, prep, {})
    
    def paginate(self, url_raw, session):
        """
        Implementa paginação para buscar todos os resultados disponíveis.
        A API do Twitter retorna no máximo 100 tweets por requisição.
        
        Args:
            url_raw: URL base para as requisições
            session: Sessão HTTP configurada
            
        Returns:
            list: Lista com todas as páginas de resultados
        """
        lista_json_response = []
        response = self.connect_to_endpoint(url_raw, session)
        json_response = response.json()
        lista_json_response.append(json_response)   
        contador = 1

        # Continua buscando enquanto houver next_token e não exceder 100 páginas
        while "next_token" in json_response.get("meta", {}) and contador < 100:
            next_token = json_response["meta"]["next_token"]
            url = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_endpoint(url, session)
            json_response = response.json()
            lista_json_response.append(json_response) 
            contador += 1
        
        return lista_json_response

    def run(self):
        """
        Método principal que executa toda a extração.
        
        Returns:
            list: Lista com todas as páginas de resultados da API
        """
        session = self.get_conn()  # Obtém sessão HTTP configurada
        url_raw = self.create_url()  # Cria URL da requisição

        return self.paginate(url_raw, session)  # Executa paginação completa

# Código de teste para execução independente
if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    # Configuração de teste: busca tweets do dia anterior
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "data science"

    # Executa teste e imprime resultados formatados
    for page in TwitterHook(end_time, start_time, query).run():
        print(json.dumps(page, indent=4, sort_keys=True))    