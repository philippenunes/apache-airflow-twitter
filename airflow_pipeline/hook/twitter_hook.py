from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import requests
import json

class TwitterHook(HttpHook):
    """
    Hook personalizado para conectar com a API do Twitter.
    Herda de HttpHook para gerenciar conexões HTTP.
    """

    def __init__(self, end_time, start_time, query, conn_id):
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
        self.conn_id = conn_id

        super().__init__(http_conn_id=self.conn_id)
    
    def get_timestamp_format(self):
        """
        Determina o formato de timestamp baseado na conexão.
        
        Returns:
            str: Formato de timestamp apropriado
        """
        if self.conn_id == "twitter-api":
            # API oficial do Twitter: sem milissegundos
            timestamp_format = "%Y-%m-%dT%H:%M:%SZ"
            print(f"DEBUG - Usando formato Twitter API: {timestamp_format}")
        else:
            # API do Lab Dados: com milissegundos
            timestamp_format = "%Y-%m-%dT%H:%M:%S.00Z"
            print(f"DEBUG - Usando formato Lab Dados: {timestamp_format}")
        
        return timestamp_format

    def format_timestamp(self, timestamp_str):
        """
        Converte timestamp string para o formato correto da API.
        
        Args:
            timestamp_str: Timestamp como string (ex: "2025-08-13 00:00:00+00:00")
            
        Returns:
            str: Timestamp no formato correto da API
        """
        try:
            # Converter string para datetime
            if isinstance(timestamp_str, str):
                # Remover timezone se existir e converter para datetime
                dt = datetime.fromisoformat(timestamp_str.replace('+00:00', ''))
            else:
                dt = timestamp_str
            
            # Formatar usando o formato da API
            formatted = dt.strftime(self.get_timestamp_format())
            print(f"DEBUG - Timestamp convertido: {timestamp_str} -> {formatted}")
            return formatted
            
        except Exception as e:
            print(f"DEBUG - Erro ao formatar timestamp {timestamp_str}: {e}")
            # Se der erro, retornar como está
            return timestamp_str

    def create_url(self):
        """
        Cria a URL da API do Twitter com todos os parâmetros necessários.
        
        Returns:
            str: URL completa para a requisição à API
        """
            # Campos específicos dos tweets que queremos extrair
        tweet_fields = (
            "tweet.fields=author_id,conversation_id,created_at,id,"
            "in_reply_to_user_id,public_metrics,lang,text"
        )
        
        
        # Campos específicos dos usuários que queremos extrair
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        # Formatar timestamps
        start_time_formatted = self.format_timestamp(self.start_time)
        end_time_formatted = self.format_timestamp(self.end_time)

       # URL final usando base_url da conexão configurada
        url_raw = (
            f"{self.base_url}/2/tweets/search/recent?"
            f"query={self.query}&"
            f"{tweet_fields}&"
            f"{user_fields}&"
            f"start_time={start_time_formatted}&"
            f"end_time={end_time_formatted}"
        )

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
        while "next_token" in json_response.get("meta", {}) and contador < 1:
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
    TIMESTAMP_FORMAT_TWITTER = "%Y-%m-%dT%H:%M:%SZ"
    TIMESTAMP_FORMAT_LAB_DADOS = "%Y-%m-%dT%H:%M:%S.00Z"

    # Configuração de teste: busca tweets do dia anterior
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT_LAB_DADOS)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT_LAB_DADOS)
    query = "data science"

    # Executa teste e imprime resultados formatados
    for page in TwitterHook(end_time, start_time, query, conn_id="twitter-default").run():
        print(json.dumps(page, indent=4, sort_keys=True))    