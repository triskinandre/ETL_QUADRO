"""
Este módulo realiza a extração de dados relacionados ao status de situações de uma API.

Funcionalidades:
- Extrai dados da tabela `R010SIT` da API, contendo informações sobre os códigos e descrições de status.
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_status()`: Realiza a extração dos dados de status e retorna um DataFrame com as colunas:
  - `CODSIT`: Código do status.
  - `DESSIT`: Descrição do status.

Requisitos:
- Biblioteca `lib.api` para interação com a API `APIRhClient`.
- Logging detalhado para monitoramento de tentativas e erros.

Exceções:
- Levanta uma exceção caso todas as tentativas de extração falhem.

Parâmetros:
- Não possui parâmetros de entrada.

Retorno:
- Retorna um DataFrame (`pandas.DataFrame`) com os dados extraídos da tabela `R010SIT`.
"""


import time
import logging
from lib.api import APIRhClient


def extraction_status():
    api_client = APIRhClient()
    table_name = 'R010SIT'
    fields = 'CODSIT, DESSIT'
        
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        logging.info(f"extraction_status: Tentativa {retry_count + 1} de extrair dados da tabela {table_name}.")
        try:
            df = api_client.post('integracoes', encryption=0, table_name=table_name, fields=fields)
            logging.info(f"extraction_status: Tentativa {retry_count + 1} de extrair dados da tabela {table_name}.")
            return df
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_status: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_status: Re-tentando...")

    logging.critical(f"extraction_status: Todas as {max_retries} tentativas falharam.")
    raise Exception(f"Falhou após {max_retries} tentativas")