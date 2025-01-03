"""
Este módulo realiza a extração de dados relacionados a centros de custo de uma API.

Funcionalidades:
- Extrai dados da tabela de centros de custo da API.
- Filtra os registros de centros de custo pertencentes às empresas '1' e '2'.
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_centro_custo()`: Realiza a extração dos dados de centros de custo e retorna um DataFrame com:
  - Código do centro de custo (`CODCCU`).
  - Nome do centro de custo (`NOMCCU`).

Requisitos:
- Biblioteca `lib.api` para interação com a API `APIRhClient`.
- Logging detalhado para monitoramento de tentativas e erros.

Exceções:
- Levanta uma exceção caso todas as tentativas de extração falhem.
"""

import time
import logging
from lib.api import APIRhClient


def extraction_centro_custo():
    api_client = APIRhClient()
    table_name = 'R018CCU'
    fields = 'CODCCU, NOMCCU'
        
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        logging.info(f"extraction_centro_custo: Tentativa {retry_count + 1} de extrair dados da tabela {table_name}.")
        try:
            df = api_client.post('integracoes', encryption=0, field_condition='NUMEMP',type_condition='IN', value_condition='1,2', table_name=table_name, fields=fields)
            logging.info("extraction_centro_custo: Dados extraídos com sucesso.")
            return df
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_centro_custo: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_centro_custo: Re-tentando...") 

    logging.critical(f"extraction_centro_custo: Todas as {max_retries} tentativas falharam.")
    raise Exception(f"Falhou após {max_retries} tentativas")