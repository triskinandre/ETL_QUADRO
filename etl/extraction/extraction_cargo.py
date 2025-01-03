"""
Este módulo realiza a extração de dados relacionados a cargos de uma API.

Funcionalidades:
- Extrai dados da tabela de cargos da API.
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_cargo()`: Realiza a extração dos dados de cargos e retorna um DataFrame com:
  - Código do cargo (`CodCar`).
  - Título do cargo (`TitCar`).

Requisitos:
- Biblioteca `lib.api` para interação com a API `APIRhClient`.
- Logging detalhado para monitoramento de tentativas e erros.
"""

import time
import logging
from lib.api import APIRhClient

def extraction_cargo():
    api_client = APIRhClient()
    table_name = 'R024CAR'
    fields = 'CodCar, TitCar'
    
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        logging.info(f"extraction_cargo: Tentativa {retry_count + 1} de extrair dados da tabela {table_name}.")
        try:
            df = api_client.post('integracoes', encryption=0, table_name=table_name, fields=fields)
            logging.info("extraction_cargo: Dados extraídos com sucesso.")
            return df
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_afastamento: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_afastamento: Re-tentando...")

    logging.critical(f"extraction_afastamento: Todas as {max_retries} tentativas falharam.")            
    raise Exception(f"Falhou após {max_retries} tentativas")