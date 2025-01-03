"""
Este módulo realiza a extração de dados organizacionais de uma API.

Funcionalidades:
- Extrai dados da tabela de organizações (`R030FIL`) de uma API.
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_organizacao()`: Realiza a extração dos dados organizacionais e retorna um DataFrame com as colunas:
  - `NUMEMP`: Número da empresa.
  - `NOMFIL`: Nome da filial.

Requisitos:
- Biblioteca `lib.api` para interação com a API `APIRhClient`.
- Logging detalhado para monitoramento de tentativas e erros.

Exceções:
- Levanta uma exceção caso todas as tentativas de extração falhem.
"""

import time
import logging
from lib.api import APIRhClient


def extraction_organizacao():
    api_client = APIRhClient()
    table_name = 'R030FIL'
    fields = 'NUMEMP, NOMFIL'
        
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        logging.info(f"extraction_organizacao: Tentativa {retry_count + 1} de extrair dados da tabela {table_name}.")
        try:
            df = api_client.post('integracoes', encryption=0,table_name=table_name, fields=fields)
            logging.info("extraction_organizacao: Dados extraídos com sucesso.")
            return df
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_organizacao: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_organizacao: Re-tentando...")

    logging.critical(f"extraction_organizacao: Todas as {max_retries} tentativas falharam.")
    raise Exception(f"Falhou após {max_retries} tentativas")