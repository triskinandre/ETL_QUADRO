"""
Este módulo realiza a extração de dados da tabela de sites do banco de dados MIS.

Funcionalidades:
- Conecta ao banco de dados utilizando uma string de conexão ODBC.
- Extrai todos os registros da tabela `QUADRO_SITE_EMP`.
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_quadro_site()`: Realiza a extração dos dados da tabela `QUADRO_SITE_EMP` e retorna um DataFrame contendo todas as colunas e registros da tabela.

Requisitos:
- Configuração do ambiente com a variável `DATABASE_URL_WOCC34` contendo a string de conexão ODBC.
- Biblioteca `pyodbc` para conexão ao banco de dados SQL Server.
- Logging detalhado para monitoramento de tentativas e erros.

Exceções:
- Levanta uma exceção caso todas as tentativas de extração falhem.

Parâmetros:
- Não possui parâmetros de entrada.

Retorno:
- Retorna um DataFrame (`pandas.DataFrame`) com os dados extraídos da tabela `QUADRO_SITE_EMP`.
"""


import os 
import time
import logging
import pandas as pd
from dotenv import load_dotenv
import pyodbc


load_dotenv()
DATABASE_URL_WOCC34 = os.getenv('DATABASE_URL_WOCC34')


def extraction_quadro_site():
    conn = pyodbc.connect(DATABASE_URL_WOCC34)
    cursor = conn.cursor()
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        logging.info(f"extraction_quadro_site: Tentativa {retry_count + 1} de extrair dados da tabela MIS.")
        try:
            table_name = 'QUADRO_SITE_EMP'
            cursor.execute(f"SELECT * FROM {table_name}")
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame.from_records(rows, columns=columns)
            cursor.close()
            conn.close()
            logging.info("extraction_quadro_site: Dados extraídos com sucesso.")
            return df
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_quadro_site: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_quadro_site: Re-tentando...")

    logging.critical(f"extraction_quadro_site: Todas as {max_retries} tentativas falharam.")
    raise Exception(f"Falhou após {max_retries} tentativas")
            