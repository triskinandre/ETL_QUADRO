"""
Este módulo realiza a extração de dados da tabela de supervisores do banco de dados.

Funcionalidades:
- Conecta ao banco de dados utilizando a string de conexão configurada no ambiente.
- Extrai todos os registros da tabela `QUADRO_SUPERVISOR`.
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_supervisor_quadro()`: Realiza a extração dos dados da tabela `QUADRO_SUPERVISOR` e retorna um DataFrame contendo todas as colunas e registros da tabela.

Requisitos:
- Configuração do ambiente com a variável `DATABASE_URL_WOCC34` contendo a string de conexão do banco de dados.
- Biblioteca `sqlalchemy` para conexão ao banco de dados.
- Logging detalhado para monitoramento de tentativas e erros.

Exceções:
- Levanta uma exceção caso todas as tentativas de extração falhem.

Parâmetros:
- Não possui parâmetros de entrada.

Retorno:
- Retorna um DataFrame (`pandas.DataFrame`) com os dados extraídos da tabela `QUADRO_SUPERVISOR`.
"""

import os 
import time
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()
DATABASE_URL_WOCC34 = os.getenv('DATABASE_URL_WOCC34')

def extraction_supervisor_quadro():
    engine = create_engine(DATABASE_URL_WOCC34)
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        logging.info(f"extraction_supervisor_quadro: Tentativa {retry_count + 1} de extrair dados da tabela {table_name}.")
        try:
            table_name = 'QUADRO_SUPERVISOR'
            with engine.connect() as connection:
                query = text(f"SELECT * FROM {table_name}")
                df = pd.read_sql(query, connection)
                logging.info("extraction_supervisor_quadro: Dados extraídos com sucesso.")
                return df
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_supervisor_quadro: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_supervisor_quadro: Re-tentando...")

    logging.critical(f"extraction_supervisor_quadro: Todas as {max_retries} tentativas falharam.")
    raise Exception(f"Falhou após {max_retries} tentativas")
            