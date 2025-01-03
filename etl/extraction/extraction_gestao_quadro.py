"""
Este módulo realiza a extração de dados de gestão de quadro operacional do banco de dados MIS.

Funcionalidades:
- Conecta ao banco de dados utilizando uma string de conexão ODBC.
- Extrai dados da tabela `QUADRO_FUNCIONARIOS`, com informações relacionadas a gestores, supervisores, gerentes, centros de custo e sites.
- Filtra os registros para incluir apenas colaboradores com o status ativo (`ID_STATUS = 1`).
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_gestao_quadro()`: Realiza a extração dos dados de gestão de quadro operacional e retorna um DataFrame com as colunas:
  - `MATRICULA`: Matrícula do colaborador.
  - `ID_GESTOR`: Identificação do gestor.
  - `ID_SUPERVISOR`: Identificação do supervisor.
  - `ID_GERENTE`: Identificação do gerente.
  - `CODIGO_CT_CUSTO`: Código do centro de custo.
  - `ID_SITE`: Identificação do site.

Requisitos:
- Configuração do ambiente com a string de conexão `DATABASE_URL_WOCC34`.
- Biblioteca `pyodbc` para conexão ao banco de dados SQL Server.
- Logging detalhado para monitoramento de tentativas e erros.

Exceções:
- Levanta uma exceção caso todas as tentativas de extração falhem.
"""

import os 
import time
import logging
import pandas as pd
from dotenv import load_dotenv
import pyodbc
from sqlalchemy import create_engine, text


load_dotenv()
DATABASE_URL_WOCC34 = os.getenv("DATABASE_URL_WOCC34")

def extraction_gestao_quadro():
    conn = pyodbc.connect(DATABASE_URL_WOCC34)
    cursor = conn.cursor()
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        logging.info(f"extraction_gestao_quadro: Tentativa {retry_count + 1} de extrair dados da tabela do MIS.")
        try:
            cursor.execute("""
                SELECT  
                    QUADRO_FUNCIONARIOS.MATRICULA, 
                    QUADRO_FUNCIONARIOS.ID_GESTOR, 
                    QUADRO_FUNCIONARIOS.ID_SUPERVISOR, 
                    QUADRO_FUNCIONARIOS.ID_GERENTE,
                    QUADRO_CENTRO_CUSTO.CODIGO AS CODIGO_CT_CUSTO,
                    QUADRO_FUNCIONARIOS.ID_SITE
                FROM QUADRO_OPERACIONAL.dbo.QUADRO_FUNCIONARIOS 
                INNER JOIN QUADRO_OPERACIONAL.dbo.QUADRO_CENTRO_CUSTO
                    ON QUADRO_CENTRO_CUSTO.ID  = QUADRO_FUNCIONARIOS.ID_CT_CUSTO 
                WHERE QUADRO_FUNCIONARIOS.ID_STATUS  = 1
            """)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame.from_records(rows, columns=columns)
            cursor.close()
            conn.close()
            logging.info("extraction_gestao_quadro: Dados extraídos com sucesso.")
            return df
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_gestao_quadro: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_gestao_quadro: Re-tentando...")

    logging.critical(f"extraction_gestao_quadro: Todas as {max_retries} tentativas falharam.")            
    raise Exception(f"Falhou após {max_retries} tentativas")
            

