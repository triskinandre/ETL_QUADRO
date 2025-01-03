"""
Este módulo realiza a extração de dados sobre o quadro de centro de custo e equipe do banco de dados MIS.

Funcionalidades:
- Conecta ao banco de dados utilizando uma string de conexão ODBC.
- Extrai dados da tabela `QUADRO_FUNCIONARIOS`, com informações relacionadas a centros de custo, estratégia e equipe.
- Filtra os registros para incluir apenas colaboradores com o status ativo (`ID_STATUS = 1`) e pertencentes a centros de custo específicos (`ID` IN (17, 13)).
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_quadro_centro_custo_equipe()`: Realiza a extração dos dados e retorna um DataFrame com as colunas:
  - `id_rh_centro_custo`: Identificação única do centro de custo no formato concatenado.
  - `id_monitoria_operacao_equipe`: Identificação única da equipe de monitoria concatenada.
  - `matricula`: Matrícula do colaborador.

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

def extraction_quadro_centro_custo_equipe():
    conn = pyodbc.connect(DATABASE_URL_WOCC34)
    cursor = conn.cursor()
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        logging.info(f"extraction_quadro_centro_custo_equipe: Tentativa {retry_count + 1} de extrair dados da tabela MIS.")
        try:
            cursor.execute("""
                SELECT  distinct
					REPLACE(CONCAT(QUADRO_CENTRO_CUSTO.CODIGO, CASE WHEN QUADRO_ESTRATEGIA.ID	= 201 THEN 112 ELSE 	QUADRO_ESTRATEGIA.ID END),'.','') AS id_rh_centro_custo
					,REPLACE(CONCAT(QUADRO_CENTRO_CUSTO.CODIGO, QUADRO_FUNCIONARIOS.MATRICULA), '.', '')                                                  AS id_monitoria_operacao_equipe
					,QUADRO_FUNCIONARIOS.MATRICULA as matricula
                FROM QUADRO_OPERACIONAL.dbo.QUADRO_FUNCIONARIOS 
                INNER JOIN QUADRO_OPERACIONAL.dbo.QUADRO_CENTRO_CUSTO
                    ON QUADRO_CENTRO_CUSTO.ID  = QUADRO_FUNCIONARIOS.ID_CT_CUSTO 
				INNER JOIN QUADRO_OPERACIONAL.dbo.QUADRO_ESTRATEGIA
					ON QUADRO_ESTRATEGIA.ID = QUADRO_FUNCIONARIOS.ID_ESTRATEGIA
				INNER JOIN QUADRO_OPERACIONAL.DBO.QUADRO_SUB_GRUPO_ESTRATEGIA
					ON QUADRO_SUB_GRUPO_ESTRATEGIA.ID = QUADRO_FUNCIONARIOS.SUB_GRUPO_CARTEIRA
                WHERE QUADRO_FUNCIONARIOS.ID_STATUS  = 1
				AND  QUADRO_CENTRO_CUSTO.ID IN (17, 13)
            """)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame.from_records(rows, columns=columns)
            cursor.close()
            conn.close()
            logging.info("extraction_quadro_centro_custo_equipe: Dados extraídos com sucesso.")
            return df
        except Exception as e:
            logging.error(f"extraction_quadro_centro_custo_equipe: Erro na tentativa {retry_count}: {e}")
            retry_count += 1
            print(f"Tentativa {retry_count} falhou: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_quadro_centro_custo_equipe: Re-tentando...")

    logging.critical(f"extraction_quadro_centro_custo_equipe: Todas as {max_retries} tentativas falharam.")
    raise Exception(f"Falhou após {max_retries} tentativas")
            