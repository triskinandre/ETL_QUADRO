"""
Este módulo realiza a extração de dados de movimento de colaboradores e informações relacionadas de uma API e do banco de dados.

Funcionalidades:
- Extrai dados da tabela de colaboradores ativos de uma API.
- Filtra os registros de colaboradores com base em condições específicas:
  - Apenas colaboradores da empresa número '1'.
  - Exclui colaboradores com situação de afastamento '7'.
- Conecta ao banco de dados e carrega tabelas auxiliares de cargos e centros de custo.
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_colaborador_movimento()`: Realiza a extração dos dados de colaboradores e retorna DataFrames com:
  - Dados de colaboradores extraídos da API.
  - Tabela de cargos (`rh_cargo`) do banco de dados.
  - Tabela de centros de custo (`rh_centro_custo`) do banco de dados.

Requisitos:
- Configuração do ambiente para obter a URL do banco de dados (`DATABASE_URL_SISTEMA`).
- Biblioteca `lib.api` para interação com a API `APIRhClient`.
- Logging detalhado para monitoramento de tentativas e erros.
"""

import os
import time
import logging
from lib.api import APIRhClient
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')

def extraction_calaborador_movimento(): 
    api_client = APIRhClient()
    engine = create_engine(DATABASE_URL_SISTEMA)
    table_name = 'R034FUN'
    fields = 'NUMCAD, NOMFUN, NUMEMP, CODCCU, CODCAR, SITAFA'
    
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            logging.info(f"extraction_colaborador_movimento: Tentativa {retry_count + 1} de extrair dados da tabela {table_name}.")
            df = api_client.post('integracoes', encryption=0, field_condition='TIPCOL',type_condition='=', value_condition='1', table_name=table_name, fields=fields)
            df =  df[df['NUMEMP'] == '1']
            df =  df[df['SITAFA'] != '7']
            df_rh_cargo = pd.read_sql_table('rh_cargo', engine, schema='homologacao')
            df_rh_centro_custo = pd.read_sql_table('rh_centro_custo', engine, schema='homologacao')
            logging.info("extraction_colaborador_movimento: Dados extraídos com sucesso.")
            return df, df_rh_cargo, df_rh_centro_custo
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_colaborador_movimento: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_colaborador_movimento: Re-tentando...")

    logging.critical(f"extraction_colaborador_movimento: Todas as {max_retries} tentativas falharam.")
    raise Exception(f"Falhou após {max_retries} tentativas")