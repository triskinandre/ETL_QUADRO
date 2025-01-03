"""
Este módulo carrega dados de afastamento para o banco de dados.

Funcionalidades:
- Lê as variáveis de ambiente para obter a URL de conexão com o banco de dados.
- Usa SQLAlchemy para se conectar ao banco e realizar inserções na tabela de afastamento.
- Implementa transações para garantir a consistência dos dados.
- Registra logs detalhados de cada etapa do processo, incluindo erros.

Função principal:
- `load_afastamento(df)`: Insere registros de afastamento no banco de dados a partir de um DataFrame.
"""

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')

def load_afastamento(df):
    try:
        logging.info("load_afastamento: Iniciando afastamento.")
        engine = create_engine(DATABASE_URL_SISTEMA)

        with engine.connect() as connection:
            trans = connection.begin()
            try:
                upsert_query = text("""
                    INSERT INTO homologacao.rh_afastamento  (matricula, data_entrada, data_saida, id_rh_situacao_afastamento)
                    VALUES (:matricula, :data_entrada, :data_saida, :id_rh_situacao_afastamento)
                """)

                records = df.to_dict(orient='records')
                connection.execute(upsert_query, records)

                trans.commit()
                logging.info("load_afastamento: Fim afastamento.")

            except Exception as e:
                trans.rollback()
                logging.error(f"load_afastamento: Erro durante a transação: {e}")
    
    except Exception as error:
        logging.error(f"load_afastamento: Erro ao carregar os dados: {error}")
