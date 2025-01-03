"""
Este módulo carrega dados de centro de custo para o banco de dados.

Funcionalidades:
- Lê as variáveis de ambiente para obter a URL de conexão com o banco de dados.
- Usa SQLAlchemy para se conectar ao banco e realizar inserções ou atualizações (upsert) na tabela de centro de custo.
- Implementa transações para garantir a consistência dos dados durante o processo.
- Registra logs detalhados de cada etapa do processo, incluindo erros.

Função principal:
- `load_centro_custo(df)`: Insere ou atualiza registros na tabela de centro de custo no banco de dados a partir de um DataFrame.
"""

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')

def load_centro_custo(df):
    try:
        logging.info("load_centro_custo: Iniciando centro custo.")
        engine = create_engine(DATABASE_URL_SISTEMA)

        with engine.connect() as connection:
            trans = connection.begin()
            try:
                upsert_query = text("""
                    INSERT INTO homologacao.rh_centro_custo  (id_rh_centro_custo, nome)
                    VALUES (:id_rh_centro_custo, :nome)
                    ON CONFLICT (id_rh_centro_custo) DO UPDATE 
                    SET nome = EXCLUDED.nome
                """)

                records = df.to_dict(orient='records')
                connection.execute(upsert_query, records)

                trans.commit()
                logging.info("load_centro_custo: Fim centro custo.")

            except Exception as e:
                trans.rollback()
                logging.error(f"load_centro_custo: Erro durante a transação: {e}")
        
    
    except Exception as error:
        logging.error(f"load_centro_custo: Erro ao carregar os dados: {error}")