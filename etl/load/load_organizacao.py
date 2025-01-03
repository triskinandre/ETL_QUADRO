"""
Este módulo gerencia o carregamento de dados de organização no banco de dados.

Funcionalidades:
- Lê as variáveis de ambiente para obter a URL de conexão com o banco de dados.
- Usa SQLAlchemy para se conectar ao banco e realizar inserções ou atualizações (upsert) na tabela de organização.
- Insere novos registros ou atualiza registros existentes com base no campo `id_rh_organizacao`.
- Garante que, ao realizar o upsert, o campo `nome` seja atualizado conforme necessário.
- Implementa transações para garantir a consistência dos dados durante o processo.
- Registra logs detalhados de cada etapa do processo, incluindo erros.

Função principal:
- `load_organizacao(df)`: Insere ou atualiza registros na tabela de organização no banco de dados a partir de um DataFrame.
"""

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')

def load_organizacao(df):
    try:
        logging.info("load_organizacao: Iniciando organizacao.")
        engine = create_engine(DATABASE_URL_SISTEMA)

        with engine.connect() as connection:
            trans = connection.begin()
            try:
                upsert_query = text("""
                    INSERT INTO homologacao.rh_organizacao  (id_rh_organizacao, nome)
                    VALUES (:id_rh_organizacao, :nome)
                    ON CONFLICT (id_rh_organizacao) DO UPDATE 
                    SET nome = EXCLUDED.nome
                """)

                records = df.to_dict(orient='records')
                connection.execute(upsert_query, records)

                trans.commit()
                logging.info("load_organizacao: Fim organizacao.")

            except Exception as e:
                trans.rollback()
                logging.error(f"load_organizacao: Erro durante a transação: {e}")
        
    
    except Exception as error:
        logging.error(f"load_organizacao: Erro ao carregar os dados: {error}")