"""
Este módulo carrega dados de monitoria de operação e equipe para o banco de dados.

Funcionalidades:
- Lê as variáveis de ambiente para obter a URL de conexão com o banco de dados.
- Usa SQLAlchemy para se conectar ao banco e realizar inserções ou ignorar registros já existentes (upsert com `DO NOTHING`) na tabela de monitoria de operação e equipe.
- Insere novos registros na tabela sem substituir registros existentes com base no campo `id_monitoria_operacao_equipe`.
- Implementa transações para garantir a consistência dos dados durante o processo.
- Registra logs detalhados de cada etapa do processo, incluindo erros.

Função principal:
- `load_monitoria_operacao_equipe_carrefour(df)`: Insere registros na tabela de monitoria de operação e equipe no banco de dados a partir de um DataFrame.
"""

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')

def load_monitoria_operacao_equipe_carrefour(df):
    try:
        logging.info("load_monitoria_operadora_equipe_carrefour: Iniciando operadora_equipe_carrefour.")
        engine = create_engine(DATABASE_URL_SISTEMA)

        with engine.connect() as connection:
            trans = connection.begin()
            try:
                upsert_query = text("""
                    INSERT INTO homologacao.monitoria_operacao_equipe  (id_monitoria_operacao_equipe, id_monitoria_operacao, matricula)
                    VALUES (:id_monitoria_operacao_equipe, :id_rh_centro_custo, :matricula)
                    ON CONFLICT (id_monitoria_operacao_equipe) DO NOTHING
                """)

                records = df.to_dict(orient='records')
                connection.execute(upsert_query, records)

                trans.commit()
                logging.info("load_monitoria_operadora_equipe_carrefour: Fim operadora_equipe_carrefour.")
            except Exception as e:
                trans.rollback()
                logging.error(f"load_afastamento: Erro durante a transação: {e}")
    
    except Exception as error:
        logging.error(f"load_afastamento: Erro ao carregar os dados: {error}")
