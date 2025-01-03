"""
Este módulo carrega dados de colaboradores para o banco de dados.

Funcionalidades:
- Lê as variáveis de ambiente para obter a URL de conexão com o banco de dados.
- Usa SQLAlchemy para se conectar ao banco e realizar inserções ou atualizações (upsert) na tabela de colaboradores.
- Insere novos registros ou atualiza registros existentes com base no campo `matricula`.
- Implementa transações para garantir a consistência dos dados durante o processo.
- Registra logs detalhados de cada etapa do processo, incluindo erros.

Função principal:
- `load_colaborador(df)`: Insere ou atualiza registros na tabela de colaboradores no banco de dados a partir de um DataFrame.
"""

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')

def load_colaborador(df):
    try:
        logging.info("load_colaborador: Iniciando colaborador.")
        engine = create_engine(DATABASE_URL_SISTEMA)

        with engine.connect() as connection:
            trans = connection.begin()
            try:
                upsert_query = text("""
                    INSERT INTO homologacao.rh_colaborador (matricula, nome_completo, cpf, data_admissao, data_nascimento)
                    VALUES (:matricula, :nome_completo, :cpf, :data_admissao, :data_nascimento)
                    ON CONFLICT (matricula) DO UPDATE 
                    SET nome_completo = EXCLUDED.nome_completo,
                        cpf = EXCLUDED.cpf,
                        data_admissao = EXCLUDED.data_admissao,
                        data_nascimento = EXCLUDED.data_nascimento;
                """)

                records = df.to_dict(orient='records')
                connection.execute(upsert_query, records)

                trans.commit()
                logging.info("load_colaborador: Fim colaborador.")

            except Exception as e:
                trans.rollback()
                logging.error(f"load_colaborador: Erro durante a transação: {e}")
    
    except Exception as error:
        logging.error(f"load_colaborador: Erro ao carregar os dados: {error}")