"""
Este módulo gerencia o carregamento de dados de monitoria de operação e equipe no banco de dados.

Funcionalidades:
- Lê as variáveis de ambiente para obter a URL de conexão com o banco de dados.
- Usa SQLAlchemy para se conectar ao banco e realizar exclusões e inserções condicionais (upsert com `DO NOTHING`) na tabela de monitoria de operação e equipe.
- Exclui registros específicos da tabela com base em critérios definidos.
- Insere novos registros na tabela, ignorando registros que já existem com base no campo `id_monitoria_operacao_equipe`.
- Implementa transações para garantir a consistência dos dados durante o processo.
- Registra logs detalhados de cada etapa do processo, incluindo erros.

Função principal:
- `load_monitoria_operacao_equipe(df)`: Realiza exclusões e inserções na tabela de monitoria de operação e equipe no banco de dados a partir de um DataFrame.
"""

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')

def load_monitoria_operacao_equipe(df):
    try:
        logging.info("load_operacao_equipe: Iniciando operacao_equipe.")
        engine = create_engine(DATABASE_URL_SISTEMA)
        with engine.connect() as connection:
            trans = connection.begin()
            try:
                # Primeiro, excluir os registros que correspondem ao critério especificado
                delete_query = text("""
                    DELETE FROM homologacao.monitoria_operacao_equipe
                    WHERE id_monitoria_operacao IN (
                        SELECT id_monitoria_operacao
                        FROM homologacao.monitoria_operacao
                        WHERE matricula_cadastro = 5804
                    )
                """)
                connection.execute(delete_query)

                # Em seguida, inserir os novos registros (upsert)
                upsert_query = text("""
                    INSERT INTO homologacao.monitoria_operacao_equipe (id_monitoria_operacao_equipe, id_monitoria_operacao, matricula)
                    VALUES (:matricula, :id_rh_centro_custo, :matricula)
                    ON CONFLICT (id_monitoria_operacao_equipe) DO NOTHING
                """)

                # Inserir cada registro individualmente
                records = df.to_dict(orient='records')
                for record in records:
                    connection.execute(upsert_query, record)

                # Confirmar a transação
                trans.commit()
                logging.info("load_operacao_equipe: Fim operacao_equipe.")
                
            except Exception as e:
                trans.rollback()
                logging.error(f"load_operacao_equipe: Erro durante a transação: {e}")
        
    except Exception as error:
        logging.error(f"load_operacao_equipe: Erro ao carregar os dados: {error}")
