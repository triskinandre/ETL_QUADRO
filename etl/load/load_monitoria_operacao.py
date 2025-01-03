"""
Este módulo gerencia o carregamento de dados de monitoria de operação no banco de dados.

Funcionalidades:
- Lê as variáveis de ambiente para obter a URL de conexão com o banco de dados.
- Usa SQLAlchemy para se conectar ao banco e realizar inserções ou atualizações (upsert) na tabela de monitoria de operação.
- Insere novos registros ou atualiza registros existentes com base no campo `id_monitoria_operacao`.
- Garante que, ao realizar o upsert, campos como `nome`, `descricao` e `matricula_cadastro` sejam atualizados conforme necessário.
- Implementa transações para garantir a consistência dos dados durante o processo.
- Registra logs detalhados de cada etapa do processo, incluindo erros.

Função principal:
- `load_monitoria_operacao(df)`: Insere ou atualiza registros na tabela de monitoria de operação no banco de dados a partir de um DataFrame.
"""

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')

def load_monitoria_operacao(df):
    try:
        logging.info("load_monitoria_operacao: Iniciando monitoria_operacao.")
        engine = create_engine(DATABASE_URL_SISTEMA)

        with engine.connect() as connection:
            trans = connection.begin()
            try:
                upsert_query = text("""                       
                    INSERT INTO homologacao.monitoria_operacao  (id_monitoria_operacao, nome, descricao, matricula_cadastro)
                    VALUES (:id_rh_centro_custo, :nome, 'Padrão do sistema', 5804)
                    ON CONFLICT (id_monitoria_operacao) DO UPDATE 
                    SET nome = EXCLUDED.nome,
                        descricao = EXCLUDED.descricao,
                        matricula_cadastro = EXCLUDED.matricula_cadastro
                """)

                records = df.to_dict(orient='records')
                connection.execute(upsert_query, records)

                trans.commit()
                logging.info("load_monitoria_operacao: Fim monitoria_operacao.")

            except Exception as e:
                trans.rollback()
                logging.error(f"load_monitoria_operacao: Erro durante a transação: {e}")
        
    
    except Exception as error:
        logging.error(f"load_monitoria_operacao: Erro ao carregar os dados: {error}")
