"""
Este módulo carrega dados de movimento de colaboradores para o banco de dados.

Funcionalidades:
- Lê as variáveis de ambiente para obter a URL de conexão com o banco de dados.
- Usa SQLAlchemy para se conectar ao banco e realizar operações de atualização e inserção na tabela de movimento de colaboradores.
- Marca registros existentes como "não atuais" e insere novos registros como "atuais".
- Implementa transações para garantir a consistência dos dados durante o processo.
- Registra logs detalhados de cada etapa do processo, incluindo erros.

Função principal:
- `load_colaborador_movimento(df)`: Atualiza e insere registros de movimento de colaboradores no banco de dados a partir de um DataFrame.
"""

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

load_dotenv()
DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')

def load_colaborador_movimento(df):
    logging.info("load_colaborador_movimento: Iniciando colaborador movimento.")
    engine = create_engine(DATABASE_URL_SISTEMA)

    df[['id_rh_supervisor', 'id_rh_coordenador', 'id_rh_gerente', 'id_rh_site']] = df[['id_rh_supervisor', 'id_rh_coordenador', 'id_rh_gerente', 'id_rh_site']].fillna(value=0)

    with engine.connect() as connection:
        trans = connection.begin()
        try:
            for _, row in df.iterrows():
                # Verifica se o id_rh_modificacao já existe
                query_check = text("SELECT id_rh_modificacao FROM homologacao.rh_colaborador_movimento WHERE id_rh_modificacao = :id_rh_modificacao")
                result = connection.execute(query_check, {'id_rh_modificacao': row['id_rh_modificacao']}).fetchone()
                if result is None:
                    # Atualiza a linha existente para 'atual = FALSE' usando a matricula
                    update_query = text("""
                        UPDATE homologacao.rh_colaborador_movimento
                        SET atual = FALSE
                        WHERE matricula = :matricula AND atual = TRUE
                    """)
                    connection.execute(update_query, {'matricula': row['matricula']})

                    # Insere o novo registro
                    insert_query = text("""
                        INSERT INTO homologacao.rh_colaborador_movimento 
                        (
                            matricula, 
                            id_rh_organizacao,  
                            id_rh_centro_custo, 
                            id_rh_cargo, 
                            id_rh_supervisor, 
                            id_rh_coordenador, 
                            id_rh_gerente, 
                            id_rh_status,
                            id_rh_modificacao,
                            atual,
                            id_rh_site
                        )
                        VALUES 
                        (
                            :matricula, 
                            :id_rh_organizacao,  
                            :id_rh_centro_custo, 
                            :id_rh_cargo,
                            :id_rh_supervisor,
                            :id_rh_coordenador,
                            :id_rh_gerente,
                            :id_rh_status, 
                            :id_rh_modificacao,
                            TRUE,
                            :id_rh_site
                        )
                    """)
                    connection.execute(insert_query, row.to_dict())

            trans.commit()
            logging.info("load_colaborador_movimento: Fim colaborador movimento.")

        except Exception as e:
            trans.rollback()
            logging.error(f"load_colaborador_movimento: Erro ao carregar os dados: {e}")