"""
Pipeline ETL para atualização de dimensões, fatos e materialized view do quadro de colaboradores.

Fluxo:
1. Atualização das dimensões: organização, centro de custo, status, colaborador, cargo, 
   situação de afastamento e quadro de sites.
2. Processamento da tabela de fato: `fato_colaborador_movimento`.
3. Atualização da materialized view: `mv_colaborador_detalhes`.

"""


import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Import Dimensão:
from etl.orchestrador.execution_manager_dim import(
    dim_organizacao
    ,dim_centro_custo
    ,dim_status
    ,dim_colaborador
    ,dim_cargo
    ,dim_situacao_afastamento
    ,dim_quadro_site
)


# Import Fato:
from etl.orchestrador.execution_manager_fato import fato_colaborador_movimento


# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)


load_dotenv()
DATABASE_URL_SISTEMA = os.getenv('DATABASE_URL_SISTEMA')



def main_quadro():
    dim_organizacao()
    dim_centro_custo()
    dim_status()
    dim_colaborador()
    dim_cargo()
    dim_situacao_afastamento()
    dim_quadro_site()

    fato_colaborador_movimento()
    engine_postgres = create_engine(DATABASE_URL_SISTEMA)
    refresh_query = text("REFRESH MATERIALIZED VIEW homologacao.mv_colaborador_detalhes")
    with engine_postgres.connect() as connection:
        connection.execute(refresh_query)
        connection.commit()
                    


if __name__ == '__main__':  
    main_quadro()