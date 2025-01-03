import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from etl.service.service_organizacao import service_organizacao
from etl.service.service_centro_custo import service_centro_custo
from etl.service.service_status import service_status
from etl.service.service_colaborador import service_colaborador
from etl.service.service_cargo import service_cargo
from etl.service.service_situacao_afastamento import service_situacao_afastamento
from etl.service.service_quadro_site import service_quadro_site

from etl.service.service_colaborador_movimento import service_colaborador_movimento

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

def main():
    # DIM
    service_organizacao()
    service_centro_custo()
    service_status()
    service_colaborador()
    service_cargo()
    service_situacao_afastamento()
    service_quadro_site()

    # FAT
    service_colaborador_movimento()
    engine_postgres = create_engine(DATABASE_URL_SISTEMA)
    refresh_query = text("REFRESH MATERIALIZED VIEW homologacao.mv_colaborador_detalhes")
    with engine_postgres.connect() as connection:
        connection.execute(refresh_query)
        connection.commit()
                    


if __name__ == '__main__':  
    main()