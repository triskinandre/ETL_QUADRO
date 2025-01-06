"""
execution_manager.py

Este módulo centraliza a execução do processo ETL (Extração, Transformação e Load) 
para a criação e atualização de dimensões no banco de dados PostgreSQL. 
É o código principal que orquestra e gerencia as funções responsáveis 
por processar os dados de origem e carregá-los em tabelas dimensionais específicas.

Principais responsabilidades:
- Coordenar o fluxo de execução de extração, transformação e carga para cada tabela dimensional.
- Garantir a execução sequencial e organizada das funções de ETL.
- Monitorar e registrar o status de cada etapa do processo para auditoria e depuração.

Este módulo é essencial para o pipeline de dados, assegurando que as dimensões 
sejam populadas de forma consistente e confiável.

"""

import logging

# 1 - Service:
from etl.extraction.extraction_afastamento import extraction_afastamento
from etl.transform.transform_afastamento import transform_afastamento
from etl.load.load_afastamento import load_afastamento

# 2 - Cargo:
from etl.extraction.extraction_cargo import extraction_cargo
from etl.transform.transform_cargo import transform_cargo
from etl.load.load_cargo import load_cargo

# 3 - Cargo Centro Custo:
from etl.extraction.extraction_centro_custo import extraction_centro_custo
from etl.transform.transform_centro_custo import transform_centro_custo
from etl.load.load_centro_custo import load_centro_custo

# 4 - Colaborador:
from etl.extraction.extraction_colaborador import extraction_colaborador
from etl.transform.transform_colaborador import transform_colaborador
from etl.load.load_colaborador import load_colaborador

# 5 -  Gestão Quadro:
from etl.extraction.extraction_gestao_quadro import extraction_gestao_quadro

# 6 - Organização:
from etl.extraction.extraction_organizacao import extraction_organizacao
from etl.transform.transform_organizacao import transform_organizacao
from etl.load.load_organizacao import load_organizacao

# 7 - Quadro Site:
from etl.extraction.extraction_quadro_site import extraction_quadro_site
from etl.transform.transform_quadro_site import transform_quadro_site
from etl.load.load_quadro_site import load_quadro_site

# 8 - Situação Afastamento:
from etl.extraction.extraction_situacao_afastamento import extraction_situacao_afastamento
from etl.transform.transform_situacao_afastamento import transform_situacao_afastamento
from etl.load.load_situacao_afastamento import load_situacao_afastamento

# 9 - Status:
from etl.extraction.extraction_status import extraction_status
from etl.transform.transform_status import transform_status
from etl.load.load_status import load_status

# 10 - Quadro Supervisor:
from etl.extraction.extraction_supervisor_quadro import extraction_supervisor_quadro


# 1 - Service Execução:
def dim_afastamento():
    logging.info("execution_manager: afastamento")
    df = extraction_afastamento()
    df = transform_afastamento(df)
    load_afastamento(df)

# 2 - Cargo Execução:
def dim_cargo():
    logging.info("execution_manager: cargo")
    df = extraction_cargo()
    df = transform_cargo(df)
    load_cargo(df)

# 3 - Centro Custo Execução:
def dim_centro_custo():
    logging.info("execution_manager: centro custo")
    df = extraction_centro_custo()
    df = transform_centro_custo(df)
    load_centro_custo(df)

# 4 - Colaborador Execução:
def dim_colaborador():
    logging.info("execution_manager: colaborador")
    df = extraction_colaborador()
    df = transform_colaborador(df)
    load_colaborador(df)

# 5 - Gestão Quadro Execução:
def dim_gestao_quadro():
    logging.info("execution_manager: gestao quadro")
    df = extraction_gestao_quadro()
    print(df)

# 6 - Organização Execução:
def dim_organizacao():
    logging.info("execution_manager: organização")
    df = extraction_organizacao()
    df = transform_organizacao(df)
    load_organizacao(df)

# 7 - Quadro Site Execução:
def dim_quadro_site():
    logging.info("execution_manager: quadro_site")
    df = extraction_quadro_site()
    df = transform_quadro_site(df)
    load_quadro_site(df)

# 8 - Afastamento Execução:
def dim_situacao_afastamento():
    logging.info("execution_manager: afastamento")
    df = extraction_situacao_afastamento()
    df = transform_situacao_afastamento(df)
    load_situacao_afastamento(df)

# 9 - Status Execução:
def dim_status():
    logging.info("execution_manager: status")
    df = extraction_status()
    df = transform_status(df)
    load_status(df)

# 10 - Quadro Supervisor Execução:
def dim_supervisor_quadro():
    logging.info("execution_manager: quadro_supervisor")
    df = extraction_supervisor_quadro()
