
# 0 - Terceiros:
import pandas as pd
import logging
from utils.string import gerar_md5_concat

# 1 - Extraction:
from etl.extraction.extraction_quadro_centro_custo_equipe import extraction_quadro_centro_custo_equipe
from etl.extraction.extraction_quadro_centro_custo import extraction_quadro_centro_custo
from etl.extraction.extraction_calaborador_movimento import extraction_calaborador_movimento
from etl.extraction.extraction_gestao_quadro import extraction_gestao_quadro

# 2 - Tranform:
from etl.transform.transform_colaborador_movimento import transform_colaborador_movimento

# 3 - Load:
from etl.load.load_monitoria_operacao_equipe_carrefour import load_monitoria_operacao_equipe_carrefour
from etl.load.load_supervisao import load_supervisao
from etl.load.load_coordenador import load_coordenador
from etl.load.load_gerente import load_gerente
from etl.load.load_colaborador_movimento import load_colaborador_movimento
from etl.load.load_monitoria_operacao_equipe import load_monitoria_operacao_equipe
from etl.load.load_monitoria_operacao import load_monitoria_operacao



def fato_colaborador_movimento():
    logging.info("execution_manager: fato")
    """
    RH Senior:
    1 - extração da tabela
    2 - modelagem da tabela
    3 - add centro custo
    4 - merge com as informações
    5 - separando somente as colunas necessarias

    1 - criação de 3 dfs definindo a hierarquia
    2 - carregamento das informações dos 3 dfs

    1 - add informações de rh padrão = 0 para o df_colaborador_movimento

    1 - preenchendo a coluna id_rh_supervisor com a matrícula do supervisor correspondente

    1 - preenchendo a coluna id_rh_coordenador com a matrícula do coordenador correspondente

    1 - preenchendo a coluna id_rh_centro_custo com a matrícula do gerente correspondente

    Quadro Operacional MIS:
    1 - extração da tabela

    1 - conversão das matriculas para int tabela colaborador movimento

    1 - conversão para int na tabela quadro online - MIS

    1 - criação 3 dfs novos com os joins necessarios
    2 - load nos 3 dfs novo no banco do postgree

    1 - transforma a coluna matricula em int
    2 - Remove espaços e pontos da coluna CODIGO_CT_CUSTO e salva em id_rh_centro_custo.
    3 - merge entre rh_centro_custo e a df_quadro_online - MIS

    1 - dropa nulos e cria um dicionario para supervisor_map
    2 - passa esses dados do supervisor_map para o df_colaborador movimento

    1 - dropa nulos e cria um dicionario para coordenador_map
    2 - passa esses dados do coordenador_map para o df_colaborador movimento


    1 - dropa nulos e cria um dicionario para gerente_map
    2 - passa esses dados do gerente_map para o df_colaborador movimento

    1 - dropa nulos e cria um dicionario para centro_de_custo_map
    2 - passa esses dados do centro_de_custo_map para o df_colaborador movimento

    1 - dropa nulos e cria um dicionario para centro_de_custo_nome_map
    2 - passa esses dados do centro_de_custo_nome_map para o df_colaborador movimento

    1 - dropa nulos e cria um dicionario para site_map
    2 - passa esses dados do site_map para o df_colaborador movimento

    1 - junção de todas colunas
    2 - criação da hash

    1 - carrega os dados com a função: load_colaborador_movimento
    2 - carrega os dados com a função: load_monitoria_operacao
    3 - carrega os dados com a função: load_monitoria_operacao_equipe

    1 - extrai os dados criando um df chamado df_carrefour
    2 - carrega os dados no postgree do df_carrefour

    1 - extrai os dados criando um df chamado df_carrefour_equipe
    2 - carrega os dados no postgree do df_carrefour_equipe
    """

    # RH Senior:
    df_colaborador_movimento, df_rh_cargo, df_rh_centro_custo = extraction_calaborador_movimento()
    df_colaborador_movimento = transform_colaborador_movimento(df_colaborador_movimento)
    df_colaborador_movimento = pd.merge(df_colaborador_movimento, df_rh_centro_custo, on='id_rh_centro_custo', how='inner')
    df_colaborador = pd.merge(df_colaborador_movimento, df_rh_cargo, on='id_rh_cargo', how='inner')
    df_colaborador = df_colaborador[['matricula', 'nome_colaborador', 'id_rh_centro_custo', 'id_rh_hierarquia']]


    df_supervisao  = df_colaborador[df_colaborador['id_rh_hierarquia'] == 2]
    df_coordenacao = df_colaborador[df_colaborador['id_rh_hierarquia'] == 3]
    df_gerencia    = df_colaborador[df_colaborador['id_rh_hierarquia'] == 4]
    load_supervisao(df_supervisao)
    load_coordenador(df_coordenacao)
    load_gerente(df_gerencia)


    df_colaborador_movimento['id_rh_supervisor'] = 0
    df_colaborador_movimento['id_rh_coordenador'] = 0
    df_colaborador_movimento['id_rh_gerente'] = 0
    df_colaborador_movimento['id_rh_site'] = 0


    supervisor_map = df_supervisao.set_index('id_rh_centro_custo')['matricula'].to_dict()
    df_colaborador_movimento['id_rh_supervisor'] = df_colaborador_movimento['id_rh_centro_custo'].map(supervisor_map)


    coordenador_map = df_coordenacao.set_index('id_rh_centro_custo')['matricula'].to_dict()
    df_colaborador_movimento['id_rh_coordenador'] = df_colaborador_movimento['id_rh_centro_custo'].map(coordenador_map)


    gerente_map = df_gerencia.set_index('id_rh_centro_custo')['matricula'].to_dict()
    df_colaborador_movimento['id_rh_gerente'] = df_colaborador_movimento['id_rh_centro_custo'].map(gerente_map)

    # Quadro Online - MIS:
    df_quadro_online = extraction_gestao_quadro()


    df_colaborador_movimento['matricula'] = df_colaborador_movimento['matricula'].astype(int)


    df_quadro_online['id_rh_supervisor']  = df_quadro_online['ID_SUPERVISOR'].astype(int)
    df_quadro_online['id_rh_coordenador'] = df_quadro_online['ID_GESTOR'].astype(int)
    df_quadro_online['id_rh_gerente']     = df_quadro_online['ID_GERENTE'].astype(int)
    df_quadro_online['id_rh_site']        = df_quadro_online['ID_SITE'].astype(int)


    df_supervisao = pd.merge(df_colaborador_movimento, df_quadro_online, left_on='matricula', right_on='id_rh_supervisor', how='inner').drop_duplicates()
    df_coordenacao = pd.merge(df_colaborador_movimento, df_quadro_online, left_on='matricula', right_on='id_rh_coordenador', how='inner').drop_duplicates()
    df_gerencia = pd.merge(df_colaborador_movimento, df_quadro_online, left_on='matricula', right_on='id_rh_gerente', how='inner').drop_duplicates()
    load_supervisao(df_supervisao[['matricula', 'nome_colaborador']])
    load_coordenador(df_coordenacao[['matricula', 'nome_colaborador']])
    load_gerente(df_gerencia[['matricula', 'nome_colaborador']])
    

    df_quadro_online['matricula'] = df_quadro_online['MATRICULA'].astype(int)
    df_quadro_online['id_rh_centro_custo'] = df_quadro_online['CODIGO_CT_CUSTO'].astype(str).str.replace(' ', '').str.replace('.', '', regex=False)
    df_rh_centro_custo['id_rh_centro_custo'] = df_rh_centro_custo['id_rh_centro_custo'].astype(str).str.replace(' ', '').str.replace('.', '', regex=False)
    df_quadro_online = pd.merge(df_quadro_online, df_rh_centro_custo, on='id_rh_centro_custo', how='inner')


    supervisor_map = df_quadro_online.set_index('matricula')['id_rh_supervisor'].dropna().to_dict()
    df_colaborador_movimento['id_rh_supervisor'] = df_colaborador_movimento['matricula'].map(supervisor_map).combine_first(df_colaborador_movimento['id_rh_supervisor'])


    coordenador_map = df_quadro_online.set_index('matricula')['id_rh_coordenador'].dropna().to_dict()
    df_colaborador_movimento['id_rh_coordenador'] = df_colaborador_movimento['matricula'].map(coordenador_map).combine_first(df_colaborador_movimento['id_rh_coordenador'])


    gerente_map = df_quadro_online.set_index('matricula')['id_rh_gerente'].dropna().to_dict()
    df_colaborador_movimento['id_rh_gerente'] = df_colaborador_movimento['matricula'].map(gerente_map).combine_first(df_colaborador_movimento['id_rh_gerente'])


    centrocusto_map = df_quadro_online.set_index('matricula')['id_rh_centro_custo'].dropna().to_dict()
    df_colaborador_movimento['id_rh_centro_custo'] = df_colaborador_movimento['matricula'].map(centrocusto_map).combine_first(df_colaborador_movimento['id_rh_centro_custo'])


    centrocustonome_map = df_quadro_online.set_index('matricula')['nome'].dropna().to_dict()
    df_colaborador_movimento['nome'] = df_colaborador_movimento['matricula'].map(centrocustonome_map).combine_first(df_colaborador_movimento['nome'])


    site_map = df_quadro_online.set_index('matricula')['id_rh_site'].dropna().to_dict()
    df_colaborador_movimento['id_rh_site'] = df_colaborador_movimento['matricula'].map(site_map).combine_first(df_colaborador_movimento['id_rh_site'])


    df_colaborador_movimento['concat_colunas'] = df_colaborador_movimento['matricula'].astype(str) + df_colaborador_movimento['id_rh_organizacao'].astype(str) + df_colaborador_movimento['id_rh_centro_custo'].astype(str) + df_colaborador_movimento['id_rh_cargo'].astype(str) + df_colaborador_movimento['id_rh_status'].astype(str) + df_colaborador_movimento['id_rh_supervisor'].astype(str) + df_colaborador_movimento['id_rh_gerente'].astype(str) + df_colaborador_movimento['id_rh_coordenador'].astype(str) + df_colaborador_movimento['id_rh_site'].astype(str)
    df_colaborador_movimento['id_rh_modificacao'] = df_colaborador_movimento['concat_colunas'].apply(gerar_md5_concat)


    load_colaborador_movimento(df_colaborador_movimento)
    load_monitoria_operacao(df_colaborador_movimento)
    load_monitoria_operacao_equipe(df_colaborador_movimento)

    df_carrefour = extraction_quadro_centro_custo()
    load_monitoria_operacao(df_carrefour)
    
    df_carrefour_equipe = extraction_quadro_centro_custo_equipe()
    load_monitoria_operacao_equipe_carrefour(df_carrefour_equipe)
