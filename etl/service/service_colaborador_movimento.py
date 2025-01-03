import pandas as pd
from etl.extraction.extraction_quadro_centro_custo_equipe import extraction_quadro_centro_custo_equipe
from etl.extraction.extraction_quadro_centro_custo import extraction_quadro_centro_custo
from etl.extraction.extraction_calaborador_movimento import extraction_calaborador_movimento
from etl.extraction.extraction_gestao_quadro import extraction_gestao_quadro

from etl.load.load_monitoria_operacao_equipe_carrefour import load_monitoria_operacao_equipe_carrefour
from etl.transform.transform_colaborador_movimento import transform_colaborador_movimento
from etl.load.load_supervisao import load_supervisao
from etl.load.load_coordenador import load_coordenador
from etl.load.load_gerente import load_gerente
from etl.load.load_colaborador_movimento import load_colaborador_movimento
from etl.load.load_monitoria_operacao_equipe import load_monitoria_operacao_equipe
from etl.load.load_monitoria_operacao import load_monitoria_operacao
from utils.string import gerar_md5_concat

def service_colaborador_movimento():
    # Extração dos dados
    df, df_rh_cargo, df_rh_centro_custo = extraction_calaborador_movimento()
    df = transform_colaborador_movimento(df)
    df = pd.merge(df, df_rh_centro_custo, on='id_rh_centro_custo', how='inner')
    #RH SENIOR 
    df_colaborador = pd.merge(df, df_rh_cargo, on='id_rh_cargo', how='inner')
    
    df_colaborador = df_colaborador[['matricula', 'nome_colaborador', 'id_rh_centro_custo', 'id_rh_hierarquia']]

    df_supervisao  = df_colaborador[df_colaborador['id_rh_hierarquia'] == 2]
    df_coordenacao = df_colaborador[df_colaborador['id_rh_hierarquia'] == 3]
    df_gerencia    = df_colaborador[df_colaborador['id_rh_hierarquia'] == 4]
    
    load_supervisao(df_supervisao)
    load_coordenador(df_coordenacao)
    load_gerente(df_gerencia)

    df['id_rh_supervisor'] = 0
    df['id_rh_coordenador'] = 0
    df['id_rh_gerente'] = 0
    df['id_rh_site'] = 0

    supervisor_map = df_supervisao.set_index('id_rh_centro_custo')['matricula'].to_dict()
    df['id_rh_supervisor'] = df['id_rh_centro_custo'].map(supervisor_map)

    coordenador_map = df_coordenacao.set_index('id_rh_centro_custo')['matricula'].to_dict()
    df['id_rh_coordenador'] = df['id_rh_centro_custo'].map(coordenador_map)

    gerente_map = df_gerencia.set_index('id_rh_centro_custo')['matricula'].to_dict()
    df['id_rh_gerente'] = df['id_rh_centro_custo'].map(gerente_map)
    # QUADRO ONLINE
    df_quadro = extraction_gestao_quadro()
    # Conversão de tipos
    df['matricula'] = df['matricula'].astype(int)
    df_quadro['id_rh_supervisor'] = df_quadro['ID_SUPERVISOR'].astype(int)
    df_quadro['id_rh_coordenador'] = df_quadro['ID_GESTOR'].astype(int)
    df_quadro['id_rh_gerente'] = df_quadro['ID_GERENTE'].astype(int)
    df_quadro['id_rh_site'] = df_quadro['ID_SITE'].astype(int)

    df_supervisao = pd.merge(df, df_quadro, left_on='matricula', right_on='id_rh_supervisor', how='inner').drop_duplicates()
    df_coordenacao = pd.merge(df, df_quadro, left_on='matricula', right_on='id_rh_coordenador', how='inner').drop_duplicates()
    df_gerencia = pd.merge(df, df_quadro, left_on='matricula', right_on='id_rh_gerente', how='inner').drop_duplicates()

    load_supervisao(df_supervisao[['matricula', 'nome_colaborador']])
    load_coordenador(df_coordenacao[['matricula', 'nome_colaborador']])
    load_gerente(df_gerencia[['matricula', 'nome_colaborador']])

    df_quadro['matricula'] = df_quadro['MATRICULA'].astype(int)

    # Converter para inteiro após arredondar os valores, tratando possíveis NaNs
    df_quadro['id_rh_centro_custo'] = df_quadro['CODIGO_CT_CUSTO'].astype(str).str.replace(' ', '').str.replace('.', '', regex=False)

    df_rh_centro_custo['id_rh_centro_custo'] = df_rh_centro_custo['id_rh_centro_custo'].astype(str).str.replace(' ', '').str.replace('.', '', regex=False)
    df_quadro = pd.merge(df_quadro, df_rh_centro_custo, on='id_rh_centro_custo', how='inner')
   
    # Atualiza o supervisor, mantendo o valor original se não houver mapeamento
    supervisor_map = df_quadro.set_index('matricula')['id_rh_supervisor'].dropna().to_dict()
    df['id_rh_supervisor'] = df['matricula'].map(supervisor_map).combine_first(df['id_rh_supervisor'])

    # Atualiza o coordenador, mantendo o valor original se não houver mapeamento
    coordenador_map = df_quadro.set_index('matricula')['id_rh_coordenador'].dropna().to_dict()
    df['id_rh_coordenador'] = df['matricula'].map(coordenador_map).combine_first(df['id_rh_coordenador'])

    # Atualiza o gerente, mantendo o valor original se não houver mapeamento
    gerente_map = df_quadro.set_index('matricula')['id_rh_gerente'].dropna().to_dict()
    df['id_rh_gerente'] = df['matricula'].map(gerente_map).combine_first(df['id_rh_gerente'])

    # Atualiza o centro de custo, mantendo o valor original se não houver mapeamento
    centrocusto_map = df_quadro.set_index('matricula')['id_rh_centro_custo'].dropna().to_dict()
    df['id_rh_centro_custo'] = df['matricula'].map(centrocusto_map).combine_first(df['id_rh_centro_custo'])

    # Atualiza o nome, mantendo o valor original se não houver mapeamento
    centrocustonome_map = df_quadro.set_index('matricula')['nome'].dropna().to_dict()
    df['nome'] = df['matricula'].map(centrocustonome_map).combine_first(df['nome'])
    # df.to_csv('quadro.csv')

    # Atualiza o site, mantendo o valor original se não houver mapeamento
    site_map = df_quadro.set_index('matricula')['id_rh_site'].dropna().to_dict()
    df['id_rh_site'] = df['matricula'].map(site_map).combine_first(df['id_rh_site'])

    df['concat_colunas'] = df['matricula'].astype(str) + df['id_rh_organizacao'].astype(str) + df['id_rh_centro_custo'].astype(str) + df['id_rh_cargo'].astype(str) + df['id_rh_status'].astype(str) + df['id_rh_supervisor'].astype(str) + df['id_rh_gerente'].astype(str) + df['id_rh_coordenador'].astype(str) + df['id_rh_site'].astype(str)
    df['id_rh_modificacao'] = df['concat_colunas'].apply(gerar_md5_concat)

    load_colaborador_movimento(df)
    
    load_monitoria_operacao(df)
    load_monitoria_operacao_equipe(df)

    df_carrefour = extraction_quadro_centro_custo()
    load_monitoria_operacao(df_carrefour)
    
    df_carrefour_equipe = extraction_quadro_centro_custo_equipe()
    load_monitoria_operacao_equipe_carrefour(df_carrefour_equipe)
