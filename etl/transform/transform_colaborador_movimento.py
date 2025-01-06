"""
Transforma e formata um DataFrame com dados de movimentação de colaboradores.

Essa função realiza as seguintes operações no DataFrame de entrada:
1. Renomeia colunas para seguir um padrão mais claro.
2. Cria uma coluna auxiliar ('concat_colunas') que concatena várias colunas de identificação.
3. Gera um hash MD5 único para cada registro baseado na concatenação das colunas especificadas ('id_rh_modificacao').
4. Converte as colunas de identificação para o tipo inteiro.
5. Seleciona apenas as colunas relevantes para o resultado final.
"""

import logging
from utils.string import gerar_md5_concat


def transform_colaborador_movimento(df):
    df = df.rename(columns={'NUMCAD': 'matricula', 'NOMFUN': 'nome_colaborador', 'NUMEMP': 'id_rh_organizacao', 'CODCCU': 'id_rh_centro_custo', 'CODCAR': 'id_rh_cargo', 'SITAFA': 'id_rh_status'})

    df['concat_colunas'] = df['matricula'] + df['id_rh_organizacao'] + df['id_rh_centro_custo'] + df['id_rh_cargo'] + df['id_rh_status']
    df['id_rh_modificacao'] = df['concat_colunas'].apply(gerar_md5_concat)

    df['id_rh_organizacao'] = df['id_rh_organizacao'].astype(int)
    df['id_rh_centro_custo'] = df['id_rh_centro_custo'].astype(int)
    df['id_rh_cargo'] = df['id_rh_cargo'].astype(int)
    df['id_rh_status'] = df['id_rh_status'].astype(int)
    columns = ['matricula', 'nome_colaborador', 'id_rh_organizacao', 'id_rh_centro_custo', 'id_rh_cargo', 'id_rh_status', 'id_rh_modificacao']
    logging.info("transform_colaborador_movimento: concluido")


    return df[columns]