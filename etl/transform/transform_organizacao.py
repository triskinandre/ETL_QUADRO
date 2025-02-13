"""
Transforma e formata um DataFrame com dados de organizações.

Essa função realiza as seguintes operações no DataFrame de entrada:
1. Aplica a conversão de formato em colunas especificadas.
2. Renomeia colunas para seguir um padrão mais claro.
3. Converte a coluna de identificação da organização ('id_rh_organizacao') para o tipo inteiro.
"""

import logging
from utils.date import convert_date_format_for_pd

def transform_organizacao(df):
    date_columns = ['NUMEMP', 'NOMFIL']
    for date_field in date_columns:
        df[date_field] = df[date_field].apply(convert_date_format_for_pd)

    df = df.rename(columns={'NUMEMP': 'id_rh_organizacao', 'NOMFIL': 'nome'})
    df['id_rh_organizacao'] = df['id_rh_organizacao'].astype(int)
    logging.info("transform_organizacao: concluido")
    return df