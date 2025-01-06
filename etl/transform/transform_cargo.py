"""
Transforma e formata um DataFrame com dados de cargos.

Essa função realiza as seguintes operações no DataFrame de entrada:
1. Aplica a conversão de formato em colunas de dados especificadas.
2. Renomeia colunas do DataFrame para seguir um padrão mais claro.
3. Converte a coluna de identificação do cargo ('id_rh_cargo') para o tipo inteiro.
"""

import logging
from utils.date import convert_date_format_for_pd

def transform_cargo(df):
    date_columns = ['CODCAR', 'TITCAR']
    for date_field in date_columns:
        df[date_field] = df[date_field].apply(convert_date_format_for_pd)

    df = df.rename(columns={'CODCAR': 'id_rh_cargo', 'TITCAR': 'nome'})
    df['id_rh_cargo'] = df['id_rh_cargo'].astype(int)
    logging.info("transform_cargo: concluido")
    return df