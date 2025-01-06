"""
Transforma e formata um DataFrame com dados de status.

Essa função realiza as seguintes operações no DataFrame de entrada:
1. Renomeia colunas para seguir um padrão mais claro.
2. Converte a coluna de identificação do status ('id_rh_status') para o tipo inteiro.
"""

import logging
from utils.date import convert_date_format_for_pd

def transform_status(df):
    df = df.rename(columns={'CODSIT': 'id_rh_status', 'DESSIT': 'nome'})
    df['id_rh_status'] = df['id_rh_status'].astype(int)
    logging.info("transform_status: concluido")
    return df