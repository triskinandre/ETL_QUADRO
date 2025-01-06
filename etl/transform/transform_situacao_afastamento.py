"""
Transforma e formata um DataFrame com dados de situações de afastamento.

Essa função realiza as seguintes operações no DataFrame de entrada:
1. Renomeia colunas para seguir um padrão mais claro.
2. Converte a coluna de identificação da situação de afastamento ('id_rh_situacao_afastamento') para o tipo inteiro.
"""

import logging
from utils.date import convert_date_format_for_pd

def transform_situacao_afastamento(df):
    df = df.rename(columns={'CODSIT': 'id_rh_situacao_afastamento', 'DESSIT': 'nome'})
    df['id_rh_situacao_afastamento'] = df['id_rh_situacao_afastamento'].astype(int)
    logging.info("transform_situacao_afastamento: concluido")
    return df