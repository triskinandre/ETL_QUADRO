"""
Transforma e formata um DataFrame com dados de sites (quadros).

Essa função realiza as seguintes operações no DataFrame de entrada:
1. Renomeia colunas para seguir um padrão mais claro.
2. Converte a coluna de identificação do site ('id_rh_site') para o tipo inteiro.
"""

import logging
from utils.date import convert_date_format_for_pd

def transform_quadro_site(df):
    df = df.rename(columns={'ID': 'id_rh_site', 'DESCRICAO': 'nome'})
    df['id_rh_site'] = df['id_rh_site'].astype(int)
    logging.info("transform_quadro_site: concluido")
    return df