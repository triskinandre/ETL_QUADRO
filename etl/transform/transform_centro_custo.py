"""
Transforma e formata um DataFrame com dados de centros de custo.

Essa função realiza as seguintes operações no DataFrame de entrada:
1. Renomeia colunas para seguir um padrão mais claro.
2. Converte a coluna de identificação do centro de custo ('id_rh_centro_custo') para o tipo inteiro.
3. Seleciona apenas as colunas relevantes para o resultado final.
"""

import logging

def transform_centro_custo(df):
    df = df.rename(columns={'CODCCU': 'id_rh_centro_custo', 'NOMCCU': 'nome'})
    df['id_rh_centro_custo'] = df['id_rh_centro_custo'].astype(int)
    columns = ['id_rh_centro_custo', 'nome']
    logging.info("transform_centro_custo: concluido")

    return df[columns]