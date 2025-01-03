import logging
from utils.date import convert_date_format_for_pd

def transform_afastamento(df):
    date_columns = ['DATAFA', 'DATTER']
    for date_field in date_columns:
        df[date_field] = df[date_field].apply(convert_date_format_for_pd)

    df = df.rename(columns={'DATAFA': 'data_entrada', 'DATTER': 'data_saida', 'SITAFA': 'id_rh_situacao_afastamento', 'NUMCAD': 'matricula'})
    columns = ['matricula', 'data_entrada', 'data_saida', 'id_rh_situacao_afastamento']
    logging.info("transform_afastamento: concluido")
    return df[columns]