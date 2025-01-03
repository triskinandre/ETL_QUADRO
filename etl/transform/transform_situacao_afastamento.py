from utils.date import convert_date_format_for_pd

def transform_situacao_afastamento(df):
    df = df.rename(columns={'CODSIT': 'id_rh_situacao_afastamento', 'DESSIT': 'nome'})
    df['id_rh_situacao_afastamento'] = df['id_rh_situacao_afastamento'].astype(int)
    return df