from utils.date import convert_date_format_for_pd

def transform_quadro_site(df):
    df = df.rename(columns={'ID': 'id_rh_site', 'DESCRICAO': 'nome'})
    df['id_rh_site'] = df['id_rh_site'].astype(int)
    return df