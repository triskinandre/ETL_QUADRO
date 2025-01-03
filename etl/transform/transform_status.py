from utils.date import convert_date_format_for_pd

def transform_status(df):
    df = df.rename(columns={'CODSIT': 'id_rh_status', 'DESSIT': 'nome'})
    df['id_rh_status'] = df['id_rh_status'].astype(int)
    return df