from utils.date import convert_date_format_for_pd

def transform_organizacao(df):
    date_columns = ['NUMEMP', 'NOMFIL']
    for date_field in date_columns:
        df[date_field] = df[date_field].apply(convert_date_format_for_pd)

    df = df.rename(columns={'NUMEMP': 'id_rh_organizacao', 'NOMFIL': 'nome'})
    df['id_rh_organizacao'] = df['id_rh_organizacao'].astype(int)
    return df