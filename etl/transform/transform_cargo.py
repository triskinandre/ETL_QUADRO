from utils.date import convert_date_format_for_pd

def transform_cargo(df):
    date_columns = ['CODCAR', 'TITCAR']
    for date_field in date_columns:
        df[date_field] = df[date_field].apply(convert_date_format_for_pd)

    df = df.rename(columns={'CODCAR': 'id_rh_cargo', 'TITCAR': 'nome'})
    df['id_rh_cargo'] = df['id_rh_cargo'].astype(int)
    return df