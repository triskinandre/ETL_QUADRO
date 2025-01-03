def transform_centro_custo(df):
    df = df.rename(columns={'CODCCU': 'id_rh_centro_custo', 'NOMCCU': 'nome'})
    df['id_rh_centro_custo'] = df['id_rh_centro_custo'].astype(int)
    columns = ['id_rh_centro_custo', 'nome']

    return df[columns]