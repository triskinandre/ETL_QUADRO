from utils.string import gerar_md5_concat


def transform_colaborador_movimento(df):
    df = df.rename(columns={'NUMCAD': 'matricula', 'NOMFUN': 'nome_colaborador', 'NUMEMP': 'id_rh_organizacao', 'CODCCU': 'id_rh_centro_custo', 'CODCAR': 'id_rh_cargo', 'SITAFA': 'id_rh_status'})

    df['concat_colunas'] = df['matricula'] + df['id_rh_organizacao'] + df['id_rh_centro_custo'] + df['id_rh_cargo'] + df['id_rh_status']
    df['id_rh_modificacao'] = df['concat_colunas'].apply(gerar_md5_concat)

    df['id_rh_organizacao'] = df['id_rh_organizacao'].astype(int)
    df['id_rh_centro_custo'] = df['id_rh_centro_custo'].astype(int)
    df['id_rh_cargo'] = df['id_rh_cargo'].astype(int)
    df['id_rh_status'] = df['id_rh_status'].astype(int)
    columns = ['matricula', 'nome_colaborador', 'id_rh_organizacao', 'id_rh_centro_custo', 'id_rh_cargo', 'id_rh_status', 'id_rh_modificacao']


    return df[columns]