from utils.date import convert_date_format_for_pd
from utils.number import convert_value_float_for_pd

def transform_colaborador(df):
    date_columns = ['DATADM', 'DATNAS', 'DATAFA']
    for date_field in date_columns:
        df[date_field] = df[date_field].apply(convert_date_format_for_pd)

    
    value_columns = ['VALSAL']
    for value_field in value_columns:
        df[value_field] = df[value_field].apply(convert_value_float_for_pd)
    
    df = df.rename(columns={'NUMCAD': 'matricula', 'NUMCPF': 'cpf', 'NOMFUN': 'nome_completo',  'DATADM': 'data_admissao', 'DATNAS': 'data_nascimento'})
    df['matricula'] = df['matricula'].astype(int)
    columns = ['matricula', 'nome_completo', 'cpf', 'data_admissao', 'data_nascimento']

    return df[columns]