import pandas as pd

def convert_date_format_for_pd(date_str):
    try:
        # Primeiro tenta converter a data assumindo o formato %d/%m/%Y
        return pd.to_datetime(date_str, format='%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError:
        # Se a conversão falhar, assume que a data já está no formato %Y-%m-%d
        return date_str
