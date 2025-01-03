from etl.extraction.extraction_colaborador import extraction_colaborador
from etl.transform.transform_colaborador import transform_colaborador
from etl.load.load_colaborador import load_colaborador

def service_colaborador():
    df = extraction_colaborador()
    df = transform_colaborador(df)
    load_colaborador(df)