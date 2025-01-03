from etl.extraction.extraction_organizacao import extraction_organizacao
from etl.transform.transform_organizacao import transform_organizacao
from etl.load.load_organizacao import load_organizacao

def service_organizacao():
    df = extraction_organizacao()
    df = transform_organizacao(df)
    load_organizacao(df)