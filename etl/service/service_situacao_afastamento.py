from etl.extraction.extraction_situacao_afastamento import extraction_situacao_afastamento
from etl.transform.transform_situacao_afastamento import transform_situacao_afastamento
from etl.load.load_situacao_afastamento import load_situacao_afastamento

def service_situacao_afastamento():
    df = extraction_situacao_afastamento()
    df = transform_situacao_afastamento(df)
    load_situacao_afastamento(df)