from etl.extraction.extraction_afastamento import extraction_afastamento
from etl.transform.transform_afastamento import transform_afastamento
from etl.load.load_afastamento import load_afastamento

def service_afastamento():
    df = extraction_afastamento()
    df = transform_afastamento(df)
    load_afastamento(df)