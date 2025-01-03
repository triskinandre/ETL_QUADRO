from etl.extraction.extraction_cargo import extraction_cargo
from etl.transform.transform_cargo import transform_cargo
from etl.load.load_cargo import load_cargo

def service_cargo():
    df = extraction_cargo()
    df = transform_cargo(df)
    load_cargo(df)