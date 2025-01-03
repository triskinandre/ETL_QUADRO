from etl.extraction.extraction_status import extraction_status
from etl.transform.transform_status import transform_status
from etl.load.load_status import load_status

def service_status():
    df = extraction_status()
    df = transform_status(df)
    load_status(df)