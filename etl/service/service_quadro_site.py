from etl.extraction.extraction_quadro_site import extraction_quadro_site
from etl.transform.transform_quadro_site import transform_quadro_site
from etl.load.load_quadro_site import load_quadro_site

def service_quadro_site():
    df = extraction_quadro_site()
    df = transform_quadro_site(df)
    load_quadro_site(df)