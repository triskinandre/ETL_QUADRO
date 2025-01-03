from etl.extraction.extraction_centro_custo import extraction_centro_custo
from etl.transform.transform_centro_custo import transform_centro_custo
from etl.load.load_centro_custo import load_centro_custo


def service_centro_custo():
    df = extraction_centro_custo()
    df = transform_centro_custo(df)
    load_centro_custo(df)


