from etl.extraction.extraction_gestao_quadro import extraction_gestao_quadro


def service_gestao_quadro():
    df = extraction_gestao_quadro()
    print(df)