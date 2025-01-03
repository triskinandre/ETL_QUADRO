from etl.extraction.extraction_supervisor_quadro import extraction_supervisor_quadro


def service_supervisor_quadro():
    df = extraction_supervisor_quadro()
    print(df)