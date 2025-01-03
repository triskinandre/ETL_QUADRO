
def convert_value_float_for_pd(value_str):
    try:
        return float(value_str.replace(',', '.'))
    except ValueError:
        return value_str
