def convert_to_float(value):
    if value is not None:
        return round(float(value.replace(",", "."))) if isinstance(value, str) else round(float(value))
    return value

def convert_to_int(value):
    if value is not None:
        return int(value)
    return value