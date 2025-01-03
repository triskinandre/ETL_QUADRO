import hashlib

def gerar_md5_concat(valor):
    # Gera o hash MD5 a partir de uma string concatenada de valores
    return hashlib.md5(valor.encode('utf-8')).hexdigest()
