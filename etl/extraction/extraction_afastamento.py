"""
Este módulo realiza a extração de dados de afastamento por meio de uma API de integração.

Funcionalidades:
- Estabelece conexão com a API `APIRhClient`.
- Implementa lógica de retries com limite configurável para tolerância a falhas temporárias.
- Extrai dados da tabela de afastamento com base em condições específicas.

Função principal:
- `extraction_afastamento()`: Retorna um DataFrame com os registros extraídos da API.

Requisitos de Configuração:
- Log detalhado para monitoramento de cada tentativa de extração, incluindo erros.
"""

import time
import logging
from lib.api import APIRhClient
from datetime import datetime

def extraction_afastamento(): 
    api_client = APIRhClient()
    table_name = 'R038AFA'
    date = datetime.now().date()
    value_condition =  date.strftime('%d/%m/%Y')
    fields ='NumEmp, NumCad, SitAfa, DatAfa, DatTer'
    
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            logging.info(f"extraction_afastamento: Tentativa {retry_count + 1} de extrair dados da tabela {table_name}.")
            df = api_client.post('integracoes', encryption=0, field_condition='DATALT',type_condition='=', value_condition='23/09/2024', table_name=table_name, fields=fields)
            logging.info("extraction_afastamento: Dados extraídos com sucesso.")
            return df
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_afastamento: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_afastamento: Re-tentando...")

    logging.critical(f"extraction_afastamento: Todas as {max_retries} tentativas falharam.")
    raise Exception(f"Falhou após {max_retries} tentativas")