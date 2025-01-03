"""
Este módulo realiza a extração de dados de colaboradores de uma API.

Funcionalidades:
- Extrai dados da tabela de colaboradores da API.
- Filtra os registros para incluir apenas colaboradores da empresa '1'.
- Implementa lógica de retries para tolerância a falhas temporárias durante a extração.

Funções:
- `extraction_colaborador()`: Realiza a extração dos dados de colaboradores e retorna um DataFrame com as colunas:
  - `NumEmp`: Número da empresa.
  - `NumCad`: Número do cadastro do colaborador.
  - `NomFun`: Nome do colaborador.
  - `NUMCPF`: Número do CPF.
  - `DatAdm`: Data de admissão.
  - `CodCar`: Código do cargo.
  - `CodFil`: Código da filial.
  - `CodCcu`: Código do centro de custo.
  - `TipSex`: Tipo de sexo.
  - `EstCiv`: Estado civil.
  - `GraIns`: Grau de instrução.
  - `DatNas`: Data de nascimento.
  - `CodNac`: Código da nacionalidade.
  - `DepIrf`: Dependentes para fins de IRF.
  - `ValSal`: Valor do salário.
  - `DatAfa`: Data de afastamento.
  - `CauDem`: Causa de demissão.

Requisitos:
- Biblioteca `lib.api` para interação com a API `APIRhClient`.
- Logging detalhado para monitoramento de tentativas e erros.

Exceções:
- Levanta uma exceção caso todas as tentativas de extração falhem.
"""

import time
import logging
from lib.api import APIRhClient

def extraction_colaborador(): 
    api_client = APIRhClient()
    table_name = 'R034FUN'
    fields = 'NumEmp, NumCad, NomFun, NUMCPF, DatAdm, CodCar, CodFil, CodCcu, TipSex, EstCiv, GraIns, DatNas, CodNac, DepIrf, ValSal, DatAfa, CauDem'
    
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        logging.info(f"extraction_colaborador: Tentativa {retry_count + 1} de extrair dados da tabela {table_name}.")
        try:
            df = api_client.post('integracoes', encryption=0, field_condition='TIPCOL',type_condition='=', value_condition='1', table_name=table_name, fields=fields)
            df =  df[df['NUMEMP'] == '1']
            logging.info("extraction_colaborador: Dados extraídos com sucesso.")
            return df
        except Exception as e:
            retry_count += 1
            logging.error(f"extraction_colaborador: Erro na tentativa {retry_count}: {e}")
            if retry_count < max_retries:
                time.sleep(2)
                logging.info("extraction_colaborador: Re-tentando...")

    logging.critical(f"extraction_colaborador: Todas as {max_retries} tentativas falharam.")
    raise Exception(f"Falhou após {max_retries} tentativas")