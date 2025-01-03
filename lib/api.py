import os
from dotenv import load_dotenv
import pandas as pd
import xmltodict
import requests

load_dotenv()
RH_SENIOR_URLBASE = os.getenv('RH_SENIOR_URLBASE')
USERNAME = os.getenv('RH_SENIOR_USERNAME')
PASSWORD = os.getenv('RH_SENIOR_PASSWORD')


class APIRhClient:
    def __init__(self):
        self.base_url = RH_SENIOR_URLBASE
        self.username = USERNAME
        self.password = PASSWORD
        self.session = requests.Session()

    def create_xml(self):
        payload = f"""
                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://services.senior.com.br">
                  <soapenv:Body>
                    <ser:ConsultarTabelas>
                      <user>{self.username}</user>
                      <password>{self.password}</password>
                      <encryption>{self.encryption}</encryption>
                      <parameters>
                        <consulta>
                          <id></id>
                          <tabela>{self.table_name}</tabela>
                          <campos>{self.fields}</campos>
                          <ordenacao></ordenacao>
                          <filtro>
                            <campo>{self.field_condition}</campo>
                            <condicao>{self.type_condition}</condicao>
                            <valor>{self.value_condition}</valor>
                          </filtro>
                        </consulta>
                      </parameters>
                    </ser:ConsultarTabelas>
                  </soapenv:Body>
                </soapenv:Envelope>
                """
        return payload

    def request(self, method, url):
        try:
            self.response = self.session.request(method, url, data=self.payload)
            self.response.encoding = 'utf-8'
            self.xmlDict = xmltodict.parse(self.response.text)
        
            
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None
            
    def __montaCmd(self):
        ss = ''
        for s in self.__lst_seq:
            ss = ss + f"['{s}']"
        ss = 'self.xmlDict' + ss
            
        return ss
    def post(self, endpoint, encryption=0, table_name='', fields='', field_condition = '', type_condition= '', value_condition=''):
        url = f"{self.base_url}{endpoint}"
        self.__lst_seq = ['S:Envelope', 'S:Body', 'ns2:ConsultarTabelasResponse',
                          'result', 'ocorrencia', 'resultado']
        
        self.encryption = encryption
        self.value_condition = value_condition
        
        self.table_name = table_name
        self.fields = fields
        self.field_condition = field_condition
        self.type_condition = type_condition
        self.value_condition = value_condition
        self.payload = self.create_xml()
        self.request('POST', url)
        lst_ds = eval(self.__montaCmd())
        self.ds = [x['campo'] for x in lst_ds]
        
        # prepara para o formato de dataset e monta o dataset
        lst_ds = [{col['nome']: col['valor'] for col in row} for row in self.ds]
        self.df = pd.DataFrame(lst_ds)

        return  self.df