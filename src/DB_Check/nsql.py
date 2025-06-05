import pandas as pd
import psycopg2
from urllib.parse import urlparse
from Data_Instances.def_atr import Attr

class SQLTable():
        def __init__(self):
                tlm = Attr()
                self.db_uri = tlm.config['sql']['sql_uri']
                self.result = urlparse(self.db_uri)
                self.cur = None
                self.conn = psycopg2.connect(
                    database=tlm.config['sql']['db_name'],
                    user=self.result.username,
                    password=self.result.password,
                    host=self.result.hostname,
                    port=self.result.port
                )
                self.cur = self.conn.cursor()

        def print(self):
            result = urlparse(self.db_uri)
            print(result.hostname, result.username,result.password,result.port)



t = SQLTable()
t.print()

