import psycopg2
import pandas as pd
from urllib.parse import urlparse

from pyarrow.jvm import schema

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


        def check_data_instance(self, key:str):
            if 'Коносамент' in key:
                self.cur.execute("""Select * From analyst.conosaments""")
                column_names = [row[0] for row in self.cur.description]
                rows = self.cur.fetchall()
                data =  pd.DataFrame(rows, columns=column_names)
                return data

        def add_data_new(self,data, key: str):
            if 'Коносамент' in key:
                self.cur.executemany({"""Insert into conosaments
                            (file, conosament, date_of_operation,
                            vessel, transport, company, production,
                            sort, pack, places, value,
                            operation, path_track) Values
                            (%s, %s, %s, 
                            %s, %s, %s, %s, 
                            %s, %s, %s, %s, 
                            %s, %s)""", data})

                self.conn.commit()

        def alter_data_slq(self, key: str):
            pass