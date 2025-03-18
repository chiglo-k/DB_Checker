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

        def sql_transfer(self):
            return self.cur

        def add_to_bl_check(self, data):
                """Добавление данных по не закрытым доплонениям"""
                self.clean_date(key='BL')
                self.cur.executemany("""insert into bl_check_contract_all
                                        (files, contract_storage, agreement_num_st,
                                        date_storage,
                                        bl_no, vessel, seller, production)
                                        values (%s,%s,%s,%s,
                                                %s,%s,%s,%s)""", data)
                self.conn.commit()

        def add_to_conosaments_null(self, data):
                """Добовление данных по пропущенным значениям"""
                self.clean_date(key='Conosaments')
                self.cur.executemany("""insert into conosaments_null 
                                        (files, collumn_missing, table_name)
                                        values (%s, %s, %s)""", data)
                self.conn.commit()


        def fetch_agreement_inner(self):
            """Выгрузка данных по существующим записям договоров внутреннего рынка"""
            self.cur.execute("select * from agreement_data")
            column_names = [desc[0] for desc in self.cur.description]
            row = self.cur.fetchall()
            data = pd.DataFrame(row, columns=column_names)
            return data

        def add_to_agreemnent_inner(self, data):
            """Доабвления данных по договорам"""
            self.cur.executemany("""insert into agreement_data
                                    (files, seller, buyer, agreement_number
                                    ,agreement_date, transport)
                                    values (%s, %s, %s, %s, %s, %s)
                                    on conflict (agreement_number)
                                    do update set
                                    files  = EXCLUDED.files,
                                    buyer = EXCLUDED.buyer,
                                    agreement_date = EXCLUDED.agreement_date,
                                    transport = EXCLUDED.transport
                                    """, data)
            self.conn.commit()


        def delete_data_agreement_inner(self, agreement_number):
            self.cur.execute("""delete from agreement_data 
                               where agreement_number = %s""",
                            (agreement_number,))
            self.conn.commit()

        def add_to_inner_save(self, data):
            """Добавление данных по остаткам продукции на хранении"""
            self.clean_date(key='innerSave')
            self.cur.executemany(""" insert into inner_save
                                    (files, vessel, producer, production, sort,
                                     value_conosament, value_inner_market,
                                     value_remaining)
                                    values (%s, %s, %s,%s, %s, %s, %s, %s)""", data)
            self.conn.commit()

        def clean_date(self, key):
            """Очистка временных таблци"""
            if key == 'Conosaments':
                self.cur.execute("delete from conosaments_null")
                self.reset_sequence("conosaments_null", "id")
            elif key == 'BL':
                self.cur.execute("delete from bl_check_contract_all")
                self.reset_sequence('bl_check_contract_all', 'id')
            elif key == 'innerSave':
                self.cur.execute("delete from inner_save")
                self.reset_sequence("inner_save", "id")

        def reset_sequence(self, table_name, column_name):

            self.cur.execute(f"select pg_get_serial_sequence('{table_name}', '{column_name}')")
            sequence_name = self.cur.fetchone()[0]

            self.cur.execute(f"alter sequence {sequence_name} restart with 1")
            self.conn.commit()


