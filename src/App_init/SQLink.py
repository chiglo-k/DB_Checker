import psycopg2
from urllib.parse import urlparse
import pandas as pd
import re
import streamlit as st

class SQLTable():
    def __init__(self):
        self.db_uri = st.secrets['data']['uri']
        self.result = urlparse(self.db_uri)
        self.cur = None
        self.conn = psycopg2.connect(
            database=st.secrets['data']['db_name'],
            user=self.result.username,
            password=self.result.password,
            host=self.result.hostname,
            port=self.result.port
        )
        self.cur = self.conn.cursor()

    def _query_to_dataframe(self, query, params=None):
        """
        Преобразует результат SQL-запроса в pandas DataFrame
        """
        try:
            if params is None:
                self.cur.execute(query)
            else:
                self.cur.execute(query, params)

            column_names = [desc[0] for desc in self.cur.description]
            rows = self.cur.fetchall()
            return pd.DataFrame(rows, columns=column_names)
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return None

    def check_bl_data(self, key):
        query = f"""select * from bl_check_contract_all where seller LIKE '%{key}%'"""
        data = self._query_to_dataframe(query)

        if data is not None:
            # Очистка данных
            for column in data.columns:
                if column in ["contract_storage", "agreement_num_st"]:
                    data[column] = data[column].apply(lambda x: re.sub(r"[^\d]", "", str(x)))
                elif column in ["vessel", "seller"]:
                    data[column] = data[column].apply(lambda x: re.sub(r"[^a-zA-Zа-яА-Я]", " ", str(x)))
                elif column == "production":
                    data[column] = data[column].apply(lambda x: x.strip('[]\''))

        return data

    def check_null_data(self):
        query = 'select * from conosaments_null'
        data = self._query_to_dataframe(query)

        if data is not None:
            data['conosaments'] = data['conosaments'].apply(lambda x: re.sub(r'[^\d\/-]', '', str(x)))

        return data

    def registry_inner_market(self):
        query = 'select * from agreement_data'
        data = self._query_to_dataframe(query)

        if data is not None:
            # Извлечение только цифр из agreement_number
            data['sort_num'] = data['agreement_number'].str.extract('(\d+)').astype(int)

            # Сортировка по извлеченным номерам
            data = data.sort_values('sort_num', ascending=True)

            # Удаление временного столбца сортировки
            data = data.drop('sort_num', axis=1)

        return data

    def save_inner(self, year):
        query = f"""Select * From inner_save Where files like '%{year}%'"""
        return self._query_to_dataframe(query)

    def bill_fesco(self):
        query = "Select * From fesco_bill limit 10"
        return self._query_to_dataframe(query)

    def bill_fesco_data(self):
        query = "Select * From fesco_bill"
        return self._query_to_dataframe(query)

    def bill_fesco_unclosed(self):
        query = "Select * From fesco_bill Where payment is Null"
        return self._query_to_dataframe(query)

    def service_fesco(self):
        query = """select service from sevice_fesco"""
        return self._query_to_dataframe(query)

    def add_new_service(self, service):
        query = """insert into sevice_fesco (service) values (%s)"""
        try:
            self.cur.execute(query, (service,))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка при добавлении сервиса: {e}")
            return False

    def add_bill_fesco_reg(self, data):
        query = """
            insert into fesco_bill 
            (bill, transport, serial, service, count, price, date_bill)
            values (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.cur.executemany(query, data)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка при добавлении счета: {e}")
            return False

    def add_bill_fesco_cls(self, data, clause):
        query = """
            UPDATE fesco_bill
            SET payment = %s, date_payment = %s, agent = %s
            WHERE bill = %s
        """
        try:
            self.cur.execute(query, (*data[0], clause))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка при обновлении счета: {e}")
            return False

    def delete_bill_fesco(self, clause):
        query = "delete from fesco_bill where transport_id = %s and service = %s and bill = %s"
        try:
            self.cur.execute(query, clause)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка при удалении счета: {e}")
            return False

    def sql_table_rev(self, company, city):
        query = f"""select * from agreement_data_{company}{city}_rev"""
        return self._query_to_dataframe(query)

    def sql_update_rev(self, data, company, city):
        query = f"""
            UPDATE agreement_data_{company}{city}_rev
            SET signed_agreement = %s, signed_upd = %s
            WHERE agreement_number = %s
        """
        try:
            self.cur.executemany(query, data)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка при обновлении ревизии: {e}")
            return False


