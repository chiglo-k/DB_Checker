import pandas as pd
import datetime as dt
from dataclasses import dataclass, field
from loguru import  logger
from DB_Check.SqlCheck import SQLTable

@dataclass
class InnerAgreement:

    """
    Реестр договоров внутреннего рынка.
    Собирается в один фрейм затем на SQL сервере
    с помощью тригерных функций происходит распределние договоров
    по компаниям.
    """

    inner_market: pd.DataFrame
    sql: SQLTable = None

    def __post_init__(self):
        self.sql = SQLTable()
        logger.add(r"logs/log.txt", backtrace=True,
                   enqueue=True, watch=True,
                   format="{time} {level} {message}", level="WARNING")
        logger.info("InnerAgreement function start.")


    def filter_agreement(self):
        agreement_data_frame: pd.DataFrame = self.inner_market.loc[self.inner_market["Дата Договора"]
                                                             >= f"01.01.{str(dt.datetime.now().year)}"].copy()
        agreement_data_frame['Договор поставки'] = (agreement_data_frame['Договор поставки'])

        agreement_data_frame = (agreement_data_frame.loc[:, ['Файлы', 'Продавец',
                          'Покупатель', 'Договор поставки',
                          'Дата Договора', 'Транспорт'
                          ]]).dropna().astype(str).values.tolist()

        agreement_data_sql: pd.DataFrame = self.sql.fetch_agreement_inner()

        self.comparasion_data(agreement_data_frame, agreement_data_sql)


    def comparasion_data(self, agreement_data_frame, agreement_data_sql):
        # Convert list to DataFrame
        df_frame = pd.DataFrame(agreement_data_frame, columns=['Файлы', 'Продавец',
                                                             'Покупатель', 'Договор поставки',
                                                             'Дата Договора', 'Транспорт'])

        frame_numbers = df_frame['Договор поставки'].tolist()
        sql_numbers = agreement_data_sql['agreement_number'].tolist()

        # Update existing records
        for number in frame_numbers:
            if number in sql_numbers:
                ### Add log update
                self.sql_enviar(df_frame[df_frame['Договор поставки'] == number],
                              keyword='update_data')

            else:
                ### Add log isnert
                self.sql_enviar(df_frame[df_frame['Договор поставки'] == number],
                              keyword='add_data')

        # Delete missing records if not in new then delete
        for number in sql_numbers:
            if number not in frame_numbers:
                self.sql_enviar(number, keyword='delete_data')

    def sql_enviar(self, data, keyword=None):
        if keyword == 'delete_data':
            # Remove 'clause=' keyword
            self.sql.delete_data_agreement_inner(data)
        elif keyword == 'add_data' or keyword == 'update_data':
            # Convert DataFrame row to list of tuples
            data_tuple = [tuple(row) for row in data.values]
            self.sql.add_to_agreemnent_inner(data=data_tuple)
