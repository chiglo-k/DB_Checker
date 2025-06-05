import streamlit as st
from dataclasses import dataclass
from typing import List
from datetime import datetime
import pandas as pd
from App_init.SQLink import SQLTable


@dataclass
class BLCheckF:


    def __post_init__(self):
        self.sql = SQLTable()
        self.contract_live_crab: List = ['401', '402', '2403',
                                         '2406', '2404', '2405',
                                         '2408', '2407', '2501',
                                         '2502', '2503']

    def bl_check_run(self):

        options: List = ["ТРК", "МСИ"]
        selection = st.pills("Выберите компанию: ", options, selection_mode="single")

        if selection == "ТРК":
            data: pd.DataFrame = self.sql.check_bl_data(key='ТРК')
            self.refactor_data(data)
        elif selection == "МСИ":
            data: pd.DataFrame = self.sql.check_bl_data(key='МСИ')
            self.refactor_data(data)

    def refactor_data(self, data):
        _DATE_AGR = 'Дата дополнения'
        data = data.loc[data['contract_storage'] != '711']
        data = data.rename(columns={'files':'Файл',
                               'contract_storage':'Контракт хранение',
                               'agreement_num_st':'Номер дополнения',
                               'date_storage':'Дата дополнения',
                               'bl_no':'BL',
                               'vessel': 'Судно',
                               'seller': 'Продавец',
                               'production': 'Продукция'
                                })

        data[_DATE_AGR] = pd.to_datetime(data[_DATE_AGR], errors='coerce')

        def highlight_column(df):
            current_date = datetime.now().date()
            days_diff = [(current_date - d.date()).days if pd.notnull(d) else 0
                         for d in data[_DATE_AGR]]

            result = []
            for d, contract in zip(days_diff, data['Контракт хранение']):
                if contract in self.contract_live_crab:
                    color = 'red' if d > 14 else ('yellow' if d > 7 else '')
                else:
                    color = 'red' if d > 35 else ('yellow' if d > 25 else '')
                result.append('background-color: ' + color if color else '')

            return result

        styled_data: pd.DataFrame = data.style.apply(highlight_column, axis=0, subset=['Файл'])
        st.dataframe(styled_data, column_config={"id": None})
