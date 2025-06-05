import pandas as pd
from App_init.SQLink import SQLTable
import streamlit as st
from dataclasses import dataclass, field
from datetime import datetime as dt

@dataclass
class InnerViser:

    data: pd.DataFrame = None
    sql: SQLTable = field(init=False)


    def __post_init__(self):
        self.sql = SQLTable()


    def higlight_column(self, data, df):
        return (['background-color: green'
             if df.loc[i, 'Остаток'] > 0
             else 'background-color: red'
             for i in df.index])

    def inner_create(self):

        year_previous = dt.now().year - 1
        cur_year = dt.now().year
        selected = st.sidebar.selectbox("Год проверки", [f"{cur_year}", f"{year_previous}"], key="Года")

        if selected:
            data = self.sql.save_inner(year=int(selected))
            data= data.rename(columns={'files':'Файл',
                               'vessel':'Судно',
                               'producer':'Компания',
                               'production':'Продукция',
                               'sort':'Сорт',
                               'value_conosament': 'Значение по коносаментам',
                               'value_inner_market': 'Значение внутренний рынок',
                               'value_remaining': 'Остаток'
                                })
            styled_data = data.style.apply(self.higlight_column, df=data, subset=['Файл'])\
                .format({
                'Значение по коносаментам': '{:.3f}',
                'Значение внутренний рынок': '{:.3f}',
                'Остаток': '{:.3f}'
            })


            st.dataframe(styled_data, column_config={"id": None})
