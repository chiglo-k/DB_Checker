import pandas as pd
from App_init.SQLink import SQLTable
from streamlit_dynamic_filters import DynamicFilters
import streamlit as st
from dataclasses import dataclass, field

@dataclass
class InnerAgreement:
    """Вывод данных реестра"""
    sql: SQLTable = field(init=False)

    def __post_init__(self):

        self.sql = SQLTable()

    def show_agreement(self):

        data_sell = self.sql.registry_inner_market()

        duplicate_mask = data_sell.duplicated(subset='agreement_number', keep=False)
        data_sell = data_sell[duplicate_mask].copy()

        def highlight_duplicates(x):
            """Если есть дубликаты, выделение красным"""
            return ['background-color: red' if i in data_sell.index else ''
                    for i in x.index]

        # Отображаем результат
        st.dataframe(
            data_sell.style.apply(highlight_duplicates, axis=0, subset=['agreement_number']),
            column_config={"id": None}
        )

