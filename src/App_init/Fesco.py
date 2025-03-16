import pandas as pd
import streamlit as st
from streamlit_dynamic_filters import DynamicFilters
from App_init.SQLink import SQLTable
from dataclasses import dataclass, field
from typing import Dict

@dataclass
class Fesco:
    #init
    main_data: pd.DataFrame = None
    data_input: Dict[str, int] = field(default_factory=dict)
    sql: SQLTable = field(init=False)

    def __post_init__(self):
        # as do like post_init functions
        self.sql = SQLTable()
        self.main_data = self.sql.bill_fesco()


    def data_fesco_show(self):
        # Select *  from
        fesco = self.sql.bill_fesco_data().fillna("")

        def highlight_unpaid(x):
            # More robust version with error handling
            return [
                'background-color: yellow' if pd.isna(val) or val == "" else ''
                for val in x
            ]

        dinamic_filters = DynamicFilters(
            df=fesco,
            filters=['transport_id', 'service']
        )
        dinamic_filters.display_filters()
        filtered_df = dinamic_filters.filter_df()

        st.dataframe(
            filtered_df.style.apply(highlight_unpaid, axis=0, subset=['bill']),
            column_config={"id": None}
        )

    def unclosed_bill(self):
        data = self.sql.bill_fesco_unclosed().fillna("")

        dinamic_filters = DynamicFilters(
            df=data,
            filters=['transport_id', 'service']
        )
        dinamic_filters.display_filters()
        filtered_df = dinamic_filters.filter_df()

        st.dataframe(filtered_df, column_config={"id": None})

    def form_registration(self):
        #create form registration
        with st.form('Fesco'):
            for column in self.main_data.columns:
                # depends of column its diff type of input data
                if column not in ['id', 'amount', 'payment', 'date_payment', 'agent', 'transport_id']:
                    if column == 'service':
                        # create select list
                        data_service = self.sql.service_fesco()
                        self.data_input[column] = st.selectbox(
                            label="Сервис",
                            options=data_service['service'].tolist()
                        )
                    elif column == 'date_bill':
                        self.data_input[column] = st.date_input(column)
                    elif column not in ['count', 'price']:
                        self.data_input[column] = st.text_input(column)
                    else:
                        self.data_input[column] = st.number_input(column, value=None)

            submit = st.form_submit_button('Добавить')
            if submit:
                self.enviar_to_sql(self.data_input, key='add_data')


    def form_close_registration(self):
        #close bill <---- here need to filter bills which are closed
        column_unique = ['bill', 'transport_id']
        unique_values = self.main_data.loc[~self.main_data['payment'].notnull()]
        unique_values = unique_values[column_unique].apply(tuple, axis=1).unique()
        select_values = st.selectbox(f"Выберите, {unique_values}")
        with st.form('Fesco'):
            for column in self.main_data.columns:
                if column in ['payment', 'date_payment', 'agent']:
                    if column in ['agent']:
                        self.data_input[column] = st.text_input(column)
                    elif column == 'date_payment':
                        self.data_input[column] = st.date_input(column)
                    else:
                        self.data_input[column] = st.text_input(column)

            submit = st.form_submit_button('Добавить')

            if submit:
                self.enviar_to_sql(self.data_input, select_values, key='add_extra')
                st.rerun()
    def delete_fesco_bill(self):
        column_unique = ['transport_id', 'serial', 'service', 'bill']
        unique_values = self.main_data[column_unique].drop_duplicates()
        select_serial = st.selectbox('Выберите серию', unique_values['serial'].unique())

        if select_serial:
            filtered_values = unique_values[unique_values['serial'] == select_serial]
            select_row = st.selectbox("Выберите запись", [tuple(row) for row in filtered_values.values])

        with st.form("Fesco"):
            submit = st.form_submit_button('Удалить')

        if submit and select_row:
            delete_values = (select_row[0], select_row[2], select_row[3])  # transport_id, service, bill
            self.enviar_to_sql(delete_values, key='delete_data')
            st.rerun()

    def add_new_service(self):
        #extra function with direct send --- how to secure?
        try:
            with st.form("Add new Servise"):
                service = st.text_input("Input Service: ")
                submit = st.form_submit_button("Добавить")
                if submit:
                    result = self.sql.add_new_service(service)
                    st.success("Данные добавлены в Базу Данных") if result else st.error("Ошибка при добавлении данных")
        except ConnectionError as cr:
            st.error(f"Ошибка соединения {cr}")


    def enviar_to_sql(self, data_tuples, select_values=None, key=None):
        try:
            if key in ['add_data', 'add_extra']:
                # Создаем кортеж значений для вставки
                data_tuples = [tuple(self.data_input.values())]
                if key == 'add_data':
                        result = self.sql.add_bill_fesco_reg(data_tuples)
                elif key == 'add_extra':
                        result = self.sql.add_bill_fesco_cls(data_tuples, select_values[0])
            elif key == 'delete_data':
                    result = self.sql.delete_bill_fesco(data_tuples)

            st.success("Данные успешно обновлены") if result else st.error("Ошибка при добавлении данных")
        except Exception as e:
            st.error(f"Ошибка: {e}")
