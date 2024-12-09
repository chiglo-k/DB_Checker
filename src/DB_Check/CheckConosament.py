import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, List
import streamlit as st
from loguru import logger

@dataclass
class CheckConosament:
    """
    Проверка ошибок, пропущенных значений в фрейме коносаменты
    """
    data: pd.DataFrame
    n_data: Dict[str, int] = field(default_factory=dict)
    keys: List[str] = field(default_factory=list)

    def __post_init__(self):
        logger.add(r"logs/log.txt", backtrace=True,
                   enqueue=True, watch=True,
                   format="{time} {level} {message}", level="WARNING")
        logger.info("Check Conosament function start.")

    def start_check(self) -> None:
        try:
            null_data = self.null_check()
            self.display_null_data(null_data)
            st.session_state.show_detailed_info = True
        except FileExistsError as e:
            logger.error(f'{e} - Фрейм коносаменты поврежден')

    def null_check(self) -> Dict[str, int]:
        """
        Проверка нулевых значений по наименованиям столбцов
        по сгрупиированным таблцицам в func:process_column
        """
        for column_name in self.data.columns:
            self.process_column(column_name)
        return self.n_data

    def process_column(self, column_name: str) -> None:
        try:
            null_count = self.data[self.data[column_name].isnull()]
            if column_name == 'Файлы':
                null_count = null_count.groupby(by='Судно').agg({column_name: 'count'}).count()
            else:
                null_count = null_count.groupby(by='Файлы').agg({column_name: 'count'}).count()

            if not (null_count == 0).any():
                self.n_data[column_name] = null_count[0]
        except KeyError as e:
            logger.warning(f'{e} - Проверьте наименование полей фрейма')

    def display_null_data(self, null_data: Dict[str, int]) -> None:
        st.markdown('#### Количество пропущенных данных')
        st.divider()
        for key_name, count in null_data.items():
            st.text(f'{key_name} - {count}')
            st.divider()

    def detailed_info(self) -> None:
        """
        Выводит значения по найденным пропускам в данных
        """
        if 'show_detailed_info' in st.session_state and st.session_state.show_detailed_info:
            self.show_column_keys()

    def show_column_keys(self) -> None:
        """
        Выбор спеиального параметра
        Передача параметра в func: check_column
        """
        for name in self.n_data.keys():
            self.keys.append(name)
        # Use a string label for the selectbox
        options = st.selectbox("Select a column", options=self.keys)
        self.check_column(options)


    def check_column(self, name_collumn:str) -> None:
        """
        Вывод фрейма по пропускам
        :param name_collumn: название столбца по которому
         проводится вывод значений пропусков
        """
        try:
            null_det = self.data[self.data[name_collumn].isnull()]
            null_det = null_det.groupby(by='Файлы').agg({name_collumn: 'count'}).reset_index()
            st.dataframe(null_det)
        except IndexError as e:
            logger.error(f'{e} - Ошибка индекса фрейма.')

