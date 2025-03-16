import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, List, Tuple
from loguru import logger
from DB_Check.SqlCheck import SQLTable

@dataclass
class CheckConosament:
    """
    Проверка ошибок, пропущенных значений в фрейме коносаменты
    """
    data: List[Tuple[pd.DataFrame, str]]  # Use capital Tuple from typing

    def __post_init__(self):
        logger.add(r"logs/log.txt", backtrace=True,
                   enqueue=True, watch=True,
                   format="{time} {level} {message}", level="WARNING")
        logger.info("Check Conosament function start.")

    def start_check(self) -> None:
        try:
            null_data = self.null_check_values()
            self.display_null_data(null_data)
        except FileExistsError as e:
            logger.error(f'{e} - Фрейм коносаменты поврежден')

    def null_check_values(self) -> pd.DataFrame:
        """
        Проверка нулевых значений по наименованиям столбцов
        """
        missing_data_frames = []
        column_exclude = ['Сорт', 'Отгрузка с', 'MSC', 'BL mod', 'Получатель']
        for data_analyse, name in self.data:
            for column_name in data_analyse.columns:
                if column_name not in column_exclude:
                    null_rows = data_analyse[data_analyse[column_name].isnull()]
                    if name == "Коносаменты":
                        null_rows = null_rows.loc[null_rows['Коносамент'].notnull()] #delete string with null conosament num
                    if not null_rows.empty:
                        null_rows = null_rows.copy()
                        null_rows['Пропуск в колонке'] = column_name
                        null_rows['Фрейм'] = name
                        missing_data_frames.append(null_rows)
            if missing_data_frames:
                return pd.concat(missing_data_frames, axis=0)

    def display_null_data(self, null_data: pd.DataFrame) -> None:
        if null_data.empty:  # если нет пропущенных значений
            sql_table = SQLTable()
            sql_table.clean_date(key='Conosaments')
        else:  # если есть пропущенные значения
            self.sql_enviar(null_data)

    def sql_enviar(self, null_data: pd.DataFrame) -> None:
        try:
            data_to_sql = null_data[['Файлы',
                                     'Пропуск в колонке', 'Фрейм']].astype(str).values.tolist()
            sql_table = SQLTable()
            sql_table.add_to_conosaments_null(data=data_to_sql)
        except ValueError as e:
            logger.error(f'{e} - Проверьте данные')

