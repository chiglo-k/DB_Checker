import pandas as pd
from dataclasses import dataclass, field
import re
import streamlit as st
from loguru import logger

@dataclass
class CheckBL:

    """
    Проверка закрытия дополнений хранения экспортными договорами
    """

    data_storage: pd.DataFrame
    data_exprt: pd.DataFrame

    def __post_init__(self):
        logger.add(r"logs/log.txt", backtrace=True,
                   enqueue=True, watch=True,
                   format="{time} {level} {message}", level="WARNING")
        logger.info("Check BL function start.")

    def preparation(self):
        try:
            self.data_exprt['BL No'] = self.data_exprt['BL No'].fillna('NaN')
            self.data_storage['BL No'] = self.data_storage['BL No'].apply(self.remove_digit_after_letter)
            self.data_exprt['BL No'] = self.data_exprt['BL No'].apply(self.remove_digit_after_letter)
            self.data_exprt['BL No'] = self.data_exprt['BL No'].apply(self.remove_between_dashes)
        except KeyError as e:
            logger.warning(f'{e} - Проверьте наименование полей в экспорте и хранении')


    @staticmethod
    def remove_digit_after_letter(text):
        return re.sub(r'([A-Za-z])\d$', r'\1', text)

    @staticmethod
    def remove_between_dashes(text):
        return re.sub(r'-(.*?)-', '-', text)

    def table_missing_pivot(self) -> pd.DataFrame:
        """
        Группировка фреймов и вызов функции объединения
        :param data_storage_gr: сгруппированный фрейм по хранению продукции
        :param data_exprt_gr: сгрупированный фрейм по экспорту продукции
        :def create_unite_table: вызов функции объединения фреймов
        """

        self.preparation()

        data_storage_gr: pd.DataFrame = (self.data_storage
                                        .groupby(by=['Файлы', 'BL No', 'Дата'])
                                        .agg({'Дополнение к контракту':'unique'})
                                        .reset_index())

        data_exprt_gr: pd.DataFrame = (self.data_exprt
                                    .groupby(by=['Файлы', 'BL No', 'Дата'])
                                    .agg({'Дополнение к контракту':'unique'})
                                    .reset_index())

        self.create_unite_table(data_storage_gr, data_exprt_gr)


    def create_unite_table(self, data_storage_gr, data_exprt_gr):
        """
        Создание объединенного фрейма по №BL, где data_storage_gr имеет не нулевые значения,
        data_check_ex_st нулевые. Так происходит вывод фрейма по  дополнениям хранения, не имеющие
        закрытие экспортынм контрактом
        :param data_check_ex_st: Фрейм сопоставления хранения и экспорта, не закрытых
        """
        data_check_ex_st: pd.DataFrame = pd.merge(data_storage_gr,
                                                data_exprt_gr,
                                                on=['Файлы','BL No'],
                                                how='outer',
                                                suffixes=['_storage', '_export'])

        data_check_ex_st: pd.DataFrame = (data_check_ex_st
                            .loc[data_check_ex_st['Дополнение к контракту_storage']
                            .notnull() & data_check_ex_st['Дополнение к контракту_export']
                            .isnull()])

        data_check_ex_st: pd.DataFrame = (data_check_ex_st
                            .reindex(columns=['Файлы','Дата_storage',
                                            'Дата_export',  'BL No',
                                            'Дополнение к контракту_storage',
                                            'Дополнение к контракту_export']))

        self.output_table(data_check_ex_st)

    def output_table(self, data_check_ex_st):
            """
            Вывод фрейма и сохранение ошибок в отдельный файл вида excel
            """
            mistakes: pd.DataFrame = data_check_ex_st[['Файлы','Дополнение к контракту_storage','BL No']]
            mistakes.to_excel(st.secrets.data.path_to_save, header=True, index=True)

            st.dataframe(data_check_ex_st[['Файлы','Дополнение к контракту_storage','BL No']])
