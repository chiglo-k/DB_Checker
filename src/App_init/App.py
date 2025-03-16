import streamlit as st
from App_init.SQLink import SQLTable
from App_init.Fesco import Fesco
from App_init.InnerViser import InnerViser
from App_init.BLCheckFormat import BLCheckF
from App_init.InnerAgreement import InnerAgreement
from dataclasses import dataclass
import pandas as pd


@dataclass
class Init:

    def __post_init__(self):
        """
        Вызов класса обработки фрейма
        :return: фреймы по видам операций
        """
        self.st = st
        self.sql = SQLTable()
        self.fesco = Fesco()
        self.inner_market_save = InnerViser()
        self.bl_check = BLCheckF()
        self.inn_agg = InnerAgreement()
        self.menu_structure = {
            "Основные функции": [
                "Инфо",
                "Анализ пропусков данных",
                "Реестр внутренний рынок"
            ],
            "Управление хранением": [
                "Закрытие хранения",
                "Хранение внутренний рынок"
            ],
            "Работа с документами": [
                "Реестр Феско"
            ]
        }

    def page_1(self):
        """
        Проверка ошибок, пропущенных значений в фрейме коносаменты
        :return: значения по которым присутствуют пропуски,
        детализированные фреймы по выбранным пропускам
        """
        st.header("Проверка пропуска данных")
        st.dataframe(self.sql.check_null_data(), column_config={"id": None})

    def page_2(self):
        """
        Класс обработки проверок закрытия дополнений хранения
        дэкспортными дополнениями
        :return: таблицу соответсвия по параметру BL
        """
        st.header("Проверка закрытия дополнений хранения")
        self.bl_check.bl_check_run()

    def page_4(self):
        """
        Проверка остатков продукции на хранении
        """
        st.header("Хранение внутренний рынок")
        self.inner_market_save.inner_create()

    def page_5(self):
        """Реестр счетов Феско/Дальрефтранс"""
        st.header("Реестр счетов Феско/Дальрефтранс")
        options = ["Реестр Феско/Дальрефтранс",
                   "Реестр не закрытых счетов Феско/Дальрефтранс",
                   "Добавить счет",
                   "Добавить информацию о платеже",
                   "Добавить новую операцию",
                   "Удалить счет"]

        selection = st.pills("Directions", options, selection_mode="single")

        if selection == "Добавить счет":
            self.fesco.form_registration()
        elif selection == "Реестр не закрытых счетов Феско/Дальрефтранс":
            self.fesco. unclosed_bill()
        elif selection == "Добавить информацию о платеже":
            self.fesco.form_close_registration()
        elif selection == "Реестр Феско/Дальрефтранс":
            self.fesco.data_fesco_show()
        elif selection == "Добавить новую операцию":
            self.fesco.add_new_service()
        elif selection == "Удалить счет":
            self.fesco.delete_fesco_bill()

    def page_6(self):
        st.header("Реестр Внутренний рынок")
        self.inn_agg.show_agreement()

    def run_app(self):
        st.sidebar.title('Checker Of DB v2')
        st.sidebar.title('Выберите раздел')

        category = st.sidebar.selectbox("Выберите категорию", list(self.menu_structure.keys()))

        # Получаем список страниц для выбранной категории
        pages = self.menu_structure[category]
        page = st.sidebar.radio("Выберите подраздел", pages)

        with st.spinner('Ожидаем обработку данных...'):
            if page == "Инфо":
                st.markdown("Проверка баз данных")
                st.caption('Анализ пропусков данных: просмотр коносаментов на пропуски в данные')
                st.caption('Закрытие хранения: анализ БД на наличие не закрытых дополнений хранения')
                st.caption('Хранение внутренний рынок: нереализованная продукция по внутреннему рынку')
                st.caption('Реестр Феско: добавление[удаление] счетов/платежных поручений по счетам грузовых операций через Магадан')
            elif page == "Анализ пропусков данных":
                self.page_1()
            elif page == "Закрытие хранения":
                self.page_2()
            elif page == "Хранение внутренний рынок":
                self.page_4()
            elif page == "Реестр Феско":
                self.page_5()
            elif page == "Реестр внутренний рынок":
                self.page_6()
