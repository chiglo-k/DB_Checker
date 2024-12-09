from dataclasses import dataclass
import streamlit as st
import hmac
from DB_Check.CheckConosament import CheckConosament
from DB_Check.CheckBL import CheckBL
from DB_Check.FrameExcel import FrameExcel

@dataclass
class Init:

    def __post_init__(self):
        """
        Вызов класса обработки фрейма
        :return: фреймы по видам операций
        """
        self.st = st
        self.excel_frame = FrameExcel()
        self.data_cons, self.data_inner, self.data_storage, self.data_exprt = self.excel_frame.frame_data()


    @staticmethod
    def page_1():
        """
        Проверка ошибок, пропущенных значений в фрейме коносаменты
        :return: значения по которым присутствуют пропуски,
        детализированные фреймы по выбранным пропускам
        """
        st.header("Проверка Конносаментов")
        checker = CheckConosament(Init().data_cons)
        checker.start_check()
        checker.detailed_info()

    @staticmethod
    @st.cache_data
    def page_2():
        """
        Класс обработки проверок закрытия дополнений хранения
        дэкспортными дополнениями
        :return: таблицу соответсвия по параметру BL
        """
        st.header("Проверка закрытия дополнений хранения")
        missing_bl = CheckBL(data_storage=Init().data_storage, data_exprt=Init().data_exprt)
        missing_bl.table_missing_pivot()

    def run(self):
        """
        Аутентификация пользователя, после проверки запускается
        основная часть приложения
        """
        def login_form():
           with st.form("Creds"):
               st.text_input("Username", key="username")
               st.text_input("Password", type="password", key="password")
               st.form_submit_button("log in", on_click=password_entered)
        def password_entered():

            if st.session_state["username"] in st.secrets[
                "passwords"
            ] and hmac.compare_digest(
                st.session_state["password"],
                st.secrets.passwords[st.session_state["username"]],
            ):
                st.session_state["password_correct"] = True
                del st.session_state["password"]  # Don't store the username or password.
                del st.session_state["username"]
            else:
                st.session_state["password_correct"] = False

        if st.session_state.get("password_correct", False):
            self.run_app()
        else:
            login_form()


    def run_app(self):
        st.markdown('#### Checker Of DB v1.0')
        st.sidebar.title('Разделы')

        page = st.sidebar.radio("Выберите раздел", ["Инфо","Анализ пропусков данных", "Закрытие хранения"])

        with st.spinner('Ожидаем обработку данных...'):
            if page == "Инфо":
                st.markdown("Проверка баз данных")
                st.caption('Анализ пропусков данных: просмотр коносаментов на пропуски в данные')
                st.caption('Закрытие хранения: анализ БД на наличие не закрытых дополнений хранения')
            elif page == "Анализ пропусков данных":
                Init.page_1()
            elif page == "Закрытие хранения":
                Init.page_2()
