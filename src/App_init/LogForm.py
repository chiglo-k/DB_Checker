from dataclasses import dataclass
import streamlit as st
from App_init.App import Init
import hmac

@dataclass
class LogForm:
    def __post_init__(self):
        self.st = st
        self.st_app = Init()

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
            if st.session_state["username"] in st.secrets["passwords"] and hmac.compare_digest(
                st.session_state["password"],
                st.secrets.passwords[st.session_state["username"]],
            ):
                st.session_state["password_correct"] = True
                del st.session_state["password"]
                del st.session_state["username"]
            else:
                st.session_state["password_correct"] = False
                st.error('This is an error', icon="🚨")

        if st.session_state.get("password_correct", False):
            self.st_app.run_app()
        else:
            login_form()
