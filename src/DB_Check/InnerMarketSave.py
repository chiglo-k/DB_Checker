import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, List
from loguru import logger
from DB_Check.SqlCheck import SQLTable


@dataclass
class InnerMarketSave:

    data_conosament: pd.DataFrame
    data_inner: pd.DataFrame

    def __post_init__(self):
        self.sql = SQLTable()
        logger.add(r"logs/log.txt", backtrace=True,
                   enqueue=True, watch=True,
                   format="{time} {level} {message}", level="WARNING")
        logger.info("InnermarketSave function start.")

    def group_conosament(self)-> pd.DataFrame:

        data_conosament: pd.DataFrame = self.data_conosament.where(self.data_conosament['Вид операции'] == 'Внутренний рынок').dropna()
        check_conosament: pd.DataFrame = data_conosament.groupby(by=["Файлы","Судно",
                                                                          "Грузоотправитель", "Продукция",
                                                                          "Сорт"])["Объем, кг"].sum().reset_index()

        check_conosament = check_conosament.rename(columns={"Объем, кг": "Объем_кн"})

        return check_conosament

    def group_inner_market(self)-> pd.DataFrame:

        check_inner: pd.DataFrame = self.data_inner.groupby(by=["Файлы","Продавец",
                                                                "Изготовитель",
                                                                "Продукция",
                                                                "Сорт"])["Объем, кг"].sum().reset_index()
        check_inner = check_inner.rename(columns={
            "Изготовитель": "Судно",
            "Продавец": "Грузоотправитель",
            "Объем, кг": "Объем_вн"
        })

        return check_inner


    def merge_holding(self):
        # Сгруппированные фреймы
        conosament_df = self.group_conosament()
        inner_market_df = self.group_inner_market()

        # Мердж по определнным колонкам
        merge_hold = pd.merge(conosament_df, inner_market_df,
                             on=["Файлы", "Судно", "Грузоотправитель", "Продукция", "Сорт"],
                             how="inner")

        return merge_hold

    def check_null_inner(self):

        null_check = self.merge_holding().where(self.merge_holding()['Объем_кн'] != self.merge_holding()['Объем_вн'])
        null_check[["Файлы","Продукция","Объем_кн","Объем_вн"]] = (null_check[["Файлы","Продукция",
                                                                               "Объем_кн","Объем_вн"]].dropna())
        null_check['Объем_остаток'] = null_check["Объем_кн"] - null_check["Объем_вн"]
        null_check = null_check.dropna().values.tolist()
        self.sql_enviar(null_check)

    def sql_enviar(self, null_check):
         data_to_sql = SQLTable()
         data_to_sql.add_to_inner_save(data=null_check)
