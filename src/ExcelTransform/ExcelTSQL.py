import pandas as pd
from Data_Instances.def_atr import Attr
from dataclasses import dataclass, field
from typing import List
from ExcelTransform.SQLInst import SQLTable


@dataclass
class ParsingExcel:

    config_tml = Attr()
    sql = SQLTable()
    LIST_TABLE: List[str] = field(default_factory=list)
    data: pd.DataFrame = None

    def __post_init__(self):
        self.LIST_TABLE = self.config_tml.config['excel']['list_excel']

    def add_sql_exel(self):

        for sheet_name in self.LIST_TABLE:
            self.data = pd.read_excel(self.config_tml.config['excel']['path_excel'],
                             sheet_name=sheet_name)

            self.check_data_frame(self.data, key=sheet_name)

    def check_data_frame(self,data, key):

        data_sql:pd.DataFrame = self.sql.check_data_instance(key=key)

        if ~data.isin(data_sql):
           self.add_sql_exel(data)



t = ParsingExcel()
t.add_sql_exel()