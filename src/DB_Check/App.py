from dataclasses import dataclass
from DB_Check.CheckConosament import CheckConosament
from DB_Check.CheckBL import CheckBL
from DB_Check.FrameExcel import FrameExcel
from DB_Check.InnerAgreement import InnerAgreement
from DB_Check.InnerMarketSave import InnerMarketSave
from DB_Check.CtrlValuesDB import CtrlValDB

@dataclass
class Init:

    def __post_init__(self):
        self.excel_frame = FrameExcel()
        self.data_cons, self.data_inner, self.data_storage, self.data_exprt = self.excel_frame.frame_data()

    def check_conosaments(self):
        checker = CheckConosament((data, name) for data, name
                                  in [(self.data_cons, "Коносаменты"),
                                      (self.data_inner, "Внутренний рынок"),
                                      (self.data_storage, "Хранение"),
                                      (self.data_exprt, "Экспорт")])
        checker.start_check()

    def check_bl(self):
        missing_bl = CheckBL(data_storage=self.data_storage, data_exprt=self.data_exprt)
        missing_bl.table_missing_pivot()

    def inner_market_agreement(self):
        agreement = InnerAgreement(inner_market=self.data_inner)
        agreement.filter_agreement()

    def inner_market_save(self):
        inner_save = InnerMarketSave(data_conosament=self.data_cons, data_inner=self.data_inner)
        inner_save.check_null_inner()

    def value_control(self):
        ctrl_values = CtrlValDB((data, name) for data, name
                                in [(self.data_cons, "Коносаменты"),
                                   (self.data_inner, "Внутренний рынок"),
                                   (self.data_storage, "Хранение"),
                                   (self.data_exprt, "Экспорт")])
        ctrl_values.analyse_vl_db()

    def run(self):
        self.check_conosaments()
        self.check_bl()
        self.inner_market_save()
        self.inner_market_agreement()
