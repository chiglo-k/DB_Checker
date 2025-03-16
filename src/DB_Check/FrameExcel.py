import pandas as pd
from dataclasses import dataclass, field
from loguru import logger
import re
from Data_Instances.def_atr import Attr

@dataclass
class FrameExcel:

    """
    Создание датафреймов из excel файла.
    Так как свод таблиц не меняется при обновлении Query, сделал присвоение таблиц к переменным.
    Файл находится в облаке OneDrive, обновление по скрипту, каждый день.
    """
    tlm = Attr()
    path = tlm.config['excel']['path_excel']
    data : pd.ExcelFile = field(init=False)
    data_cons : pd.DataFrame = None
    data_inner : pd.DataFrame = None
    data_storage : pd.DataFrame = None
    data_exprt : pd.DataFrame = None

    def __post_init__(self):
        logger.add(r"logs/log.txt", backtrace=True,
                   enqueue=True, watch=True,
                   format="{time} {level} {message}", level="WARNING")
        logger.info("Start session.")
        try:
            self.data = pd.ExcelFile(self.path)
        except FileNotFoundError as e:
            logger.error(f"{e} - Ошибка: Файл не найден.")
        except PermissionError as e:
            logger.error(f"{e} - Ошибка: Отсутствует разрешение на чтение файла.")
        except Exception as e:
            logger.error(f"{e} - Неожиданная ошибка: {str(e)}")


    def path_reducer(self):
        for data in [self.data_cons, self.data_inner, self.data_storage, self.data_exprt]:
            column_name = 'Файлы'
            data[column_name] = data[column_name].apply(lambda x: tuple(re.split(r"\\", x)[-3:-1]))
            data[column_name] = data[column_name].apply(lambda x: ', '.join(map(str, x)))

    def frame_data(self):
        """
        Функция возвращает pd.Dataframe из листов в excel файле
        и размещенных по разным операциям.
        :return: data_cons (Таблица коносаменты), data_inner (Таблица по внутреннему рынку),
        data_storage (Таблица хранения), data_exprt (Таблица по экспорту)
        """
        try:
            for i, name in enumerate(self.data.sheet_names):  # индексация по порядку размещения листов в книге, начало с 0.
                if name == "Коносаменты_21_24":
                    self.data_cons =  pd.read_excel(self.path, sheet_name=name)
                elif name == "ВнутреннийР":
                    self.data_inner = pd.read_excel(self.path, sheet_name=name)
                elif name == "ХранениеЭ":
                    self.data_storage = pd.read_excel(self.path, sheet_name=name)
                elif name == "ЭкспортП":
                    self.data_exprt = pd.read_excel(self.path, sheet_name=name)
                else:
                    logger.error(f"Лист {name} отсутствует в Excel-файле")
            self.path_reducer()
        except FileNotFoundError as e:
                logger.error('Файл не найден')
        except PermissionError as e:
                logger.error(f"{e} - Ошибка: Отсутствует разрешение на чтение файла.")
        except pd.errors.EmptyDataError as e:
                logger.error(f"{e} - Ошибка: В файле нет данных.")

        return self.data_cons,self.data_inner, self.data_storage, self.data_exprt





