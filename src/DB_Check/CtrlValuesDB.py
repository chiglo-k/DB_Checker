import pandas as pd
from dataclasses import dataclass, field
from typing import List, Dict, Tuple
from loguru import logger
from DB_Check.SqlCheck import SQLTable

@dataclass
class CtrlValDB:
    data: List[Tuple[pd.DataFrame, str]]
    name_table: list = field(default_factory=lambda: ['Внутренний рынок', 'Хранение', 'Экспорт'])

    def __post_init__(self):
        logger.add(r"logs/log.txt", backtrace=True,
                   enqueue=True, watch=True,
                   format="{time} {level} {message}", level="WARNING")
        logger.info("Values DB function start.")

    def load_data(self, file_path):
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')

            required_columns = ['Файлы', 'Продукция', 'Судно', 'Объем, кг']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                logger.error(f"Отсутствуют обязательные столбцы: {missing_columns}")
                raise ValueError(f"Отсутствуют обязательные столбцы: {missing_columns}")

            df.columns = df.columns.str.strip()
            return df

        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {str(e)}")
            raise

    def process_frame(self, data, base_cols):
        if not isinstance(data, pd.DataFrame):
            raise TypeError("Входные данные должны быть типа pandas.DataFrame")

        if not all(col in data.columns for col in base_cols):
            raise ValueError(f"Отсутствуют необходимые столбцы: {base_cols}")

        numeric_cols = data.select_dtypes(include=['int64', 'float64']).columns
        datetime_cols = data.select_dtypes(include=['datetime64']).columns

        if len(numeric_cols) == 0 and len(datetime_cols) == 0:
            logger.warning("Нет столбцов для агрегации")
            return data[base_cols]

        # Создаем словарь агрегации
        agg_dict = {}

        for col in numeric_cols:
            if col == 'Объем, кг':
                # Для экспорта и хранения конвертируем в тонны
                if any(name in data.columns for name in ['Экспорт', 'Хранение']):
                    data['Объем, тн'] = data['Объем, кг'] / 1000
                    agg_dict['Объем, тн'] = 'sum'
                else:
                    agg_dict[col] = 'sum'
            else:
                agg_dict[col] = 'sum'

        agg_dict.update({col: 'first' for col in datetime_cols})

        try:
            result = data.groupby(base_cols).agg(agg_dict).reset_index()

            # Переименовываем колонку для внутреннего рынка
            if 'Внутренний рынок' in data.columns:
                if 'Объем, кг' in result.columns:
                    result = result.rename(columns={'Объем, кг': 'Объем, тн'})
                    result['Объем, тн'] = result['Объем, тн'] / 1000

            return result

        except Exception as e:
            logger.error(f"Ошибка при группировке данных: {str(e)}")
            raise

    def analyse_vl_db(self):
        data_cons = None
        data_inner = None
        data_strg = None
        data_exprt = None

        for data_fr, name in self.data:
            if name == 'Коносаменты':
                data_cons = data_fr
            elif name == 'Внутренний рынок':
                data_inner = data_fr
                data_inner = data_inner.rename(columns={'Изготовитель': 'Судно'})
            elif name == 'Хранение':
                data_strg = data_fr
            elif name == 'Экспорт':
                data_exprt = data_fr

        if data_cons is not None:
            required_cols = ['Файлы', 'Продукция', 'Судно', 'Вид операции']
            data_cons = self.process_frame(data_cons, required_cols)

        base_cols = ['Файлы', 'Продукция', 'Судно']

        for frame_name in self.name_table:
            if frame_name == 'Внутренний рынок' and data_inner is not None:
                data_inner = self.process_frame(data_inner, base_cols)
                self.value_res(data_cons, data_inner)
            elif frame_name == 'Хранение' and data_strg is not None:
                data_strg = self.process_frame(data_strg, base_cols)
                self.value_res(data_cons, data_strg)
            elif frame_name == 'Экспорт' and data_exprt is not None:
                data_exprt = self.process_frame(data_exprt, base_cols)
                self.value_res(data_cons, data_exprt)

    def value_res(self, data_cons, data):
        # Определяем возможные названия столбцов с объемом
        volume_columns = ['Объем, кг', 'Объем, тн']

        # Проверяем наличие столбца объема в data_cons
        found_cons = False
        for col in volume_columns:
            if col in data_cons.columns:
                volume_col_cons = col
                found_cons = True
                break

        if not found_cons:
            logger.error("В DataFrame data_cons отсутствует столбец с объемом")
            logger.error(f"Текущие столбцы: {list(data_cons.columns)}")
            raise KeyError("Отсутствует столбец с объемом в data_cons")

        # Проверяем наличие столбца объема в data
        found_data = False
        for col in volume_columns:
            if col in data.columns:
                volume_col_data = col
                found_data = True
                break

        if not found_data:
            logger.error("В DataFrame data отсутствует столбец с объемом")
            logger.error(f"Текущие столбцы: {list(data.columns)}")
            raise KeyError("Отсутствует столбец с объемом в data")

        try:
            cons_grouped = data_cons.groupby(['Файлы', 'Продукция', 'Судно'])[volume_col_cons].sum().reset_index()
            data_grouped = data.groupby(['Файлы', 'Продукция', 'Судно'])[volume_col_data].sum().reset_index()

            merged = pd.merge(
                cons_grouped,
                data_grouped,
                on=['Файлы', 'Продукция', 'Судно'],
                suffixes=('_cons', '_data'),
                how='outer',
                validate='many_to_many'
            )

            # Преобразуем все значения в тонны для сравнения
            if volume_col_cons == 'Объем, кг':
                merged[f'{volume_col_cons}_cons'] = merged[f'{volume_col_cons}_cons'] / 1000
            if volume_col_data == 'Объем, кг':
                merged[f'{volume_col_data}_data'] = merged[f'{volume_col_data}_data'] / 1000

            less_volume = merged[merged[f'{volume_col_cons}_cons'] < merged[f'{volume_col_data}_data']]

            if not less_volume.empty:
                print("\nНайдены позиции с меньшим объемом в коносаментах:")
                print(less_volume[['Файлы', 'Продукция', f'{volume_col_cons}_cons', f'{volume_col_data}_data']])

        except Exception as e:
            logger.error(f"Произошла ошибка при сравнении объемов: {str(e)}")
            raise
