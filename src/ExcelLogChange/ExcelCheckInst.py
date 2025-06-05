import os
from dataclasses import dataclass, field
from typing import Dict, List
import hashlib
from ExcelLogChange.Redis import RedisInst
from Data_Instances.def_atr import Attr
from urllib.parse import urlparse
import psycopg2


@dataclass
class HashFile:

    """Класс для работы с хэшами файлов"""
    tlm = Attr()
    PATH = tlm.config['redis']['path']
    file_name: List[str] = field(default_factory=list)
    dict_hash: Dict[str, str] = field(default_factory=dict)
    redis = RedisInst()

    def files_check(self) -> List[str]:
        """Находит все нужные файлы в указанной директории"""
        for roots, _, files in os.walk(self.PATH):
            if 'ВЛД' in roots or 'МГД' in roots:
                for file in files:
                    if 'Движение продукции' in file and "~$" not in file:
                        full_path = os.path.join(roots, file)
                        self.file_name.append(full_path)

    def hash_transform(self, algorithm: str = 'sha256', block_size: int = 65536) -> None:
        """Вычисляет хэши для всех найденных файлов"""
        for file_path in self.file_name:
            try:
                hash_func = getattr(hashlib, algorithm)()

                with open(file_path, 'rb') as file:
                    for block in iter(lambda: file.read(block_size), b''):
                        hash_func.update(block)

                self.dict_hash[file_path] = hash_func.hexdigest()

            except Exception as e:
                print(f"Ошибка при обработке файла {file_path}: {str(e)}")

    def match_redis_data(self):
        """Сохраняет хэши в Redis"""
        try:
            # Получаем существующие хэши
            existing_hashes = self.redis.CLIENT.hgetall('file:hash') or {}

            # Словарь для обновления
            hashes_to_update = {}

            for filename, hash_value in self.dict_hash.items():
                existing_hash = existing_hashes.get(filename)

                if existing_hash is None:
                    # Новый файл
                    self.redis.new_data.append(filename)
                    hashes_to_update[filename] = hash_value
                elif existing_hash != hash_value:
                    # Изменённый файл
                    self.redis.list_alter.append(filename)
                    hashes_to_update[filename] = hash_value

            # Массовое обновление хэшей
            if hashes_to_update:
                self.redis.CLIENT.hset('file:hash', mapping=hashes_to_update)

        except Exception as e:
            print(f"Ошибка при обработке файлов: {str(e)}")


@dataclass
class SQL():
    """Класс отправки значений в PostgreSQL"""
    new_files: List[str] = field(default_factory=list)
    alter_files: List[str] = field(default_factory=list)
    def __post_init__(self):
        tlm = Attr()
        self.db_uri = tlm.config['sql']['sql_uri']
        self.result = urlparse(self.db_uri)
        self.cur = None
        self.conn = psycopg2.connect(
            database=tlm.config['sql']['db_name'],
            user=self.result.username,
            password=self.result.password,
            host=self.result.hostname,
            port=self.result.port
        )
        self.cur = self.conn.cursor()

    def prepare_data(self, data, name):
        for file_name in data:
            file_name = str(file_name).split('/')[-4:]
            if name == 'new_files':
                self.new_files.append(str('/'.join(file_name)))
            elif name == 'alter_files':
                self.alter_files.append(str('/'.join(file_name)))

        self.queries_sql()

    def queries_sql(self):

        data_to_insert = list(zip(self.new_files or ['None'],
                                  self.alter_files or ['None']))

        self.cur.execute("""Truncate check_sys.check_files;""")

        self.cur.executemany("""INSERT INTO check_sys.check_files (new_files, alter_files) 
                            VALUES (%s, %s)""", data_to_insert)
        self.conn.commit()


def main():
    """Основная функция"""
    try:
        test = HashFile()
        test.files_check()
        test.hash_transform()
        test.match_redis_data()

        new_files = test.redis.new_data
        alter_files = test.redis.list_alter
    #Отправка значений в БД
        sql_enviar = SQL()
        sql_enviar.prepare_data(data=new_files, name='new_files')
        sql_enviar.prepare_data(data=alter_files, name='alter_files')

    except Exception as e:
        print(f"Ошибка выполнения: {str(e)}")


if __name__ == "__main__":
    main()
