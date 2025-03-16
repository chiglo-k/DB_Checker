import os
from dataclasses import dataclass, field
from typing import Dict, List
import hashlib
from ExcelLogChange.Redis import RedisInst
from Data_Instances.def_atr import Attr


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


def main():
    """Основная функция"""
    try:
        test = HashFile()
        test.files_check()
        test.hash_transform()
        test.match_redis_data()

        print(f"Новые файлы: {test.redis.new_data}")
        print(f"Изменённые файлы: {test.redis.list_alter}")

    except Exception as e:
        print(f"Ошибка выполнения: {str(e)}")


if __name__ == "__main__":
    main()