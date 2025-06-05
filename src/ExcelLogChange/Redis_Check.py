import redis
from Data_Instances.def_atr import Attr

#Доступ к секрету
tml = Attr()

# Создаём клиент
client = redis.Redis(host=tml.config['redis']['host'], port=tml.config['redis']['port'],
                     db=tml.config['redis']['db'], decode_responses=True)

# Получаем все хэши
all_hashes = client.hgetall('file:hash')
for filename, hash_value in all_hashes.items():
    print(f"Файл: {filename}")
    print(f"Хэш: {hash_value}")
    print("-" * 50)

# Получаем количество записей
count = client.hlen('file:hash')
print(f"Всего файлов: {count}")

# Проверка конкретного файла
filename = "путь_к_файлу"  # Замените на реальный путь
hash_value = client.hget('file:hash', filename)
print(f"Хэш файла {filename}: {hash_value}")