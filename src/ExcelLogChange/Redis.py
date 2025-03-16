import redis
from typing import List
from dataclasses import dataclass, field
from Data_Instances.def_atr import Attr

@dataclass
class RedisInst:

    """Класс для работы с Redis и хэшами файлов"""
    new_data: List[str] = field(default_factory=list)
    list_alter: List[str] = field(default_factory=list)
    tml = Attr()

    CLIENT = redis.Redis(
        host=tml.config['redis']['host'],
        port=tml.config['redis']['port'],
        db=tml.config['redis']['db'],
        decode_responses=True,
        socket_timeout=5  # Таймаут соединения
    )

