import abc
import datetime
import json
import os
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    """
    Класс сохраняет данные в формате JSON в указанный файл (file_path) и достаёт данные из этого файла.
    """
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        if not os.path.exists(self.file_path):
            os.mknod(self.file_path)

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as file:
            json.dump(state, file, indent=2)

    def retrieve_state(self) -> dict:
        if os.path.getsize(self.file_path) == 0:
            return {}
        with open(self.file_path, 'r') as file:
            state = json.load(file)
            return state


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.state[key] = value.isoformat()
        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state = self.state.get(key, None)
        if state:
            state = datetime.datetime.fromisoformat(state)
        return state

