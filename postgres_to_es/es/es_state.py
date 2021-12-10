import os
import abc
import json

from abc import ABC, ABCMeta
from typing import Any, Optional, Union


class BaseStorage(ABC):
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage, metaclass=ABCMeta):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w', encoding='utf-8') as file:
            json.dump(state, file, ensure_ascii=False, indent=4)

    def retrieve_state(self) -> Union[dict, None]:
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as file:
                return json.load(file)
        else:
            return None


class State:

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        if self.state is not None:
            if self.state.get(key) is not None:
                self.state[key] = value
                self.storage.save_state(state=self.state)
            else:
                return None
        else:
            return None

    def get_state(self, key: str) -> dict:

        if self.state is not None:
            return self.state.get(key)
