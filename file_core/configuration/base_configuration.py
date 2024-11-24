from __future__ import annotations

from abc import ABC
from enum import StrEnum

import pandas as pd
from pydantic import BaseModel


class PersistenceType(StrEnum):
    MEMORY = 'memory'
    ROW = 'row'


class PersistenceConfiguration(BaseModel):
    persist_name: str
    replace_value: bool = False
    persist_type: PersistenceType


class RowOperation(ABC, BaseModel):
    persistence: PersistenceConfiguration = None

    def __persist_memory(self, row: pd.Series, value: any, memory: dict) -> None:
        if self.persistence.persist_name not in memory:
            memory[self.persistence.persist_name] = value
        elif self.persistence.replace_value:
            memory[self.persistence.persist_name] = value
        else:
            raise ValueError(
                f'Persist name {self.persistence.persist_name} already exists and is marked as not replaceable')

    def __persist_row(self, row: pd.Series, value: any) -> None:
        if self.persistence.persist_name not in row:
            row[self.persist_name] = value
        elif self.persistence.replace_value:
            row[self.persistence.persist_name] = value
        else:
            raise ValueError(
                f'Persist name {self.persistence.persist_name} already exists and is marked as not replaceable')

    def __persist(self, row: pd.Series, value: any, memory: dict) -> None:
        if self.persistence:
            match self.persistence.persist_type:
                case PersistenceType.MEMORY:
                    self.__persist_memory(row, value, memory)
                case PersistenceType.ROW:
                    self.__persist_row(row, value)
                case _:
                    raise ValueError(f'Invalid persistence type {self.persistence.persist_type}')

    def _process(self, row: pd.Series, memory: dict) -> any:
        pass

    def process(self, row: pd.Series, memory: dict) -> pd.Series:
        value = self._process(row, memory)
        self.__persist(row, value, memory)
        return row


class FileConfiguration(BaseModel):
    row_operations: list[RowOperation]

    def proces_row(self, row: pd.Series, memory: dict) -> pd.Series:
        for operation in self.row_operations:
            operation.process(row, memory)
        return row
