from __future__ import annotations

from enum import StrEnum

import pandas as pd
from pydantic import BaseModel

from file_core.configuration.base_configuration import RowOperation


class StringOperand(BaseModel):
    constant_value: str = None
    field_value: str = None
    operation_value: StringFieldsOperation = None
    memory_value: str = None

    def get_value(self, row: pd.Series, memory: dict) -> str:
        if self.constant_value:
            return self.constant_value
        if self.field_value:
            return row[self.field_value]
        if self.operation_value:
            return self.operation_value.__process(row, memory)
        if self.memory_value:
            return memory[self.memory_value]
        else:
            raise ValueError('No value provided for StringOperand')


class StringOperator(StrEnum):
    CONCAT = 'concat'
    TO_LOWER = 'to_lower'
    TO_UPPER = 'to_upper'
    TRIM = 'trim'


class StringFieldsOperation(RowOperation):
    first_operand: StringOperand
    second_operand: StringOperand = None
    operator: StringOperator

    def _process(self, row: pd.Series, memory: dict) -> str:
        first_value = self.first_operand.get_value(row, memory)
        second_value = self.second_operand.get_value(row, memory)
        match self.operator:
            case StringOperator.CONCAT:
                return first_value + second_value
            case StringOperator.TO_LOWER:
                return first_value.lower()
            case StringOperator.TO_UPPER:
                return first_value.upper()
            case StringOperator.TRIM:
                return first_value.strip()
            case _:
                raise ValueError(f'Invalid operator: {self.operator}')
