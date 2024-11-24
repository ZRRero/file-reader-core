from __future__ import annotations

from enum import StrEnum

import pandas as pd
from pydantic import BaseModel

from file_core.configuration.base_configuration import RowOperation
from file_core.configuration.compare_configuration import CompareFieldsOperation


class LogicalOperand(BaseModel):
    constant_value: bool = None
    field_value: str = None
    operation_value: CompareFieldsOperation | LogicalFieldsOperation = None
    memory_value: str = None

    def get_value(self, row: pd.Series, memory: dict) -> bool:
        if self.constant_value:
            return self.constant_value
        if self.field_value:
            return row[self.field_value]
        if self.operation_value:
            return self.operation_value.__process(row, memory)
        if self.memory_value:
            return memory[self.memory_value]
        else:
            raise ValueError('No value provided for LogicalOperand')


class LogicalOperator(StrEnum):
    AND = 'and'
    OR = 'or'
    NOT = 'not'


class LogicalFieldsOperation(RowOperation):
    first_operand: LogicalOperand
    second_operand: LogicalOperand = None
    operator: LogicalOperator

    def __process(self, row: pd.Series, memory: dict) -> bool:
        first_value = self.first_operand.get_value(row, memory)
        second_value = self.second_operand.get_value(row, memory)
        match self.operator:
            case LogicalOperator.AND:
                return first_value and second_value
            case LogicalOperator.OR:
                return first_value or second_value
            case LogicalOperator.NOT:
                return not first_value
            case _:
                raise ValueError(f'Invalid operator: {self.operator}')
