from enum import StrEnum

import pandas as pd
from pydantic import BaseModel

from file_core.configuration.base_configuration import RowOperation
from file_core.configuration.math_configuration import MathFieldsOperation
from file_core.configuration.string_configuration import StringFieldsOperation

supported_constant_value_types = float | str
supported_operation_value_types = MathFieldsOperation | StringFieldsOperation


class CompareOperand(BaseModel):
    constant_value: supported_constant_value_types = None
    field_value: str = None
    operation_value: supported_operation_value_types = None
    memory_value: str = None

    def get_value(self, row: pd.Series, memory: dict) -> float:
        if self.constant_value:
            return self.constant_value
        if self.field_value:
            return row[self.field_value]
        if self.operation_value:
            return self.operation_value.__process(row, memory)
        if self.memory_value:
            return memory[self.memory_value]
        else:
            raise ValueError('No value provided for CompareOperand')


class CompareOperator(StrEnum):
    LESS_THAN = 'less_than'
    GREATER_THAN = 'greater_than'
    EQUAL_TO = 'equal_to'
    NOT_EQUAL_TO = 'not_equal_to'
    LESS_THAN_OR_EQUAL_TO = 'less_than_or_equal_to'
    GREATER_THAN_OR_EQUAL_TO = 'greater_than_or_equal_to'


class CompareFieldsOperation(RowOperation):
    first_operand: CompareOperand
    second_operand: CompareOperand
    operator: CompareOperator

    def _process(self, row: pd.Series, memory: dict) -> bool:
        first_value = self.first_operand.get_value(row, memory)
        second_value = self.second_operand.get_value(row, memory)
        match self.operator:
            case CompareOperator.LESS_THAN:
                return first_value < second_value
            case CompareOperator.GREATER_THAN:
                return first_value > second_value
            case CompareOperator.EQUAL_TO:
                return first_value == second_value
            case CompareOperator.NOT_EQUAL_TO:
                return first_value != second_value
            case CompareOperator.LESS_THAN_OR_EQUAL_TO:
                return first_value <= second_value
            case CompareOperator.GREATER_THAN_OR_EQUAL_TO:
                return first_value >= second_value
            case _:
                raise ValueError(f'Invalid operator: {self.operator}')
