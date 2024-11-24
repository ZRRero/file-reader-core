from __future__ import annotations

from enum import StrEnum

import pandas as pd
from pydantic import BaseModel

from file_core.configuration.base_configuration import RowOperation


class MathOperand(BaseModel):
    constant_value: float = None
    field_value: str = None
    operation_value: MathFieldsOperation = None
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
            raise ValueError('No value provided for MathOperand')


class MathOperator(StrEnum):
    ADD = 'add'
    SUB = 'sub'
    MUL = 'mul'
    DIV = 'div'


class MathFieldsOperation(RowOperation):
    first_operand: MathOperand
    second_operand: MathOperand
    operation: MathOperator

    def _process(self, row: pd.Series, memory: dict) -> float:
        first_value = self.first_operand.get_value(row, memory)
        second_value = self.second_operand.get_value(row, memory)
        match self.operation:
            case MathOperator.ADD:
                result = first_value + second_value
            case MathOperator.SUB:
                result = first_value - second_value
            case MathOperator.MUL:
                result = first_value * second_value
            case MathOperator.DIV:
                result = first_value / second_value
            case _:
                raise ValueError('Invalid operation')
        return result
