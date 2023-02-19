import logging
from dataclasses import dataclass, replace

from typing_extensions import Self

from fuefpyco.ds import Failure, msum
from fuefpyco.pa import ComputationConfig, mapreduce


@dataclass
class AMonoid:
    a: int

    def __add__(self, other: Self) -> Self:
        return replace(self, a=self.a + other.a)

    @classmethod
    def empty(cls) -> Self:
        return cls(0)


_error = ValueError("thou shalt not pass more than 9")


def simple_f(a: int):
    if a > 9:
        raise _error
    return AMonoid(a=a * 2)


def test_mapreduce_happy():
    logging.basicConfig(level="DEBUG", force=True)
    config = ComputationConfig(parallelism=4, single_task_timeout_s=1)
    input_seq = [1, 2, 3]
    result = mapreduce(simple_f, input_seq, config)
    assert result.result == msum((simple_f(e) for e in input_seq), AMonoid)
    assert result.failure == []

    seq_with_error = [1, 2, 10]
    result = mapreduce(simple_f, seq_with_error, config)
    assert result.result == msum((simple_f(e) for e in seq_with_error[:2]), AMonoid)
    assert result.failure == [Failure("failure with args 10", _error)]
