import logging
from dataclasses import dataclass, replace

from typing_extensions import Self

from fuefpyco.ds import Failure, msum
from fuefpyco.pa import PPEConfig, PPTConfig, ProcessPerTask, ProcessPoolExecutor, mapreduce


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

    input_seq = [1, 2, 3]
    expected = msum((simple_f(e) for e in input_seq), AMonoid)

    # succ
    ppt = ProcessPerTask(PPTConfig(parallelism=4, task_timeout_s=1))
    ppt_result = mapreduce(simple_f, input_seq, ppt)
    assert ppt_result.result == expected
    assert ppt_result.failure == []

    ppe = ProcessPoolExecutor(PPEConfig(parallelism=4))
    ppe_result = mapreduce(simple_f, input_seq, ppe)
    assert ppe_result.result == expected
    assert ppe_result.failure == []

    # succ + fail
    seq_with_error = [1, 2, 10]
    expected_succ = msum((simple_f(e) for e in seq_with_error[:2]), AMonoid)
    expected_fail = [Failure("failure with args 10", _error)]

    ppt_result = mapreduce(simple_f, seq_with_error, ppt)
    assert ppt_result.result == expected_succ
    assert ppt_result.failure == expected_fail

    ppe_result = mapreduce(simple_f, seq_with_error, ppt)
    assert ppe_result.result == expected_succ
    assert ppe_result.failure == expected_fail
