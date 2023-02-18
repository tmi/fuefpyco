from dataclasses import dataclass, replace

from typing_extensions import Self

from fuefpyco.ds import Failure, MaybeResult, msum


@dataclass
class AMonoid:
    a: int

    def __add__(self, other: Self) -> Self:
        return replace(self, a=self.a + other.a)

    @classmethod
    def empty(cls) -> Self:
        return cls(0)


def test_monoid() -> None:
    l1 = [AMonoid(4), AMonoid(5)]
    l2: list[AMonoid] = []
    assert msum(l1, AMonoid) == AMonoid(a=9)
    assert msum(l2, AMonoid) == AMonoid(a=0)


def test_maybe_result() -> None:
    succ1 = MaybeResult(result=AMonoid(a=4), failure=[])
    fail1 = MaybeResult(result=AMonoid.empty(), failure=[Failure(origin="a", exception=ValueError())])
    succ2 = MaybeResult(result=AMonoid(a=5), failure=[])
    fail2 = MaybeResult(result=AMonoid.empty(), failure=[Failure(origin="b", exception=ValueError())])
    all_r = msum([succ1, fail1, succ2, fail2], MaybeResult)
    assert all_r.result == AMonoid(a=9)
    assert all_r.failure == [fail1.failure[0], fail2.failure[0]]
