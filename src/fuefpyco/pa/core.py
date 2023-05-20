from typing import Callable, Iterable, Protocol, TypeVar, runtime_checkable

from fuefpyco.ds import MaybeResult, TMonoid

T = TypeVar("T")


@runtime_checkable
class ComputationFactory(Protocol):
    """We bundle config and a factory to build the respective computation engine. This protocol handles the factory
    part."""

    def mapreduce(self, f: Callable[[T], TMonoid], s: Iterable[T]) -> MaybeResult[TMonoid]:
        raise NotImplementedError


def mapreduce(f: Callable[[T], TMonoid], s: Iterable[T], c: ComputationFactory) -> MaybeResult[TMonoid]:
    return c.mapreduce(f, s)
