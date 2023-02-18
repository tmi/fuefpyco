"""
Module contents:
 - Monoid: a Protocol representing "things you can sum together". Useful for running concurrent computation and then
   merging results together. Accompandied with `msum` function.
 - MaybeResult and Failure: useful for computations where you don't want the first exception to crash everything down,
   but rather finish what can be finished and collect all exceptions at the end.
"""

from abc import abstractmethod
from dataclasses import dataclass, replace
from typing import Generic, Iterable, Optional, Protocol, Type, TypeVar, runtime_checkable

from typing_extensions import Self


# *** Monoid ***
@runtime_checkable
class Monoid(Protocol):
    """This simplifies applying map-reduce style processing logic. Imagine you have a list of urls of csvs you want
    to download and concat as a pandas. Or a list of dictionaries, each of which has some field 'k' you want to sum
    together. You can do that with for cycles, comprehensions, functools.reduce, ... Or, you can just define
    a dataclass that represents the result of a single computation, and how two computations can be put together
    (for the examples above, `pd.concat`, `+`), to satisfy the Monoid protocol, and then the `msum` function does
    the rest for you."""

    @abstractmethod
    def __add__(self, other: Self) -> Self:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def empty(cls) -> Self:
        raise NotImplementedError


# NOTE it is rather unfortunate that the following method needs the Type explicitly -- because Python has, afaik,
# no proper template/macro capability to infer that information from the context. Also, we can't just turn it
# into a classmethod, if we want Monoid to stay a protocol and not become an ABC.
TMonoid = TypeVar("TMonoid", bound=Monoid)


def msum(i: Iterable[TMonoid], t: Type[TMonoid]) -> TMonoid:
    return sum(i, start=t.empty())


@dataclass
class Failure:
    """Represents a caught Exception. User fills `origin` based on context -- it can be stack trace, params,
    identifier, ... To be used in concurrent compute, when single Exception should not bring it all down."""

    origin: str
    exception: Exception


@dataclass
class MaybeResult(Generic[TMonoid]):
    result: Optional[TMonoid]
    failure: list[Failure]

    @classmethod
    def empty(cls) -> Self:
        return cls(result=None, failure=[])

    def __add__(self, other: Self) -> Self:
        # annoyance due to python Optional not being well combinable. TODO improve
        if self.result is None and other.result is None:
            result = None
        elif self.result is None:
            result = other.result
        elif other.result is None:
            result = self.result
        else:
            result = self.result + other.result
        return replace(self, result=result, failure=self.failure + other.failure)
