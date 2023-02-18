"""
Module contents:
    - flatmap -- standard functional construct, useful for e.g. map-and-filter or maps that produce Optional results,
    - fold_transform -- simpler application of a pipeline of transformations on a single object,
    - unzip -- just a name for the inverse zip operation,
    - consume -- return first n elements of iterator, and then the rest of the iterator. Generalisation of head-tail,
    - windows -- `windows([1,2,3,4,5], 2) -> [[1, 2], [3,4], [5]].`

This whole module is based on iterators/generators, no unneeded list allocations are happening

"""
from functools import reduce
from itertools import chain
from typing import Callable, Iterable, Iterator, Optional, TypeVar

TA = TypeVar("TA")
TB = TypeVar("TB")


def flatmap(f: Callable[[TA], Iterable[TB]], xs: Iterable[TA]) -> Iterable[TB]:
    """Often one wants to map-and-filter a sequence, which perfectly suits the flatMap."""
    return (y for x in xs for y in f(x))


def fold_transform(obj: TA, pipeline: Iterable[Callable[[TA], TA]]) -> TA:
    """Say you have a dataframe and want to apply a chain of transformations on it"""
    return reduce(lambda obj, func: func(obj), pipeline, obj)


def unzip(zipped: Iterable[tuple[TA, TB]]) -> tuple[Iterable[TA], Iterable[TB]]:
    """Inverse operation to zip: [(a, b), (c, d), (e, f)] => ((a, c, e), (b, d, f))"""
    # NOTE this is basically just a type-anotated more-readable alias
    a, b = zip(*zipped)
    return a, b


def consume(it: Iterator[TA], n: Optional[int]) -> tuple[list[TA], Iterator[TA]]:
    """Retuns up to first `n` elements of an iterator, and the remaining iterator part. Generalises head-tail."""
    # TODO return iterator instead of list?
    ls = []
    i = 0
    if n is None:
        return list(it), iter(())
    while i < n:
        try:
            ls.append(next(it))
            i += 1
        except StopIteration:
            break
    return ls, it


def windows(s: Iterable[TA], n: int) -> Iterable[Iterable[TA]]:
    """windows([1,2,3,4,5], 2) -> [[1, 2], [3,4], [5]].
    Beware that this shares a similar quirk as groupby -- don't advance the outer iterator until the inner iterator
    is wholly consumed. In other words:
    ```
    for window in windows(s, n):
        for element in window:
            work(element)
    ```
    is fine, but list(windows(s, n)) is not because it does not consume individual elements."""
    it = iter(s)

    def _take_n(it: Iterator[TA], n: int) -> Iterable[TA]:
        while n > 0:
            try:
                yield next(it)
            except StopIteration:
                break
            n = n - 1

    while True:
        try:
            head_w = _take_n(it, n).__iter__()
            head_w_head = next(head_w)
            yield chain([head_w_head], head_w)
        except StopIteration:
            break
