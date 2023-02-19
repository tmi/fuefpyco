# Functional Efficient Python Computations

This project provides utilities for making computations (data transformations) using functional interfaces.
Efficiency is both in resource consumption---no additional memory allocations, reliance on vanilla iterators, etc---and cognitive load -- we don't introduce any new syntax or metaprogramming, we stick to core python.
We have a full typing annotations coverage, though at times Python limitation itself manifest itself in not being unable to guarantee correctness.

## Subpackages
* [ds](src/fuefpyco/ds) contains useful dataclasses or contracts such as Monoid or Either,
* [it](src/fuefpyco/it) provides iterator/iterable oriented functionality such as flatmaps, monoid sums or windowing,
* [pa](src/fuefpyco/pa) boasts with parallel processing interfaces, alternative to that of standard python's `multiprocessing` or `concurrent`.

Individual functions are documented in the docstring of the module, with illustrative examples located in [tests](tests).

All public members of the subpackages are accessible from the main package -- users are encouraged to import them from there to protect from future refactoring moves within subpackages.
