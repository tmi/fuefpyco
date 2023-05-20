"""
Implements MapReduce via ProcessPoolExecutor's submit.

Features:
 - you can control the size of the pool (and thus the parallelism),
 - you can control max_tasks_per_child (how many tasks a process solves until recycled),
   - actually not supported until Python 3.11
 - you can specify retry count per task,
 - you can specify *total* timeout -- but not timeout per task.

This is somehow faster than ProcessPerTask (unless you set max_tasks_per_child=1, in which case they become the ~same)
thanks to less processes being started in total. You pay for that by being punished by possible memory leaks, as well
as inability to control timeout for individual task execution.

To use, instantiate the dataclass ProcessPoolExecutor, with the config field containing all the tweakable behaviour, and
pass to the core.mapreduce method.
"""

import multiprocessing as mp
import time
from concurrent.futures import FIRST_EXCEPTION, Future
from concurrent.futures import ProcessPoolExecutor as PPE
from concurrent.futures import wait
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, TypeVar

from fuefpyco.ds import Failure, MaybeResult, TMonoid


@dataclass
class Config:
    parallelism: int
    max_tasks_per_child: Optional[int] = None  # None for unlimited
    task_retries: int = 0
    total_timeout_s: Optional[int] = None  # None for unlimited

    mp_context: str = "forkserver"


T = TypeVar("T")


def _mapreduce(f: Callable[[T], TMonoid], s: Iterable[T], c: Config) -> MaybeResult[TMonoid]:
    start_time = time.monotonic_ns() // 1e9
    tasks: dict[Future, tuple[T, list[Failure]]] = {}
    result: MaybeResult[TMonoid] = MaybeResult.empty()
    running = 0
    # TODO after python 3.11, enable this in the PPE call:
    # max_tasks_per_child = c.max_tasks_per_child
    if c.max_tasks_per_child:
        raise ValueError("max_tasks_per_child supported only after python 3.11")
    ctx = mp.get_context(c.mp_context)
    with PPE(max_workers=c.parallelism, mp_context=ctx) as ppe:
        for t in s:
            future: Future = ppe.submit(f, t)
            tasks[future] = (
                t,
                [],
            )
            running += 1

        while running > 0:
            if c.total_timeout_s:
                elapsed_s = (time.monotonic_ns() // 1e9) - start_time
                timeout = c.total_timeout_s - elapsed_s
                if timeout <= 0:
                    break
            else:
                timeout = None
            # Any because wait is probably not properly annotated
            intermediate: Any = wait(tasks.keys(), timeout=timeout, return_when=FIRST_EXCEPTION)
            for e in intermediate.done:
                if e.exception():
                    previous = tasks.pop(e)
                    if len(previous[1]) < c.task_retries:
                        future = ppe.submit(f, previous[0])
                        tasks[future] = (
                            previous[0],
                            previous[1] + [Failure(f"failure of arg {previous[0]}", e.exception())],
                        )
                else:
                    previous = tasks.pop(e)
                    result = result + MaybeResult(e.result(), previous[1])
                    running -= 1

        if running > 0:
            raise TimeoutError("total timeout elapsed yet some tasks are still running")

    return result


@dataclass
class ProcessPoolExecutor:
    config: Config

    def mapreduce(self, f: Callable[[T], TMonoid], s: Iterable[T]) -> MaybeResult[TMonoid]:
        return _mapreduce(f, s, self.config)
