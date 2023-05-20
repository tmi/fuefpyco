"""
Implements MapReduce via vanilla processes. Every task is processed by exactly one process.
Parallelism is by starting `n` processes at the start and giving each its own task, waiting for any to finish and
starting new ones to keep the number of running `n`.

Features:
 - timeout of a task (by killing the respective processes),
 - retry of a task (by re-submitting a new process),
   - applies in the case of timeout or any exception thrown by the function. A case of non-zero exit of
     the process with _any_ result returned is considered a success, and the non-zero exit code is provided
     in a message in the `failure.origin` field,
   - in case of a retry, the failure reason(s) are returned as well, with the attempt counter prepended to
     the `failure.origin` field.
 - low memory consumption (no risk of memory leak across multiple tasks),
 - slower throughput (has to start multiple processes),
 - mp.Queue is used to retrieve the results from local workers. In case of huge data volumes, this may bring some
   troubles -- you may want to write disks instead, with the ipc being the file names only.

To use, instantiate the dataclass ProcessPerTask, with the config field containing all the tweakable behaviour, and
pass to the core.mapreduce method.
"""

# TODO instead of full timeout, wait just on the queue
# TODO retries

import logging
import time
from dataclasses import dataclass
from multiprocessing import Process, Queue, get_context
from queue import Empty as EmptyQueueException
from typing import Callable, Generic, Iterable, Iterator, Optional, TypeVar

from fuefpyco.ds import Failure, MaybeResult, TMonoid

logger = logging.getLogger(__name__)

_process_join_grace = 3  # number of seconds we wait to join processes that returned. 0 should suffice


@dataclass
class Config:
    parallelism: int
    task_timeout_s: int
    task_retries: int = 0

    mp_context: str = "forkserver"


T = TypeVar("T")


@dataclass
class _IntermediateResult(Generic[TMonoid]):
    result: MaybeResult[TMonoid]
    p_id: int


@dataclass
class _ProgressTracker(Generic[T]):
    p: Process
    arg: T
    submit_time_s: int


def _entrypoint(f: Callable[[T], TMonoid], arg: T, q: Queue, p_id: int):
    try:
        result = MaybeResult(f(arg), [])
    except Exception as e:
        result = MaybeResult(None, [Failure(f"failure with args {arg}", e)])
    q.put(_IntermediateResult(result=result, p_id=p_id))


def process_per_task_mapreduce(f: Callable[[T], TMonoid], s: Iterable[T], c: Config) -> MaybeResult[TMonoid]:
    if c.task_retries > 0:
        raise NotImplementedError("non-zero retries are not implemented yet")
    result: MaybeResult[TMonoid] = MaybeResult.empty()
    ctx = get_context(c.mp_context)
    queue = ctx.Queue()

    running: dict[int, _ProgressTracker] = {}
    capacity: Callable[[], int] = lambda: c.parallelism - len(running)
    it: Optional[Iterator[T]] = iter(s)
    p_id = 0

    while True:
        # maybe submit new process
        if capacity() > 0 and it is not None:
            try:
                arg = next(it)
            except StopIteration:
                it = None
                continue
            p = Process(target=_entrypoint, args=(f, arg, queue, p_id))
            tracker = _ProgressTracker(p, arg, int(time.monotonic_ns() // 1e9))
            running[p_id] = tracker
            p.start()
            logging.debug(f"submitted a process #{p_id} with pid p.pid")
            p_id += 1
            continue
        # maybe quit
        if capacity() == c.parallelism:
            if running:
                raise ValueError(f"internal error: expected no processes running, but {len(running)} entries remain")
            logger.debug("all processes completed, breaking main loop")
            break
        # wait for next result
        oldest_submit = min(e.submit_time_s for e in running.values())
        next_timeout = oldest_submit + c.task_timeout_s
        current_time = time.monotonic_ns() // 1e9
        if next_timeout > current_time:
            try:
                wait_time = next_timeout - current_time
                logger.debug(f"about to wait for {wait_time} seconds for a result")
                intermediate = queue.get(timeout=wait_time)
                tracker = running.pop(intermediate.p_id)
                tracker.p.join(_process_join_grace)
                exitcode = tracker.p.exitcode
                if exitcode is None:
                    raise ValueError(f"process {tracker.p.pid} with arg {tracker.arg} failed to terminate")
                if exitcode != 0:
                    nonzero: MaybeResult[TMonoid] = MaybeResult(
                        None, [Failure(f"non-zero exit code {exitcode} with arg {tracker.arg}", Exception())]
                    )
                    intermediate.result = intermediate.result + nonzero
                result = result + intermediate.result
                logger.debug(f"a result for {intermediate.p_id} processed")
                continue
            except EmptyQueueException:
                logger.debug("wait unsuccessful")
                pass
        # kill
        for p_id in running:
            logger.debug(f"inspecting process #{p_id}")
            if running[p_id].submit_time_s < next_timeout:
                convict = running.pop(p_id)
                sentence: MaybeResult[TMonoid] = MaybeResult(
                    None, [Failure(f"timed out with arg {convict.arg}", Exception())]
                )
                result = result + sentence
                logger.debug(f"killing process #{p_id} with pid {convict.p.pid}")
                convict.p.kill()
                break
    return result


@dataclass
class ProcessPerTask:
    config: Config

    def mapreduce(self, f: Callable[[T], TMonoid], s: Iterable[T]) -> MaybeResult[TMonoid]:
        return process_per_task_mapreduce(f, s, self.config)
