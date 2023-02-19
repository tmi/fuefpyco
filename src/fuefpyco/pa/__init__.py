"""
This module provides parallel execution to map-reduce computations: single function to be applied on a sequence
of arguments, and the result being a monoid sum. The primary advantages over existing python high level interfaces are:
 - timeouts on the process level -- even when the GIL is held by the computation, you can still time it out
 - argument check by mypy. We enforce the function to be executed takes a single argument, typically a dataclass
   of the actual parameters. This restriction is due to **kwargs being quite hard to check.
 - MaybeResult-based exception propagation and process-level retries -- we don't want a single crash to bring the whole
   computation down. Note that if the function itself already returns MaybeResult, those get nested.

Notes:
  - Retry applies in the case of timeout or any exception thrown by the function. A case of non-zero exit of
    the process with _any_ result returned is considered a success, and the non-zero exit code is provided
    in a message in the `failure.origin` field.
  - In case of a retry, the failure reason(s) are returned as well, with the attempt counter prepended to
    the `failure.origin` field.
  - We use mp.Queue to retrieve the results from local workers. In case of huge data volumes, this may bring some
    troubles -- you may want to write disks instead, with the ipc being the file names only.
"""

import logging
import time
from dataclasses import dataclass
from multiprocessing import Process, Queue, get_context
from queue import Empty as EmptyQueueException
from typing import Callable, Generic, Iterable, Iterator, Literal, Optional, TypeVar

from fuefpyco.ds import Failure, MaybeResult, TMonoid

logger = logging.getLogger(__name__)

_process_join_grace = 3  # number of seconds we wait to join processes that returned. 0 should suffice


@dataclass
class ComputationConfig:
    parallelism: int
    single_task_timeout_s: int
    retry: int = 0
    implementation: Literal["process"] = "process"  # TODO add processes with reusal


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


def mapreduce(f: Callable[[T], TMonoid], s: Iterable[T], c: ComputationConfig) -> MaybeResult[TMonoid]:
    if c.retry > 0:
        raise NotImplementedError("non-zero retries are not implemented yet")
    result: MaybeResult[TMonoid] = MaybeResult.empty()
    ctx = get_context("forkserver")
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
        next_timeout = oldest_submit + c.single_task_timeout_s
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
