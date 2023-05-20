"""
This module provides parallel execution to map-reduce computations: single function to be applied on a sequence
of arguments, and the result being a monoid sum. The primary advantages over existing python high level interfaces are:
 - timeouts on the process level -- even when the GIL is held by the computation, you can still time it out
 - argument check by mypy. We enforce the function to be executed takes a single argument, typically a dataclass
   of the actual parameters. This restriction is due to **kwargs being quite hard to check.
 - MaybeResult-based exception propagation and process-level retries -- we don't want a single crash to bring the whole
   computation down. Note that if the function itself already returns MaybeResult, those get nested.

There are multiple implementations, each with its own vices and virtues. In particular:
 - ProcessPerTask -- launches a new process for every task. Gives the most timeout-retry control, slowest, memory-safe.
 - ProcessPoolExecutor -- submits tasks to a process pool executor. Less control on timeouts, but faster.

To use, instantiate the respective class, and feed it to the `mapreduce` function along with your `f` (the "map") and
inputs (an iterable). The "reduce" part is given via the return value being a Monoid, with the summing happening
usually in the main process.

Additionally, both input and output class in the mapreduce must be serializable, as well as the `f` itself.
"""

from fuefpyco.pa.core import mapreduce  # noqa: F401
from fuefpyco.pa.process_per_task import Config as PPTConfig  # noqa: F401
from fuefpyco.pa.process_per_task import ProcessPerTask  # noqa: F401
from fuefpyco.pa.process_pool_executor import Config as PPEConfig  # noqa: F401
from fuefpyco.pa.process_pool_executor import ProcessPoolExecutor  # noqa: F401
