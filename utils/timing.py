import wrapt
import time
from contextlib import contextmanager
from dataclasses import dataclass
from logging import Logger

logger = Logger(name="timming")


@dataclass
class Config:
    debug = False
    print_callback = logger.info


config = Config()


@wrapt.decorator
def timeit_ns(wrapped, instance, args, kwargs):
    start_time = time.time_ns()
    result = wrapped(*args, **kwargs)
    end_time = time.time_ns()
    run_time = (end_time - start_time) / 1e6
    name = getattr(wrapped, '__name__', repr(wrapped))
    if config.debug: config.print_callback(f"[timeit_ns] {name}:: {run_time:,.3f} msec :: {args=} {kwargs=} {result=}")
    return result


@wrapt.decorator
async def timeit_ns_async(wrapped, instance, args, kwargs):
    start_time = time.time_ns()
    result = await wrapped(*args, **kwargs)
    end_time = time.time_ns()
    run_time = (end_time - start_time) / 1e6
    name = getattr(wrapped, '__name__', repr(wrapped))
    if config.debug: config.print_callback(f"[timeit_ns_async] {name}:: {run_time:,.3f} msec :: {args=} {kwargs=} {result=}")
    return result


@wrapt.decorator
def timeit_perf_ns(wrapped, instance, args, kwargs):
    start_time = time.perf_counter_ns()
    result = wrapped(*args, **kwargs)
    end_time = time.perf_counter_ns()
    run_time = (end_time - start_time) / 1e6
    name = getattr(wrapped, '__name__', repr(wrapped))
    config.print_callback(f"[timeit_perf_ns] {name}:: {run_time:,.3f} msec")
    return result


@contextmanager
def context_time_ns():
    start_time = time.perf_counter_ns()
    duration = None

    def get_duration():
        if duration is None:
            raise RuntimeError("Duration is not available yet.")
        return duration

    yield get_duration  # Provide the callback to the user

    end_time = time.perf_counter_ns()
    duration = (end_time - start_time) / 1e6
