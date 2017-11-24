""" The executor is used to concurrently dispatch requests to RPC services """

from concurrent.futures.thread import ThreadPoolExecutor

__executor = None

executor_options = {}


def set_executor_options(max_workers, thread_name_prefix):
    executor_options.update(dict(
        max_workers=max_workers,
        thread_name_prefix=thread_name_prefix
    ))


def get_executor():
    global __executor
    if not __executor:
        __executor = ThreadPoolExecutor(**executor_options)
    return __executor
