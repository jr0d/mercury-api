""" Complex job injection, storage, and aggregation

A "complex job" is a job that targets devices which originated from two or more
backends. Such jobs would actually require injection to multiple RPC services,
which would generate multiple JobID's. In order to simplify the caller
interface, the API service will aggregate complex jobs by transparently
returning a aggregate job_id on complex job injections.

Why do we need this?

Mercury is designed to operate against devices in multiple network segments,
zones, and datacenter regions. Thus, a single interaction with the jobs API
should be able to target a single node, every node in a datacenter, or every
node in every datacenter without being concerned about what backend system is
responsible for dispatching messages to each target device.
"""

import logging
import json
import redis

log = logging.getLogger(__name__)

JOB_ID_PREFIX = 'cplxjob-'


def load_connection(host, port, db):
    """
    Load the connection once at runtime
    :param host:
    :param port:
    :param db:
    :return:
    """
    global redis_client
    redis_client = redis.Redis(host, port, db)


def check_connection(f):
    def wrap(*args, **kwargs):
        if not 'redis_client' in globals():
            raise RuntimeError('Please call load_connection first')
        return f(*args, **kwargs)
    wrap.__name__ = f.__name__
    wrap.__doc__ = f.__doc__
    return wrap


@check_connection
def store_complex_job(job_id, backends):
    """

    :param job_id: The job_id we are dispatching to each backend
    :param backends:
    :return:
    """


if __name__ == '__main__':
    load_connection('localhost', '6397', 0)
    store_complex_job('', '')
