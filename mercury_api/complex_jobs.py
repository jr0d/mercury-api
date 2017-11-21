""" Complex job injection, storage, and aggregation

A "complex job" is a job that targets devices which originated from two or more
backends. Such jobs would actually require injection to multiple RPC services,
which would generate multiple JobID's. In order to simplify the caller
interface, the API service will aggregate complex jobs by transparently
returning a aggregate job_id.

Why do we need this?

Mercury is designed to operate against devices in multiple network segments,
zones, and datacenter regions. Thus, a single interaction with the jobs API
should be able to target a single node, every node in a datacenter, or every
node in every datacenter. The caller should not be concerned about what backend
system is responsible for dispatching messages, so this is abstracted away.

Future iterations of mercury will feature a tighter coupling of backend devices
to the central mercury services ( right now this is the inventory and the API
services ). Meaning, back end systems will register explicitly with the central
service, rather than relying on the origin inventory field to discover back end
systems.
"""

import logging
import json
import redis

log = logging.getLogger(__name__)

JOB_ID_PREFIX = 'cplxjob-'

__redis_client = None
redis_client_info = {}


def set_redis_info(host, port, db):
    """ Load the connection details at runtime

    :param host: Redis host
    :param port: Redis port
    :param db: redis database
    """
    redis_client_info.update({'host': host, 'port': port, 'db': db})


def get_redis_client():
    """ Lazy loader for __redis_client """
    global __redis_client
    if not __redis_client:
        try:
            __redis_client = redis.Redis(redis_client_info['user'],
                                         redis_client_info['port'],
                                         redis_client_info['db'])
        except KeyError:
            raise RuntimeError('redis_client_info must be populated')
    return __redis_client


def store_complex_job(job_id, backend_targets, ttl):
    """ Store job_id and backend relationship

    :param job_id: The job_id we'll be using
    :param backend_targets: Targets that the job will be created on
    :param ttl: TTL for record
    :type backend_targets: list
    :return: bool
    """
    return get_redis_client().setex(JOB_ID_PREFIX + job_id,
                                    json.dumps(backend_targets), ttl)


def get_complex_job(job_id):
    """ Get backend servers associated with job
    :param job_id: The job_id we are searching for
    :return: list of backend or an empty list
    """
    response = get_redis_client().get(JOB_ID_PREFIX + job_id)

    return response and json.loads(response) or []
