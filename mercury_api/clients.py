# Copyright 2017 Jared Rodriguez (jared.rodriguez@rackspace.com)
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
""" Process global storage for socket base clients """

import redis

from mercury.common.clients.inventory import InventoryClient
from mercury.common.clients.rpc.frontend import RPCFrontEndClient

__inventory_client = None
__redis_client = None

rpc_client_cache = {}


def create_inventory_client(inventory_zurl):
    """
    Creates an inventory client. Call this once perprocess
    :param inventory_zurl: The inventory routers 0mq URL
    """
    global __inventory_client
    __inventory_client = InventoryClient(inventory_zurl)


def create_redis_client(host, port, db):
    """
    Creates a redis client. Call this once per process

    :param host: Redis host
    :param port: Redis port
    :param db: redis database
    """
    # TODO: Support more redis options
    global __redis_client
    __redis_client = redis.Redis(host, port, db)


def get_inventory_client():
    """ Get the inventory client """

    if not __inventory_client:
        raise RuntimeError('Please run create_inventory_client')
    return __inventory_client


def get_redis_client():
    """ Gets the redis client """

    if not __redis_client:
        raise RuntimeError('Please run create_redis_client')
    return __redis_client


def get_rpc_client(router_zurl):
    """
    getter for RPC clients, support for multiple back ends. Connections will be
    cached for the life of the process.
    :param router_zurl: The 0mq URL. Usually discovered from the inventory's
            origin field.
    :return: A new or cached RPCFrontendClient
    """
    client = rpc_client_cache.get(router_zurl)
    if not client:
        client = RPCFrontEndClient(router_zurl)
        rpc_client_cache[router_zurl] = client
    return client
