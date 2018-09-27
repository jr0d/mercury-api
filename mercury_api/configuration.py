# Copyright 2015 Jared Rodriguez (jared.rodriguez@rackspace.com)
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

from mercury.common.configuration import MercuryConfiguration

API_CONFIG_FILE = 'mercury-api.yaml'

# TODO: FIX inconsistent option names


def options(configuration):
    """ A single place to add program options """

    configuration.add_option(
        'api.host',
        default='0.0.0.0',
        help_string='The host address to bind to')

    configuration.add_option(
        'api.port',
        default=9005,
        special_type=int,
        help_string='The host address to bind to')

    configuration.add_option(
        'api.inventory.inventory_router',
        '--api-inventory-router',
        default='tcp://127.0.0.1:9000',
        help_string='The inventory router url')

    configuration.add_option(
        'api.rpc.rpc_router',
        '--api-rpc-router',
        default='tcp://127.0.0.1:9001',
        help_string='The RPC router url')

    configuration.add_option(
        'api.paging.limit',
        default=250,
        special_type=int,
        help_string='The number of return results')

    configuration.add_option(
        'api.paging.offset_id',
        default=None,
        help_string='The paging offset id')

    configuration.add_option(
        'api.paging.sort_direction',
        default=1,
        special_type=int,
        help_string='The paging sort direction, '
        'default is 1 (ascending)')

    configuration.add_option(
        'api.logging.log_file',
        default='mercury-api.log',
        help_string='The log file path')

    configuration.add_option(
        'api.logging.level', default='DEBUG', help_string='The app log level')

    configuration.add_option(
        'api.logging.console_out',
        default=True,
        special_type=bool,
        help_string='Stream log output to the console')


def get_api_configuration():
    api_configuration = MercuryConfiguration('mercury-api', API_CONFIG_FILE)
    options(api_configuration)

    return api_configuration.scan_options()

