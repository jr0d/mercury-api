# Copyright 2017 Ruben Quinones (ruben.quinones@rackspace.com)
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

from flask import Blueprint, request, jsonify

from mercury_api.exceptions import HTTPError
from mercury_api.decorators import validate_json, check_query
from mercury_api.views import (
    get_projection_from_qsa,
    get_paging_info_from_qsa,
    inventory_client,
)


active_blueprint = Blueprint('active', __name__)


@active_blueprint.route('/computers', methods=['GET'], strict_slashes=False)
def list_active_computers():
    """
    Query the inventory client for active records with a given projection.

    :return: List of inventory objects.
    """
    projection = get_projection_from_qsa()
    page_info = get_paging_info_from_qsa()
    if not projection:
        projection = {'mercury_id': 1}

    data = inventory_client.query(
        {
            'active': {
                '$ne': None
            }
        },
        projection=projection,
        limit=page_info['limit'],
        sort=page_info['sort'],
        sort_direction=page_info['sort_direction'],
        offset_id=page_info['offset_id']
    )

    return jsonify(data)


@active_blueprint.route('/computers/<blacklist(query):mercury_id>',
                        methods=['GET'], strict_slashes=False)
def get_active_computer(mercury_id):
    """
    Get one active device by mercury_id.

    :param mercury_id: Device mercury id.
    :return: Inventory object.
    """
    projection = get_projection_from_qsa()
    if not projection:
        projection = {'active': 1, 'mercury_id': 1}
    data = inventory_client.get_one(mercury_id,
                                    projection=projection)
    if not data:
        message = 'mercury_id {} does not exist in inventory'
        raise HTTPError(message.format(mercury_id), status_code=404)

    return jsonify(data)


@validate_json
@check_query
@active_blueprint.route('/computers/query',
                        methods=['POST'], strict_slashes=False)
def query_active_computers():
    """
    Query the active inventory with a given projection.

    :return: List of inventory objects.
    """
    query = request.json.get('query')
    # Make sure we get only active devices
    query.update({'active': {'$ne': None}})
    projection = get_projection_from_qsa()
    page_info = get_paging_info_from_qsa()
    data = inventory_client.query(
        query,
        projection=projection,
        limit=page_info['limit'],
        sort=page_info['sort'],
        sort_direction=page_info['sort_direction'],
        offset_id=page_info['offset_id']
    )
    return jsonify(data)
