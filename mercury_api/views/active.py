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

from flask import request, jsonify

from mercury_api.views.base import BaseMethodView
from mercury_api.exceptions import HTTPError
from mercury_api.decorators import validate_json, check_query


class ActiveComputerView(BaseMethodView):
    """
    Active computer API method view
    """

    def get(self, mercury_id=None):
        """
        Query the inventory client for active records with a given projection
        or get one by mercury_id.

        :param mercury_id: Inventory object mercury id, default is None.
        :return: List of inventory objects or a single inventory object.
        """
        projection = self.get_projection_from_qsa()

        if mercury_id is None:
            projection = self.get_projection_from_qsa()
            limit, sort_direction = self.get_limit_and_sort_direction()
            if not projection:
                projection = {'mercury_id': 1}

            data = self.inventory_client.query(
                {
                    'active': {
                        '$ne': None
                    }
                },
                projection=projection,
                limit=limit,
                sort_direction=sort_direction)
        else:

            if not projection:
                projection = {'active': 1, 'mercury_id': 1}
            data = self.inventory_client.get_one(
                {
                    'mercury_id': mercury_id,
                    'active': {
                        '$ne': None
                    }
                },
                projection=projection)
            if not data:
                message = 'mercury_id {} does not exist in inventory'
                raise HTTPError(message.format(mercury_id), status_code=404)

        return jsonify(data)


class ActiveComputerQueryView(BaseMethodView):
    """ Active computer API query view """

    decorators = [check_query, validate_json]

    def post(self):
        """
        Query the active inventory with a given projection.

        :return: List of inventory objects.
        """
        query = request.json.get('query')
        # Make sure we get only active devices
        query.update({'active': {'$ne': None}})
        projection = self.get_projection_from_qsa()
        limit, sort_direction = self.get_limit_and_sort_direction()
        data = self.inventory_client.query(
            query,
            projection=projection,
            limit=limit,
            sort_direction=sort_direction)
        return jsonify(data)
