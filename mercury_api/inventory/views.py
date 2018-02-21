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

import logging

from flask import request, jsonify, abort, Response

from mercury_api.views import BaseMethodView
from mercury_api.exceptions import HTTPError
from mercury_api.decorators import validate_json, check_query

log = logging.getLogger(__name__)


class ComputerView(BaseMethodView):
    """ Inventory computer API method view """

    def get(self, mercury_id=None):
        """
        Query the inventory client for device records with a given projection
        or get one by mercury_id.

        :param mercury_id: Inventory object mercury id, default is None.
        :return: List of inventory objects or a single inventory object.
        """
        projection = self.get_projection_from_qsa()

        if mercury_id is None:
            projection = self.get_projection_from_qsa()
            limit, sort_direction = self.get_limit_and_sort_direction()
            data = self.inventory_client.query(
                {},
                projection=projection,
                limit=limit,
                sort_direction=sort_direction)
        else:
            data = self.inventory_client.get_one(
                mercury_id, projection=projection)

            if not data:
                message = 'mercury_id {} does not exist in inventory'
                raise HTTPError(message.format(mercury_id), status_code=404)

        return jsonify(data)


class ComputerQueryView(BaseMethodView):
    """ Inventory computer API query view """

    decorators = [check_query, validate_json]

    def post(self):
        """
        Query the inventory with a given set of parameters.
        
        :return: List of inventory objects.
        """
        query = request.json.get('query')
        projection = self.get_projection_from_qsa()
        limit, sort_direction = self.get_limit_and_sort_direction()
        log.debug('QUERY: {}'.format(query))

        data = self.inventory_client.query(
            query,
            projection=projection,
            limit=limit,
            sort_direction=sort_direction)
        return jsonify(data)


class ComputerCountView(BaseMethodView):
    """ Inventory computer count API view """

    decorators = [check_query, validate_json]

    def post(self):
        """
        Return the device count that matches the given projection.
        
        :return: Device count dictionary.
        """
        query = request.json.get('query')
        data = {'count': self.inventory_client.count(query)}
        return jsonify(data)


class ComputerBootStateView(BaseMethodView):
    """ Inventory computer boot API view """

    decorators = [validate_json]

    def post(self, mercury_id):
        # TODO: openapi doc strings???
        """
        Sets the inventory boot state
        :param mercury_id: The mercury_id of the target
        :return:
        """

        state = request.json.get('state', '')

        if state not in ['local', 'rescue']:
            log.error('Invalid state received: {}'.format(state))
            abort(400, 'state must be either local or rescue')

        log.info('Setting boot state to {} for {}'.format(state, mercury_id))
        self.inventory_client.update_one(mercury_id, {
            'boot.state': state
        })

        return jsonify(dict(message='accepted'))

    def delete(self, mercury_id):
        """ Delete the current state """

        log.info('Deleting boot state for {}'.format(mercury_id))
        self.inventory_client.update_one(mercury_id, {
            'boot.state': None
        })

        return jsonify(dict(message='deleted'))
