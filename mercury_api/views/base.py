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

import bson
import logging

from flask import request
from flask.views import MethodView

from mercury_api.clients import get_inventory_client, get_redis_client
from mercury_api.configuration import get_api_configuration

log = logging.getLogger(__name__)
api_configuration = get_api_configuration()


class BaseMethodView(MethodView):
    """ Base method view to be used in the app views """

    def __init__(self):
        super(BaseMethodView, self).__init__()

        self.inventory_client = get_inventory_client()
        self.redis_client = get_redis_client()

    @staticmethod
    def get_projection_from_qsa():
        """
        Gets the projection from the request url parameters and transforms
        them into a dictionary.

        :return: Dictionary
        """
        projection_keys = request.args.get('projection', '')
        projection = {}
        if projection_keys:
            for k in projection_keys.split(','):
                projection[k] = 1

        return projection or None

    @staticmethod
    def get_paging_info_from_qsa():
        """
        Gets the paging delimiters from the url if any, otherwise return
        the defaults from the api configuration.

        :return: Dictionary
        """
        delimiters = api_configuration.api.paging.to_dict()
        limit = request.args.get('limit')
        offset_id = request.args.get('offset_id')
        sort_direction = request.args.get('sort_direction')

        if limit and limit.isdigit():
            delimiters['limit'] = int(limit)

        if bson.ObjectId.is_valid(offset_id):
            delimiters['offset_id'] = offset_id

        try:
            delimiters['sort_direction'] = int(sort_direction)
        except (TypeError, ValueError):
            # None == TypeError, anything else == ValueError
            pass
        return delimiters

    def get_limit_and_sort_direction(self):
        """
        Get the limit and sort direction from the paging method.

        :return: Tuple
        """
        paging_data = self.get_paging_info_from_qsa()
        limit = paging_data.get('limit')
        sort_direction = paging_data.get('sort_direction')

        return limit, sort_direction
