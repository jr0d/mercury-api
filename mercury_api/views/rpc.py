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
import json

import redis

from flask import request, jsonify

from mercury_api.views.base import BaseMethodView
from mercury_api.clients import get_rpc_client
from mercury_api.exceptions import HTTPError
from mercury_api.decorators import validate_json, check_query


class BaseJobView(BaseMethodView):
    JOB_ID_PREFIX = 'job_id-'

    def __init__(self):
        super(BaseJobView, self).__init__()

    def store_job(self, job_id, backend_targets, ttl):
        """ Store job_id and backend relationship

        :param job_id: The job_id we'll be using
        :param backend_targets: Targets that the job will be created on
        :param ttl: TTL for record
        :type backend_targets: list
        :return: bool
        """
        return self.redis_client.setex(self.JOB_ID_PREFIX + job_id,
                                       json.dumps(backend_targets), ttl)

    def get_job(self, job_id):
        """ Get backend servers associated with job
        :param job_id: The job_id we are searching for
        :return: list of backend or an empty list
        """
        response = self.redis_client.get(self.JOB_ID_PREFIX + job_id)

        return response and json.loads(response) or []


class JobView(BaseMethodView):
    """ RPC job API view """

    decorators = [check_query, validate_json]

    def get(self, job_id=None):
        """
        Query the RPC service for job records with a given projection
        or get one by job_id.

        :param job_id: RPC job id, default is None.
        :return: List of job objects or a single job object.
        """
        projection = self.get_projection_from_qsa()
        if job_id is None:
            data = self.rpc_client.get_jobs(projection or {'instruction': 0})
        else:
            data = self.rpc_client.get_job(job_id, projection)
            if not data:
                raise HTTPError(
                    'Job {} does not exist'.format(job_id), status_code=404)
        return jsonify(data)

    def post(self):
        """
        Creates a job with the given instructions.

        :return: The created job id.
        """
        instruction = request.json.get('instruction')
        if not isinstance(instruction, dict):
            raise HTTPError(
                'Command is missing from request or is malformed',
                status_code=400)
        query = request.json.get('query')
        job_id = self.rpc_client.create_job(query, instruction)

        if not job_id:
            raise HTTPError(
                'Query did not match any active agents', status_code=404)
        return jsonify(job_id)


class JobStatusView(BaseMethodView):
    """ RPC job status view """

    def get(self, job_id):
        """
        Query the RPC service for a job record with a given job_id.

        :param job_id: RPC job id.
        :return: Job status dictionary.
        """
        job = self.rpc_client.get_job_status(job_id)
        if not job:
            raise HTTPError(
                'Job {} does not exist'.format(job_id), status_code=404)
        return jsonify(job)


class JobTaskView(BaseMethodView):
    """ RPC job task view """

    def get(self, job_id):
        """
        Query the RPC service for tasks associated to a given job_id.

        :param job_id: RPC job id.
        :return: List of task objects.
        """
        projection = self.get_projection_from_qsa()
        tasks = self.rpc_client.get_job_tasks(job_id, projection)
        if tasks['count'] == 0:
            raise HTTPError(
                'No tasks exist for job {}'.format(job_id), status_code=404)
        return jsonify(tasks)


class TaskView(BaseMethodView):
    """ RPC task view """

    def get(self, task_id):
        """
        Query the RPC service for a task record with a given task_id.

        :param task_id: RPC task id.
        :return: Sinle RPC task object.
        """
        task = self.rpc_client.get_task(task_id)
        if not task:
            raise HTTPError('Task not found', status_code=404)
        return jsonify(task)
