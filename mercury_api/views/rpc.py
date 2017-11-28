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
import logging
import uuid

from flask import request, jsonify
from mercury.common.transport import format_zurl

from mercury_api.views.base import BaseMethodView
from mercury_api.clients import get_rpc_client
from mercury_api.concurrent import get_executor
from mercury_api.exceptions import HTTPError
from mercury_api.decorators import validate_json, check_query

log = logging.getLogger(__name__)


class BaseJobView(BaseMethodView):
    JOB_ID_PREFIX = 'job_id-'

    def __init__(self,
                 soft_dispatch_timeout=2,
                 dispatch_timeout=10,
                 future_poll_time=.2):
        """

        :param soft_dispatch_timeout:
        :param dispatch_timeout:
        :param future_poll_time:
        """
        super(BaseJobView, self).__init__()
        self.soft_dispatch_timeout = soft_dispatch_timeout
        self.dispatch_timeout = dispatch_timeout
        self.future_poll_time = future_poll_time

    def store_job_relationship(self, job_id, backend_targets, ttl):
        """ Store job_id and backend relationship

        :param job_id: The job_id we'll be using
        :param backend_targets: Targets that the job will be created on
        :param ttl: TTL for record
        :type backend_targets: list
        :return: bool
        """
        return self.redis_client.setex(self.JOB_ID_PREFIX + job_id,
                                       json.dumps(backend_targets), ttl)

    def get_job_relationship(self, job_id):
        """ Get backend servers associated with job
        :param job_id: The job_id we are searching for
        :return: list of backend or an empty list
        """
        response = self.redis_client.get(self.JOB_ID_PREFIX + job_id)

        return response and json.loads(response) or []

    @staticmethod
    def _submit_job(backend_zurl, job_id, target_query, instruction):
        """
        :param backend_zurl:
        :param job_id:
        :param target_query:
        :param instruction:
        :return:
        """
        log.info(f'Dispatching job {job_id} to {backend_zurl}')
        return get_rpc_client(backend_zurl).create_job(
            target_query,
            instruction,
            job_id=job_id
        )

    def poll_futures(self, futures, poll_time):
        pass

    def submit_jobs(self, target_query, instruction):
        """

        :param target_query:
        :param instruction:
        :return:
        """
        targets = self.get_origins_from_query(target_query)
        log.debug(f'Scheduling task targeting {", ".join(targets)}')
        job_id = uuid.uuid4()

        futures = list()
        for target in targets:
            futures.append(self._submit_job(
                target, job_id, target_query, instruction))

    def get_origins_from_query(self, target_query):
        """ As we move things outward and upward, such as the Active collection,
        it becomes more apparent that having the jobs data layer exist parallel
        with the backend is not the correct approach. Back ends should only
        contain task workers (redis). Job and task updates should be forwarded
        to the control layer ( currently where the inventory and API services
        live ). For now, we will aggregate the job data returned from each
        target back end. This temporary implementation will satisfy the design
        requirement of scheduling jobs on multiple backend systems with one
        API call.

        Why is this implementation bad:

        1) Complexity: The current implementation requires three databases and
        two mongo collections. Multiple calls for injection and status must be
        made to target backend. Data returned from those backends must be
        aggregated by at the API layer. In the future implementation jobs
        collection (and job creation service) was centralized, we would remove
        the need to store target backends separately from actual job. Also, we
        would no longer need to aggregate data from multiple sources.

        2) Performance: At least two and at most 1 + len(backends) inventory
        queries are now required to inject a job. If the backends simply took
        a list of pre-rendered tasks, the backends would only be concerned with
        dispatch and forwarding updates to the control server. Rather than
        attempting to enumerate targets on their own

        :param target_query: The query we will use to aggregate targets
        """
        origins = self.inventory_client.query(
            target_query,
            projection=['origin']
        )['items']

        origin_urls = set()

        for origin in origins:
            origin_urls.add(format_zurl(origin['address'], origin['port']))

        return list(origin_urls)


class JobView(BaseJobView):
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


class JobStatusView(BaseJobView):
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


class JobTaskView(BaseJobView):
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


class TaskView(BaseJobView):
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
