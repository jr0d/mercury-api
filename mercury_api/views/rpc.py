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
import time
import uuid

from concurrent.futures import TimeoutError

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

    def __init__(self, dispatch_timeout=10):
        """

        :param dispatch_timeout:
        """
        super(BaseJobView, self).__init__()
        self.dispatch_timeout = dispatch_timeout

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

    def aggregate_job(self, job_id):
        """
        Really lazy aggregation, simply displays the result from each backend
        :param job_id:
        :return:
        """
        backends = self.get_job_relationship(job_id)
        if not backends:
            return {}  # Should 404 in view

        def get_job(backend):
            return get_rpc_client(backend).get_job(job_id)

        aggregate_result = {'job_id': job_id, 'backend_responses': []}
        for result in get_executor().map(get_job,
                                         backends,
                                         timeout=self.dispatch_timeout):
            aggregate_result['backend_responses'].append(result)

        return aggregate_result

    def aggregate_status(self, job_id):
        """

        :param job_id:
        :return:
        """

        backends = self.get_job_relationship(job_id)
        if not backends:
            return

        def get_status(backend):
            return get_rpc_client(backend).get_job_status(job_id)

        aggregate_result = {'job_id': job_id,
                            'tasks': [],
                            'task_count': 0,
                            'job_start_times': [],
                            'job_completed_times': []}
        for backend, result in zip(backends,
                                   get_executor().map(
                                       get_status,
                                       backends,
                                       timeout=self.dispatch_timeout)):
            aggregate_result['task_count'] += result['task_count']
            aggregate_result['tasks'] += \
                [task['backend'] for task in result['tasks']]
            aggregate_result['has_failures'] = result['has_failures']
            aggregate_result['job_start_times'].append({
                'backend': backend,
                'time_started': result['time_started']
            })
            aggregate_result['job_completed_times'].append({
                'backend': backend,
                'time_completed': result['time_completed']
            })






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

    def poll_futures(self, futures):
        results = []
        for future in futures:
            try:
                results.append(future.result(timeout=self.dispatch_timeout))
            except TimeoutError:
                results.append(future.exception())
        return results

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

        # noinspection PyBroadException
        try:
            results = self.poll_futures(futures)
        except Exception:
            log.exception(f'An unhandled exception occurred has occurred while '
                          f'submitting job {job_id}')
            raise

        errors = []
        for target, result in zip(targets, results):
            if isinstance(result, TimeoutError):
                errors.append(target)
                log.error(f'Timeout dispatching job to {target}')
        if errors:
            raise HTTPError('Timeout while interacting with backend system(s): '
                            '{}\n'.format('\n'.join(errors)) +
                            '\nSome jobs may have executed.')

        return job_id

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
        aggregated by at the API layer.

        2) Performance: At least two and at most 1 + len(backends) inventory
        queries are now required to inject a job. If the backends simply took
        a list of pre-rendered tasks, the backends would only be concerned with
        dispatch and forwarding updates to the control server. Rather than
        attempting to enumerate targets on their own

        In the future implementation the jobs collection (and job creation
        service) will be centralized, thus removing the need to store target
        backends separately from the actual job. Data would no longer need to be
        aggregated from multiple sources and round trips to the database and the
        backends will be greatly reduced

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

    def get(self, job_id):
        """
        Query the RPC service for job records with a given projection
        or get one by job_id.

        :param job_id: RPC job id, default is None.
        :return: List of job objects or a single job object.
        """
        projection = self.get_projection_from_qsa()
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
