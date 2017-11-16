import bson
import logging

from bottle import route, run, request, HTTPResponse
from mercury.common.clients.inventory import InventoryClient
from mercury.common.clients.rpc.frontend import RPCFrontEndClient

from mercury_api.configuration import api_configuration


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

inventory_router_url = api_configuration['inventory']['inventory_router']
rpc_router_url = api_configuration['rpc']['rpc_router']

inventory_client = InventoryClient(inventory_router_url)

__rpc_client_cache = {}


def get_rpc_client(router_zurl):
    if not __rpc_client_cache.get(router_zurl):
        log.info(f'Creating connection to backend service: {router_zurl}')
        __rpc_client_cache[router_zurl] = RPCFrontEndClient(router_zurl)
    return __rpc_client_cache[router_zurl]


def http_error(message, code=500):
    return HTTPResponse({'error': True, 'message': message}, status=code)


def validate_json(f):
    def wrapper(*args, **kwargs):
        try:
            if not request.json:
                return http_error('JSON request is missing', code=400)
        except ValueError:
            log.debug('JSON request is malformed: {}'.format(
                request.body.read()))
            return http_error('JSON request is malformed', code=400)

        return f(*args, **kwargs)

    return wrapper


def check_query(f):
    def wrapper(*args, **kwargs):
        if not isinstance(request.json.get('query'), dict):
            return http_error('JSON request is malformed', code=400)
        return f(*args, **kwargs)
    return wrapper


def get_projection_from_qsa():
    projection_keys = request.query.get('projection', '')
    projection = {}
    if projection_keys:
        for k in projection_keys.split(','):
            projection[k] = 1

    return projection or None


def get_paging_info_from_qsa():
    _d = {
        'limit': 250,  # TODO: Move this to configuration files
        'offset_id': None,
        'sort_direction': 1
    }
    limit = request.query.get('limit')
    offset_id = request.query.get('offset_id')
    sort_direction = request.query.get('sort_direction')

    if limit and limit.isdigit():
        _d['limit'] = int(limit)

    if bson.ObjectId.is_valid(offset_id):
        _d['offset_id'] = offset_id

    try:
        _d['sort_direction'] = int(sort_direction)
    except (TypeError, ValueError):
        pass

    return _d


def convert_id(doc):
    if '_id' in doc:
        doc['_id'] = str(doc['_id'])
    return doc


def doc_transformer(doc):
    if not doc:
        return

    convert_id(doc)
    ttl_time_completed = doc.get('ttl_time_completed')
    if ttl_time_completed:
        doc['ttl_time_completed'] = ttl_time_completed.ctime()

    return doc

# TODO: Check for MercuryClientException when using inventory and rpc clients


@route('/api/inventory/computers', method='GET')
def computers():
    projection = get_projection_from_qsa()
    paging_data = get_paging_info_from_qsa()
    return inventory_client.query({}, projection=projection,
                                  limit=paging_data['limit'],
                                  sort_direction=paging_data['sort_direction'])


@route('/api/inventory/computers/query', method='POST')
@validate_json
@check_query
def computers_query():
    query = request.json.get('query')
    projection = get_projection_from_qsa()
    paging_data = get_paging_info_from_qsa()
    log.debug('QUERY: {}'.format(query))

    return inventory_client.query(query, projection=projection,
                                  limit=paging_data['limit'],
                                  sort_direction=paging_data['sort_direction'])


@route('/api/inventory/computers/count', method='POST')
@validate_json
@check_query
def computer_query_count():
    query = request.json.get('query')
    return {'count': inventory_client.count(query)}


@route('/api/inventory/computers/<mercury_id>', method='GET')
def computer(mercury_id):
    projection = get_projection_from_qsa()
    c = inventory_client.get_one(mercury_id, projection=projection)

    if not c:
        return http_error('mercury_id %s does not exist in inventory' % mercury_id,
                          404)

    return c


@route('/api/active/computers', method='GET')
def active_computers():
    projection = get_projection_from_qsa()
    paging_data = get_paging_info_from_qsa()
    if not projection:
        projection = {'mercury_id': 1}

    return inventory_client.query({'active': {'$ne': None}},
                                  projection=projection,
                                  limit=paging_data['limit'],
                                  sort_direction=paging_data['sort_direction'])


@route('/api/active/computers/<mercury_id>', method='GET')
def active_computer(mercury_id):
    projection = get_projection_from_qsa()
    if not projection:
        projection = {'active': 1, 'mercury_id': 1}

    c = inventory_client.query({'mercury_id': mercury_id, 'active': {'$ne': None}},
                               projection=projection)

    if not c:
        return http_error('mercury_id %s does not exist in inventory' % mercury_id,
                          404)
    return c


@route('/api/active/computers/query', method='POST')
@validate_json
@check_query
def active_computer_query():
    query = request.json.get('query')

    # Make sure we get only active devices
    query.update({'active': {'$ne': None}})
    projection = get_projection_from_qsa()
    paging_data = get_paging_info_from_qsa()
    return inventory_client.query(query, projection=projection,
                                  limit=paging_data['limit'],
                                  sort_direction=paging_data['sort_direction'])


@route('/api/rpc/jobs/<job_id>', method='GET')
def get_job(origin, job_id):
    projection = get_projection_from_qsa()
    jobs = rpc_client.get_job(job_id, projection)
    if not jobs:
        return http_error('Job {} does not exit'.format(job_id), 404)
    return jobs


@route('/api/rpc/jobs/<job_id>/status', method='GET')
def get_job_status(job_id):
    jobs = rpc_client.get_job_status(job_id)
    if not jobs:
        return http_error('Job {} does not exit'.format(job_id), 404)
    return jobs


@route('/api/rpc/jobs/<job_id>/tasks', method='GET')
def get_tasks(job_id):
    projection = get_projection_from_qsa()
    tasks = rpc_client.get_job_tasks(job_id, projection)
    if tasks['count'] == 0:
        return http_error('No tasks exist for job {}'.format(job_id), 404)
    return tasks


@route('/api/rpc/task/<task_id>')
def get_task(task_id):
    task = rpc_client.get_task(task_id)
    if not task:
        return http_error('Task not found', code=404)
    return task


@route('/api/rpc/jobs', method='GET')
def get_jobs():
    projection = get_projection_from_qsa() or {'instruction': 0}
    return rpc_client.get_jobs(projection)


@route('/api/rpc/jobs', method='POST')
@validate_json
@check_query
def post_jobs():
    instruction = request.json.get('instruction')

    if not isinstance(instruction, dict):
        return http_error('Command is missing from request or is malformed',
                          code=400)

    query = request.json.get('query')

    job_id = rpc_client.create_job(query, instruction)

    if not job_id:
        http_error('Query did not match any active agents', 404)

    return job_id


run(host='0.0.0.0', port=9005, debug=True)
