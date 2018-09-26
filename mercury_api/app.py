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

from flask import Flask, jsonify

from mercury_api.configuration import get_api_configuration
from mercury_api.exceptions import HTTPError
from mercury_api.transaction_log import setup_logging
from mercury_api.views.active import active_blueprint
from mercury_api.views.inventory import inventory_blueprint
from mercury_api.views.rpc import rpc_blueprint
from mercury_api.custom_converters import BlackListConverter


app = Flask(__name__)

# Set default app logging
log = setup_logging(app)


# Add custom converters
app.url_map.converters['blacklist'] = BlackListConverter


# Attach http error handler
@app.errorhandler(HTTPError)
def http_error(error):
    """
    Sets the app error handler to modify the error message in the 
    response object and log it in the transactional logger.
    :param error: 
    :return: Flask response object
    """
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    log.error(error.message)
    return response


# Attach transactional log handler
@app.after_request
def log_request(response):
    """
    Logs the response status in the transactional logger after the
    request is processed.
    :param response: 
    :return: Flask response object 
    """
    log.info(response.status)
    return response


# Register blueprints
app.register_blueprint(active_blueprint, url_prefix='/api/active')
app.register_blueprint(inventory_blueprint, url_prefix='/api/inventory')
app.register_blueprint(rpc_blueprint, url_prefix='/api/rpc')


if __name__ == '__main__':
    config = get_api_configuration()
    app.run(host=config.api.host, port=config.api.port, debug=True)
