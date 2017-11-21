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
import datetime

from flask import request

from mercury_api.configuration import get_api_configuration


class TransactionFilter(logging.Filter):
    """
    Filter log records to include request data in the transactional log.
    """

    def filter(self, record):
        now = datetime.datetime.utcnow()
        record.utcnow = now.strftime('%Y-%m-%d %H:%M:%S,%f %Z')
        record.url = request.path
        record.method = request.method
        record.client = request.environ.get('X-Forwarded-For',
                                            request.remote_addr)
        return True


formatter = logging.Formatter("%(utcnow)s : %(levelname)s client=%(client)s "
                              "[%(method)s] url=%(url)s : %(message)s")


def setup_logging(app):
    """
    Sets the log level set in the api configuration and attaches the
    required log handlers to the default app logger.

    :param app: Flask app instance
    :return: The Flask app default logger.
    """
    log_configuration = get_api_configuration().api.logging

    for handler in app.logger.handlers:
        app.logger.removeHandler(handler)

    logging.basicConfig(level=log_configuration.level)

    if log_configuration.console_out:
        # Ignore the werkzeug request logs
        werkzeug = logging.getLogger('werkzeug')
        werkzeug.setLevel(logging.ERROR)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.addFilter(TransactionFilter())
        stream_handler.setFormatter(formatter)
        app.logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_configuration.log_file)
    file_handler.setLevel(log_configuration.level)
    file_handler.addFilter(TransactionFilter())
    file_handler.setFormatter(formatter)
    app.logger.addHandler(file_handler)
    app.logger.setLevel(log_configuration.level)
    app.logger.addFilter(TransactionFilter())
    return app.logger
