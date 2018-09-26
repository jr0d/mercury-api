"""
This module defines all the flask custom converters for mercury api.
"""
from werkzeug.routing import BaseConverter, ValidationError


class BlackListConverter(BaseConverter):
    """
    Converter to validate the value is not part of a specified blacklist
    """
    def __init__(self, map, *blacklist):
        super().__init__(map)
        self.blacklist = blacklist

    def to_python(self, value):
        if value in self.blacklist:
            raise ValidationError()
        return value
