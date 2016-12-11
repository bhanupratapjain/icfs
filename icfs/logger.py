# At the beginning of every .py file in the project
import logging

import sys


def class_decorator(decorator):
    def decorate(cls):
        for attr in cls.__dict__: # there's propably a better way to do this
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate


def logger(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        f_back = sys._getframe().f_back
        log = logging.getLogger(fn.__name__)
        log.error('[%s] [%s] [Start]' % (f_back.f_code.co_filename, fn.__name__))
        out = apply(fn, args, kwargs)
        log.error('[%s] [%s] [End]' % (f_back.f_code.co_filename, fn.__name__))
        # Return the return value
        return out
    return wrapper


# Do the following section only in application's app/__init__.py
# Other files will pick it up from here.
# FORMAT = '[%(asctime)s] [%(name)s] [%(levelname)s] [%(funcName)s] %(message)s'
FORMAT = '[%(asctime)s] %(message)s'

# Change logging LEVEL according to debugging needs.
# Probably better to read this from a config or a launch parameter.
LEVEL = logging.ERROR

logging.basicConfig(format=FORMAT, level=LEVEL)
# Up to here only for app/__init__.py

# This section again at the beginning of every .py file
log = logging.getLogger(__name__)
log.info('Entered module: %s' % __name__)
