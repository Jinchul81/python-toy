import datetime
import functools
import time

from .text_color import *

def timed(level=None, format='%s: %s ms'):
    def decorator(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
          start_time = time.time()
          result = fn(*args, **kwargs)
          end_time = time.time()

          curr_time = "[%s]" % datetime.datetime.strftime(
              datetime.datetime.now(), "%Y/%m/%d %H:%M:%S")

          func_args = "(...)"

          msg =  "[%s] %s%s %2.2f sec" % \
              (curr_time, fn.__name__, func_args, end_time - start_time)
          print (yellow(msg))
          return result
        return inner

    return decorator
