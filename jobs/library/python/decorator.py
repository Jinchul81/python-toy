import time
import datetime
import functools
from termcolor import colored

class duration(object):
  def __init__(self, detailed = False):
    self.detailed = detailed

  def __call__(self, f):
    def wrapper_func(*args, **kwargs):
      start_time = time.time()
      result = f(*args, **kwargs)
      end_time = time.time()

      curr_time = "[%s]" % datetime.datetime.strftime(
          datetime.datetime.now(), "%Y/%m/%d %H:%M:%S")

      func_args = "(...)"
      if self.detailed:
        func_args = "(%r, %r)" % (args, kwargs)

      msg =  "[%s] %s%s %2.2f sec" % \
          (curr_time, f.__name__, func_args, end_time - start_time)
      print colored(msg, "yellow")

    return wrapper_func

