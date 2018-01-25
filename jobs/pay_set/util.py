import datetime
import functools
import time


def timeit(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        print "[%s] %s (%r) Start" % \
              (datetime.datetime.strftime(datetime.datetime.now(), "%Y/%m/%d %H:%M:%S"),
               f.__name__, args)

        start_time = time.time()
        result = f(*args, **kwargs)
        end_time = time.time()

        print "[%s] %s (%r) End - %2.2f sec" % \
              (datetime.datetime.strftime(datetime.datetime.now(), "%Y/%m/%d %H:%M:%S"),
               f.__name__, args, end_time - start_time)

        return result

    return wrapper