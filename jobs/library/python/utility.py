import re
import os
import sys
import time
from datetime import datetime, timedelta
from termcolor import colored
from enum import Enum

class Env:
  @staticmethod
  def get_env(key):
    try:
      value = os.environ[key]
    except Exception as e:
      return ""
    else:
      return value

  @staticmethod
  def check_and_get_env(key):
    value = Env.get_env(key)
    if None == value or "" == value:
      raise ValueError(TextColor.red(msg))
    return value

class Date:
  @staticmethod
  def get_format_YYYYMMDD():
    return '%Y%m%d'

  @staticmethod
  def get_nday_ago(dt, days):
    return dt - timedelta(days)

  @staticmethod
  def get_start_date_of_two_weeks_ago(dt):
    return Date.get_nday_ago(dt, 2*7-1)

  @staticmethod
  def get_start_date_of_four_weeks_ago(dt):
    return Date.get_nday_ago(dt, 4*7-1)

  @staticmethod
  def is_sunday(dt = datetime.now()):
    return dt.weekday() == 6

  @staticmethod
  def get_str_format_YYYYMMDD(dt):
    return dt.strftime(Date.get_format_YYYYMMDD())

  @staticmethod
  def get_date_format_YYYYMMDD(s):
    if len(s) != 8:
      raise ValueError(TextColor.red("Format should be YYYYMMDD: [{0}]".format(s)))
    return datetime.strptime(s, Date.get_format_YYYYMMDD())

  @staticmethod
  def get_calendar_week(dt):
    return dt.date().isocalendar()[1]

class TimerFormat(Enum):
  MICRO_SECOND  = "usec(s)"
  MILLI_SECOND  = "msec(s)"
  SECOND        = "sec(s)"
  MINUTE        = "min(s)"
  HOUR          = "hour(s)"

class Timer:
  def __init__(self):
    self.init_time = self.get_elapsed_cputime()
    self.last_check_point = self.get_elapsed_cputime()

  def get_elapsed_cputime(self):
    return datetime.datetime.now()

  def convert_datetime_delta_to_msecs(self, t1, t2):
    delta = t2 - t1
    return delta.total_seconds() * 1e6

  def get_formatted_time(self, t1, t2, format):
    delta = self.convert_datetime_delta_to_msecs(t1, t2)
    TimerFormatDivider = {
      TimerFormat.MICRO_SECOND  : 1,
      TimerFormat.MILLI_SECOND  : 1e3,
      TimerFormat.SECOND        : 1e6,
      TimerFormat.MINUTE        : 1e6 * 60,
      TimerFormat.HOUR          : 1e6 * 360,
    }
    return delta / TimerFormatDivider.get(format)

  def get_elapsed_time(self, format = TimerFormat.SECOND):
    prev_time = self.last_check_point
    self.last_check_point = self.get_elapsed_cputime()
    return self.get_formatted_time(prev_time, self.last_check_point, format)

  def get_total_elapsed_time(self, format = TimerFormat.SECOND):
    return self.get_formatted_time(self.init_time, self.get_elapsed_cputime(), format)

  def get_formatted_elapsed_time(self, format = TimerFormat.SECOND):
    s = "%.2f %s" % (self.get_elapsed_time(format), format.value)
    return s;

  def get_formatted_total_elapsed_time(self, format = TimerFormat.SECOND):
    s = "%.2f %s" % (self.get_total_elapsed_time(format), format.value)
    return s;

class System:
  @staticmethod
  def exit_with_ok():
    sys.exit(0)

  @staticmethod
  def exit_with_faile():
    sys.exit(1)

  @staticmethod
  def get_function_name():
    return sys._getframe(1).f_code.co_name

  @staticmethod
  def get_parent_function_name():
    return sys._getframe(2).f_code.co_name

class Shell:
  @staticmethod
  def check_error(retval, command, chdir_path):
    if 0 != retval:
      errMsg = colored("""
      Unexpected error:
      * error code={0}
      * command="{1}"
      * new path={2}
      """.format(retval, command, chdir_path), "red")
      raise Exception(errMsg)

  @staticmethod
  def change_path(chdir_path):
    if None != chdir_path:
      real_path = os.path.realpath(chdir_path)
      os.chdir(real_path)
      curr_path = os.getcwd()
      retval = 0 if curr_path == real_path else 1
      Shell.check_error(retval,
          "os.chdir() executed but the comparison of os.getcwd() failed: os.getcwd()={0}"
          .format(curr_path), real_path)

  @staticmethod
  def exists_file(command, chdir_path = None):
    Shell.change_path(chdir_path)
    retval = 0 if os.path.isfile(command) else 1
    Shell.check_error(retval, command, chdir_path)

  @staticmethod
  def execute_command(command, chdir_path = None):
    Shell.change_path(chdir_path)
    retval = os.system(command)
    Shell.check_error(retval, command, chdir_path)

  @staticmethod
  def execute_r_command(script_path, db):
    Shell.execute_command("R -f {0} --no-save --args {1}".format(script_path, db))

  @staticmethod
  def execute_command_no_throw(command, chdir_path = None):
    try:
      Shell.execute_command(command, chdir_path)
    except Exception as e:
      pass

  @staticmethod
  def uncompress_snappy_files(path):
    command="snzip -d {0}/*".format(path)
    Shell.execute_command(command)

class File:
  @staticmethod
  def exists_file(path_text):
    return os.path.exists(path_text)

  @staticmethod
  def is_file(path_text):
    if File.exists_file(path_text):
      return os.path.isfile(path_text)
    return False

  @staticmethod
  def is_directory(path_text):
    if File.exists_file(path_text):
      return os.path.isdir(path_text)
    return False

  @staticmethod
  def throw_not_found_exception(path_text):
      raise Exception("Not found {0}".path_text)

  @staticmethod
  def throw_error_if_invalid_file(path_text):
    if False == File.is_file(path_text):
      File.throw_not_found_exception(path_text)

  @staticmethod
  def throw_error_if_invalid_directory(path_text):
    if False == File.is_directory(path_text):
      File.throw_not_found_exception(path_text)

class PrintPretty:
  def __init__(self):
    self.depth = 1
    self.timer = Timer()
    self.colors = ["green", "blue", "yellow"]

  def get_elapsed_time(self, timer):
    return " [%s]" % timer.get_formatted_elapsed_time()

  def print_pretty(self, msg, color, show_elapsed_time):
    if show_elapsed_time:
      msg = "{0} {1}".format(msg, self.get_elapsed_time(self.timer))

    print colored(msg, color)

  def print_message(self, msg, show_elapsed_time):
    prefix = ""
    for i in range(self.depth):
      prefix = "{0}{1}".format(prefix, "*")
    index_of_color = self.depth % len(self.colors)
    self.print_pretty("{0} {1}".format(prefix, msg), self.colors[index_of_color], show_elapsed_time)

  def increase_depth(self):
    self.depth = self.depth + 1

  def decrease_depth(self):
    self.depth = self.depth - 1

class PrintHelper:
  def __init__(self, msg, print_pretty):
    self.print_pretty = print_pretty
    self.msg = msg
    self.print_pretty.print_message("{0} started.".format(self.msg), show_elapsed_time = False)
    self.print_pretty.increase_depth()

  def __del__(self):
    self.print_pretty.decrease_depth()
    self.print_pretty.print_message("{0} finished.".format(self.msg), show_elapsed_time = True)

class TextColor:
  @staticmethod
  def grey(msg):
    return colored(msg, "grey")

  @staticmethod
  def red(msg):
    return colored(msg, "red")

  @staticmethod
  def green(msg):
    return colored(msg, "green")

  @staticmethod
  def yellow(msg):
    return colored(msg, "yellow")

  @staticmethod
  def blue(msg):
    return colored(msg, "blue")

  @staticmethod
  def magenta(msg):
    return colored(msg, "magenta")

  @staticmethod
  def cyan(msg):
    return colored(msg, "cyan")

  @staticmethod
  def white(msg):
    return colored(msg, "white")
