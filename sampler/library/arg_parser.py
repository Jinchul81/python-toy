import argparse

from library import yaml_parser

def get():
  parser = argparse.ArgumentParser(description=
    "Web request generator using dummy data")
  parser.add_argument("-c", "--conf", dest="yaml_path", default="conf/dev.yaml",
    help="use custom yaml config (default: conf/dev.yaml)")
  args = parser.parse_args()

  return yaml_parser.load(args.yaml_path)
