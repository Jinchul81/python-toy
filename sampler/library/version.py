import sys

def check():
  if sys.version_info < (3, 5):
    sys.stderr.write("Requires Python >= 3.5\n")
    sys.exit(1)
