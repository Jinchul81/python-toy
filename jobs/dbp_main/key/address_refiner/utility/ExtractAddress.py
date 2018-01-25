import sys
import re
from os import listdir

# Extract address parts from CSV style files
#
# Steps)
# 1. Collect relevant file list
# 2. Parse each line and extract address
# 3. Write address
#
# Author: Jinchul (Aug 04, 2017)

def getFileList():
  filenames = []
  prog = re.compile('^0.*_0$')
  for filename in listdir("."):
    if prog.match(filename):
      filenames.append(filename)
  return filenames

output = open ('addr.txt', 'w')
for filename in getFileList():
  print "* open and convert file: %s" % filename
  input = open (filename, 'r').read().split('\n')
  for line in input: 
    cols = line.split(chr(0x02))
    if (len(cols) > 18):
       output.write("%s %s\n" % (cols[17], cols[18]))

output.close()

sys.exit(1)
