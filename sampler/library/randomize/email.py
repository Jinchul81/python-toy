import names
from . import domain

def get(name=None, gender=None):
  if name is None:
    name = names.get_first_name(gender).lower()
  return '{0}@{1}'.format(name, domain.get())

if __name__ == '__main__':
  for i in range(1, 10):
    print (get_email())
