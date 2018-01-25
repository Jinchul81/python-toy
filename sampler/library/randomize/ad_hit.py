from random import choice

def get():
  return choice(['true', 'false'])

if __name__ == '__main__':
  for i in range(1, 10):
    print (get())
