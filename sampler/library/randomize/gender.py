from random import choice

def get():
  return choice(['male', 'female'])

if __name__ == '__main__':
  for i in range(1, 10):
    print (get())
