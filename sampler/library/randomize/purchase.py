from random import choice

def get():
  return choice(['1 day', '1 week', '1 month', '3 months', '6 months'])

if __name__ == '__main__':
  for i in range(1, 10):
    print (get())
