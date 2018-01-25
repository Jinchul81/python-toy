from random import randint

def get(start=1, end=10):
  # start <= N <= end
  return randint(start, end)

if __name__ == '__main__':
  for i in range(1, 10):
    print (get())
