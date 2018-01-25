from random import choice

def get():
  return choice(['gmail.com', 'mail.com', 'yahoo.com', 'naver.com'])

if __name__ == '__main__':
  for i in range(1, 10):
    print (get_email())
