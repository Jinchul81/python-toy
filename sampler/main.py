from library import version
version.check()

if __name__ == '__main__':
  from library import arg_parser, http_client, version
  http_client.run(arg_parser.get())
