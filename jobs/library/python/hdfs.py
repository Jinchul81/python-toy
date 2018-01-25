from snakebite.client import Client
from conf import HDFSConf

class HDFSHelper:
  def __init__(self, hostname=HDFSConf.hostname
             , port=HDFSConf.port):
    self.client = self.issue_client(hostname=hostname, port=port)

  def issue_client(self, hostname, port, use_trash=False):
    client = Client(host=hostname, port=port, use_trash=use_trash)
    return client

  def get_client(self):
    return self.client
