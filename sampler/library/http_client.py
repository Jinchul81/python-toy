import asyncio

from aiohttp import ClientSession
from .context import Context
from library import param_builder
from library.timer import timed
from urllib.parse import urlencode

async def fetch(session, url):
  async with session.get(url) as response:
    return await response.read()

async def bound_fetch(sem, session, context):
  async with sem:
    await fetch(session, context)

async def _run(context):
  tasks = []
  sem = asyncio.Semaphore(1000)

  async with ClientSession() as session:
    for iter in context:
      url = "{url}/{controller}?{params}".format(
        url=iter.base_url,
        controller=iter.controller,
        params=urlencode(param_builder.get(iter)))
      task = asyncio.ensure_future(bound_fetch(sem, session, url))
      tasks.append(task)

    responses = asyncio.gather(*tasks)
    await responses

@timed()
def run(properties):
  context = Context(properties)
  loop = asyncio.get_event_loop()
  future = asyncio.ensure_future(_run(context))
  loop.run_until_complete(future)
  loop.close()
