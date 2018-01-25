from aiohttp import web

async def handle(request):
    print (request.url.human_repr())
#    print (request.path_qs)
    return web.Response(body="got it!".encode('utf-8'))

app = web.Application()
app.router.add_route('GET', '/{name}', handle)

web.run_app(app)
