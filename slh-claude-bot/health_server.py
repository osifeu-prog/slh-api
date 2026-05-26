import aiohttp.web as web

async def health_handler(request):
    return web.Response(text="OK")

async def run_health_server():
    app = web.Application()
    app.router.add_get('/api/health', health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8000)
    await site.start()
    print("Health server running on port 8000")
