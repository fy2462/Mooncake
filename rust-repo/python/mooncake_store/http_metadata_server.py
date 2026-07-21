"""Small HTTP bootstrap metadata server shipped with the Rust wheel."""

import argparse
import asyncio

from .service import install_shutdown_handlers


class KVBootstrapServer:
    def __init__(self, port, host="0.0.0.0", response_factory=None):
        self.port = port
        self.host = host
        self.store = {}
        self.lock = asyncio.Lock()
        self._response_factory = response_factory

    def _response(self, **kwargs):
        if self._response_factory is None:
            from aiohttp import web

            return web.Response(**kwargs)
        return self._response_factory(**kwargs)

    async def handle_metadata(self, request):
        key = request.query.get("key", "").strip()
        if not key:
            return self._response(
                text="metadata key is required",
                status=400,
                content_type="application/json",
            )
        if request.method == "GET":
            async with self.lock:
                value = self.store.get(key)
            if value is None:
                return self._response(text="metadata not found", status=404)
            return self._response(body=value, content_type="application/json")
        if request.method == "PUT":
            value = await request.read()
            async with self.lock:
                if "rpc_meta" in key and key in self.store:
                    return self._response(text="duplicate rpc_meta key", status=400)
                self.store[key] = value
            return self._response(text="metadata updated")
        if request.method == "DELETE":
            async with self.lock:
                if key not in self.store:
                    return self._response(text="metadata not found", status=404)
                del self.store[key]
            return self._response(text="metadata deleted")
        return self._response(text="method not allowed", status=405)

    def create_app(self):
        from aiohttp import web

        app = web.Application()
        app.router.add_route("*", "/metadata", self.handle_metadata)
        return app


async def main(argv=None):
    from aiohttp import web

    parser = argparse.ArgumentParser(description="Mooncake HTTP metadata server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)

    shutdown_event = asyncio.Event()
    install_shutdown_handlers(asyncio.get_running_loop(), shutdown_event)
    server = KVBootstrapServer(args.port, args.host)
    runner = web.AppRunner(server.create_app())
    await runner.setup()
    try:
        await web.TCPSite(runner, args.host, args.port).start()
        await shutdown_event.wait()
    finally:
        await runner.cleanup()


def sync_main():
    asyncio.run(main())
