"""Signal-aware REST service backed by the Rust ``MooncakeClient``."""

import argparse
import asyncio
import json
import logging
import signal
from collections.abc import Awaitable, Callable


def install_shutdown_handlers(loop, shutdown_event):
    """Publish SIGINT/SIGTERM through one asyncio event."""

    def request_shutdown(signum):
        logging.info("received signal %s; shutting down", signum)
        shutdown_event.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request_shutdown, signum)
        except (NotImplementedError, RuntimeError):
            signal.signal(
                signum,
                lambda value, _frame: loop.call_soon_threadsafe(
                    request_shutdown, value
                ),
            )


class StoreService:
    """Own one Rust client and make its startup/shutdown idempotent."""

    def __init__(self, create_client, retry_interval=1.0):
        self._create_client = create_client
        self._retry_interval = retry_interval
        self.client = None

    async def start(self, shutdown_event, max_wait_time=60):
        deadline = asyncio.get_running_loop().time() + max_wait_time
        while not shutdown_event.is_set():
            try:
                client = await self._create_client()
                await asyncio.sleep(0)
                if shutdown_event.is_set():
                    client.close()
                    return False
                self.client = client
                return True
            except Exception as error:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    logging.error("store startup timed out: %s", error)
                    return False
                logging.warning("store startup failed; retrying: %s", error)
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(),
                        timeout=min(self._retry_interval, remaining),
                    )
                except asyncio.TimeoutError:
                    pass
        return False

    async def stop(self):
        client, self.client = self.client, None
        if client is not None:
            client.close()


async def run_service(
    service: StoreService,
    serve_http: Callable[[asyncio.Event], Awaitable[None]],
    shutdown_event: asyncio.Event,
    max_wait_time=60,
):
    """Run startup and HTTP lifetime under one shutdown event."""
    try:
        if not await service.start(shutdown_event, max_wait_time=max_wait_time):
            return
        await serve_http(shutdown_event)
    finally:
        await service.stop()


def _json_response(payload, status=200):
    from aiohttp import web

    return web.json_response(payload, status=status)


def create_app(service):
    from aiohttp import web

    app = web.Application(client_max_size=100 * 1024 * 1024)

    async def health(_request):
        if service.client is None:
            return _json_response({"status": "unavailable"}, 503)
        healthy = await service.client.health_check()
        return _json_response(
            {"status": "ok" if healthy else "unhealthy"},
            200 if healthy else 503,
        )

    async def put(request):
        data = await request.json()
        key = data.get("key")
        value = data.get("value")
        if not isinstance(key, str) or not key.strip() or value is None:
            return _json_response({"error": "Missing key or value"}, 400)
        await service.client.put(key.strip(), str(value).encode())
        return _json_response({"status": "success"})

    async def get(request):
        value = await service.client.get(request.match_info["key"])
        if value is None:
            return _json_response({"error": "Key not found"}, 404)
        return web.Response(body=value, content_type="application/octet-stream")

    async def exists(request):
        value = await service.client.exists(request.match_info["key"])
        return _json_response({"exists": bool(value)})

    async def remove(request):
        await service.client.remove(request.match_info["key"])
        return _json_response({"status": "success"})

    app.add_routes(
        [
            web.get("/health", health),
            web.put("/api/put", put),
            web.get("/api/get/{key}", get),
            web.get("/api/exist/{key}", exists),
            web.delete("/api/remove/{key}", remove),
        ]
    )
    return app


async def _serve_app(app, host, port, shutdown_event):
    from aiohttp import web

    runner = web.AppRunner(app)
    await runner.setup()
    try:
        await web.TCPSite(runner, host, port).start()
        await shutdown_event.wait()
    finally:
        await runner.cleanup()


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description="Rust-backed Mooncake REST service")
    parser.add_argument("--config", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--max-wait-time", type=float, default=60)
    return parser.parse_args(argv)


async def main(argv=None):
    args = parse_arguments(argv)
    with open(args.config, encoding="utf-8") as config_file:
        config = json.load(config_file)

    async def create_client():
        from . import MooncakeClient

        return await MooncakeClient.create(
            config["local_hostname"],
            config["metadata_server"],
            config["master_server_address"],
            config.get("protocol", "tcp"),
            config.get("device_name", ""),
            config.get("global_segment_size", -1),
            config.get("local_buffer_size", -1),
            enable_client_http_server=config.get("enable_client_http_server", False),
            client_http_port=config.get("client_http_port", 9300),
        )

    shutdown_event = asyncio.Event()
    install_shutdown_handlers(asyncio.get_running_loop(), shutdown_event)
    service = StoreService(create_client)
    await run_service(
        service,
        lambda event: _serve_app(create_app(service), args.host, args.port, event),
        shutdown_event,
        max_wait_time=args.max_wait_time,
    )


def sync_main():
    asyncio.run(main())
