import asyncio
import signal
import unittest
from unittest import mock

from mooncake_store.service import StoreService, install_shutdown_handlers, run_service


class FakeClient:
    def __init__(self):
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


class ServiceLifecycleTest(unittest.IsolatedAsyncioTestCase):
    async def test_sigterm_sets_the_shared_shutdown_event(self):
        shutdown = asyncio.Event()
        loop = mock.Mock()

        install_shutdown_handlers(loop, shutdown)

        sigterm_call = next(
            call
            for call in loop.add_signal_handler.call_args_list
            if call.args[0] == signal.SIGTERM
        )
        sigterm_call.args[1](*sigterm_call.args[2:])
        self.assertTrue(shutdown.is_set())

    async def test_shutdown_interrupts_startup_retry(self):
        shutdown = asyncio.Event()
        attempts = 0

        async def create_client():
            nonlocal attempts
            attempts += 1
            shutdown.set()
            raise RuntimeError("not ready")

        service = StoreService(create_client, retry_interval=60)
        self.assertFalse(await service.start(shutdown, max_wait_time=60))
        self.assertEqual(attempts, 1)

    async def test_initialized_client_is_closed_exactly_once(self):
        client = FakeClient()

        async def create_client():
            return client

        service = StoreService(create_client)
        shutdown = asyncio.Event()
        self.assertTrue(await service.start(shutdown, max_wait_time=1))
        await service.stop()
        await service.stop()
        self.assertEqual(client.close_calls, 1)

    async def test_shutdown_exits_http_loop_and_closes_client(self):
        client = FakeClient()
        http_started = asyncio.Event()
        http_stopped = asyncio.Event()

        async def create_client():
            return client

        async def serve_http(shutdown):
            http_started.set()
            await shutdown.wait()
            http_stopped.set()

        shutdown = asyncio.Event()
        task = asyncio.create_task(
            run_service(
                StoreService(create_client),
                serve_http,
                shutdown,
                max_wait_time=1,
            )
        )
        await asyncio.wait_for(http_started.wait(), timeout=1)
        shutdown.set()
        await asyncio.wait_for(task, timeout=1)

        self.assertTrue(http_stopped.is_set())
        self.assertEqual(client.close_calls, 1)


if __name__ == "__main__":
    unittest.main()
