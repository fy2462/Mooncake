import unittest

from mooncake_store.http_metadata_server import KVBootstrapServer


class FakeRequest:
    def __init__(self, method, key=None, body=b""):
        self.method = method
        self.query = {} if key is None else {"key": key}
        self._body = body

    async def read(self):
        return self._body


class FakeResponse:
    def __init__(self, status=200, text=None, body=None, content_type=None):
        self.status = status
        self.text = text
        self.body = body
        self.content_type = content_type


class MetadataServerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = KVBootstrapServer(port=0, response_factory=FakeResponse)

    async def test_missing_empty_and_whitespace_keys_are_rejected(self):
        for key in (None, "", "   "):
            for method in ("GET", "PUT", "DELETE"):
                with self.subTest(key=key, method=method):
                    response = await self.server.handle_metadata(
                        FakeRequest(method, key, b"value")
                    )
                    self.assertEqual(response.status, 400)
        self.assertEqual(self.server.store, {})

    async def test_valid_keys_are_trimmed_before_dispatch(self):
        put = await self.server.handle_metadata(
            FakeRequest("PUT", "  valid  ", b"value")
        )
        get = await self.server.handle_metadata(FakeRequest("GET", " valid "))

        self.assertEqual(put.status, 200)
        self.assertEqual(get.status, 200)
        self.assertEqual(get.body, b"value")
        self.assertEqual(self.server.store, {"valid": b"value"})


if __name__ == "__main__":
    unittest.main()
