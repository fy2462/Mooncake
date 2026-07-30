"""Source-only fixture: importing this module must never be required."""

FAKE_DECLARATION = "def test_from_string(): pass"


def test_function():
    pass


def helper():
    pass


def outer_helper():
    def test_nested():
        pass

    return test_nested


class TestClient:
    def test_method(self):
        pass

    def helper_method(self):
        pass


class HelperClient:
    def test_method_on_non_test_class(self):
        pass


# def test_from_comment(): pass
