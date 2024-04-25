import os
import uuid


def pytest_addoption(parser):
    parser.addoption("--token", action="store", default="")


def pytest_generate_tests(metafunc):
    # token = metafunc.config.option.token
    token = os.environ.get("ACCESS_TOKEN")
    token = token if token and len(token) > 0 else None

    if "token" in metafunc.fixturenames:
        metafunc.parametrize("token", [token])
