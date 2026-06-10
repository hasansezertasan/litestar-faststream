from litestar_faststream.di import build_broker_provide


def test_build_broker_provide_resolves_to_instance() -> None:
    sentinel = object()
    provide = build_broker_provide(sentinel)
    assert provide.dependency() is sentinel
