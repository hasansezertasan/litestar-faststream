from litestar_faststream.di import build_broker_provide


def test_build_broker_provide_resolves_to_instance() -> None:
    sentinel = object()
    provide = build_broker_provide(sentinel)
    assert provide.dependency() is sentinel


def test_brokers_missing_name_raises_with_registered_names() -> None:
    import pytest

    from litestar_faststream.di import Brokers
    from litestar_faststream.exceptions import BrokerNotRegisteredError

    registry = Brokers({})
    with pytest.raises(BrokerNotRegisteredError, match="No broker named 'nope'"):
        _ = registry["nope"]
