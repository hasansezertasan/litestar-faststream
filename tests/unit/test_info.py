from unittest.mock import MagicMock

from litestar_faststream.info import render_plugin_info


def test_renders_subscribers_and_publishers() -> None:
    plugin = MagicMock()
    plugin.name = "rabbit"
    plugin._registered_subscribers = [
        ("OrdersController.on_order", ("orders.new",), {}),
        ("process_email", ("send-email",), {}),
    ]
    plugin._registered_response_publishers = [
        ("OrdersController.on_order", ("orders.processed",), {})
    ]

    output = render_plugin_info(plugin)
    assert "Broker: rabbit" in output
    assert "Subscribers:" in output
    assert "orders.new" in output
    assert "-> OrdersController.on_order" in output
    assert "Publishers:" in output
    assert "orders.processed" in output


def test_renders_empty_plugin() -> None:
    plugin = MagicMock()
    plugin.name = "empty"
    plugin._registered_subscribers = []
    plugin._registered_response_publishers = []

    output = render_plugin_info(plugin)
    assert output == "Broker: empty\n"


def test_target_label_prefers_name_attribute() -> None:
    from types import SimpleNamespace

    from litestar_faststream.info import _target_label

    assert _target_label(SimpleNamespace(name="orders")) == "orders"
    assert _target_label(SimpleNamespace(name=3)) == "namespace(name=3)"
    assert _target_label("plain") == "plain"
