"""Litestar integration for FastStream message brokers."""

from litestar_faststream.decorators import publisher, subscriber
from litestar_faststream.di import Brokers
from litestar_faststream.exceptions import (
    BrokerConfigurationError,
    BrokerNotRegisteredError,
    DuplicateBrokerNameError,
    FastStreamPluginError,
    HandlerDiscoveryError,
    MarkerConfigurationError,
)
from litestar_faststream.hooks import (
    LoggingMiddleware,
    shutdown_banner,
    startup_banner,
)
from litestar_faststream.host import FastStreamConfig, FastStreamPlugin
from litestar_faststream.instrumentation import OtelMiddleware, monitored
from litestar_faststream.plugin import BrokerConfig

__all__ = (
    "BrokerConfig",
    "BrokerConfigurationError",
    "BrokerNotRegisteredError",
    "Brokers",
    "DuplicateBrokerNameError",
    "FastStreamConfig",
    "FastStreamPlugin",
    "FastStreamPluginError",
    "HandlerDiscoveryError",
    "LoggingMiddleware",
    "MarkerConfigurationError",
    "OtelMiddleware",
    "monitored",
    "publisher",
    "shutdown_banner",
    "startup_banner",
    "subscriber",
)
