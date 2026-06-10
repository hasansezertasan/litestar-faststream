"""Exception-hierarchy contract: backward-compat + discriminability.

Every error raised by litestar-faststream must still be catchable as the
upstream ``ImproperlyConfiguredException`` (existing user code depends on
that), AND callers must be able to catch a *specific* subclass without
parsing message strings.
"""

import pytest
from litestar.exceptions import ImproperlyConfiguredException

from litestar_faststream import (
    BrokerConfigurationError,
    BrokerNotRegisteredError,
    DuplicateBrokerNameError,
    FastStreamPluginError,
    HandlerDiscoveryError,
    MarkerConfigurationError,
)


def test_root_inherits_from_litestar_improperly_configured() -> None:
    assert issubclass(FastStreamPluginError, ImproperlyConfiguredException)


def test_all_subclasses_inherit_from_root() -> None:
    for cls in (
        BrokerConfigurationError,
        DuplicateBrokerNameError,
        BrokerNotRegisteredError,
        HandlerDiscoveryError,
        MarkerConfigurationError,
    ):
        assert issubclass(cls, FastStreamPluginError), cls.__name__


def test_broker_subgroup_hierarchy() -> None:
    assert issubclass(DuplicateBrokerNameError, BrokerConfigurationError)
    assert issubclass(BrokerNotRegisteredError, BrokerConfigurationError)


def test_marker_inherits_from_discovery() -> None:
    assert issubclass(MarkerConfigurationError, HandlerDiscoveryError)


def test_subclass_caught_by_root() -> None:
    with pytest.raises(FastStreamPluginError, match="duplicate"):
        raise DuplicateBrokerNameError("duplicate")  # noqa: EM101 - inline keeps PT012 happy


def test_subclass_caught_by_litestar_base() -> None:
    """Backward-compat: existing ``except ImproperlyConfiguredException`` works.

    Raises:
        BrokerNotRegisteredError: deliberately, to exercise the catch path.
    """
    with pytest.raises(ImproperlyConfiguredException, match="missing"):
        raise BrokerNotRegisteredError("missing")  # noqa: EM101 - inline keeps PT012 happy
