"""Exception hierarchy for litestar-faststream.

All errors inherit from :class:`litestar.exceptions.ImproperlyConfiguredException`
so existing ``except ImproperlyConfiguredException`` blocks keep working;
specific subclasses let callers (and tests) discriminate failure modes
without parsing message strings.

Hierarchy::

    ImproperlyConfiguredException                  (litestar)
      FastStreamPluginError                        (base for this package)
        BrokerConfigurationError                   (FastStreamConfig / host)
          DuplicateBrokerNameError
          BrokerNotRegisteredError
        HandlerDiscoveryError                      (discovery.collect / markers)
          MarkerConfigurationError
"""

from litestar.exceptions import ImproperlyConfiguredException

__all__ = (
    "BrokerConfigurationError",
    "BrokerNotRegisteredError",
    "DuplicateBrokerNameError",
    "FastStreamPluginError",
    "HandlerDiscoveryError",
    "MarkerConfigurationError",
)


class FastStreamPluginError(ImproperlyConfiguredException):
    """Base class for all litestar-faststream configuration errors."""


class BrokerConfigurationError(FastStreamPluginError):
    """FastStreamConfig / BrokerConfig setup is invalid."""


class DuplicateBrokerNameError(BrokerConfigurationError):
    """Two BrokerConfig entries share the same ``name``."""


class BrokerNotRegisteredError(BrokerConfigurationError):
    """A broker was referenced by name but no BrokerConfig owns that name."""


class HandlerDiscoveryError(FastStreamPluginError):
    """A handler or extra_handler could not be discovered/registered."""


class MarkerConfigurationError(HandlerDiscoveryError):
    """An ``@subscriber`` / ``@publisher`` marker entry is malformed."""
