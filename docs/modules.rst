.. A 7-character "=" underline (the length of "Modules") is treated as a
   merge-conflict separator by ``git diff --check`` / ``check-merge-conflict``;
   keep this underline longer than the title to avoid the false positive.

Modules
=========

An overview of the packages that make up ``litestar_faststream``.
The API reference below is generated automatically from the source docstrings.

Plugin (``litestar_faststream.plugin``)
-------------------------------------------

Host, configuration, and lifespan wiring.

.. automodule:: litestar_faststream.plugin

.. automodule:: litestar_faststream.host

.. automodule:: litestar_faststream.lifespan

.. automodule:: litestar_faststream.hooks

Handlers (``litestar_faststream.decorators``)
---------------------------------------------

Free ``@subscriber`` / ``@publisher`` markers and discovery.

.. automodule:: litestar_faststream.decorators

.. automodule:: litestar_faststream.discovery

Dependency injection (``litestar_faststream.di``)
-------------------------------------------------

Litestar DI bridge and broker registry.

.. automodule:: litestar_faststream.di

AsyncAPI (``litestar_faststream.asyncapi``)
-------------------------------------------

Combined AsyncAPI document rendering.

.. automodule:: litestar_faststream.asyncapi

Observability (``litestar_faststream.instrumentation``)
-------------------------------------------------------

OpenTelemetry middleware and helpers.

.. automodule:: litestar_faststream.instrumentation

CLI and info (``litestar_faststream.cli``)
-------------------------------------------

``litestar faststream`` subcommands and introspection.

.. automodule:: litestar_faststream.cli

.. automodule:: litestar_faststream.info

Support (``litestar_faststream.exceptions``)
--------------------------------------------

Errors and shared helpers.

.. automodule:: litestar_faststream.exceptions
