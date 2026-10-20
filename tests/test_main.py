"""Tests for the package's runnable entrypoint (``python -m litestar_faststream``)."""

from __future__ import annotations

import importlib

import pytest


def test_main_is_callable() -> None:
    """The package exposes a callable ``main()`` entrypoint.

    Every standalone-executable build (launcher / freezer / compiler)
    targets ``litestar_faststream.__main__:main``, so this pins the
    contract that the symbol exists and is callable for whichever component is
    enabled, and that the active branch's component import actually resolves.
    The module is imported (not executed as ``__main__``), so a runnable
    component's blocking ``main()`` (server loop, mainloop, ...) is never invoked.
    """
    main_module = importlib.import_module("litestar_faststream.__main__")

    assert callable(main_module.main)


def test_main_exits_loudly_when_no_component() -> None:
    """With no runnable component, ``main()`` exits non-zero instead of no-op-ing.

    This combination (a standalone build with nothing to run) is degenerate but
    allowed; ``main()`` must fail loudly so the produced binary does not look
    like a silently broken executable. Safe to invoke here because there is no
    blocking component behind it.
    """
    main_module = importlib.import_module("litestar_faststream.__main__")

    with pytest.raises(SystemExit):
        main_module.main()
