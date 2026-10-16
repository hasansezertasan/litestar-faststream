"""Module entrypoint for the project.

This is the single runnable entrypoint used by ``python -m litestar_faststream``
and by every standalone-executable build (PyCrucible launcher, PyInstaller
freezer, Nuitka compiler — see https://hasansezertasan.github.io/copier-pyproject/context/standalone-executables/#executables-are-named-per-tool-and-os-and-all-target-the-package-main-module).
The build tools all target this file,
so the component-selection logic lives here and nowhere else.

When a ``litestar_faststream`` console root exists (``include_console_root`` —
the CLI, or ≥2 components sharing a launcher; see https://hasansezertasan.github.io/copier-pyproject/context/components/#secondary-components-are-subcommands-of-one-console-root-not-separate-scripts),
``main()`` runs it,
which dispatches to the primary component by default and to each secondary via a
subcommand. Otherwise the single enabled component with the highest precedence —
CLI > GUI > TUI > web > MCP > worker — is wired to ``main()`` directly at
template-generation time (via the Jinja conditionals below). To change the
default entrypoint, re-render with a different component enabled or edit the
import/``main`` binding here directly. With no runnable component enabled,
``main()`` exits non-zero with an explanatory message.

Either binding routes its import through a loader that turns a missing
dependency into one actionable line instead of a traceback (https://hasansezertasan.github.io/copier-pyproject/context/components/#launcher-imports-are-guarded-with-exact-match-preflighted-dependency-errors).
This is
the boundary that needs it most: a standalone executable's user has no console
root to fall back on and no obvious way to read a Python stack trace.
"""

import sys


def main() -> None:
    """Fail loudly: no runnable application component is enabled.

    A standalone-executable build was produced for a project with no
    CLI/GUI/TUI/web/MCP/worker component, so there is nothing to run. Exit with
    a non-zero status and a message rather than a silent no-op, which would look
    like a broken binary to whoever launched it.
    """
    sys.exit("No runnable application component is enabled; nothing to run.")


__all__ = ["main"]


if __name__ == "__main__":
    main()
