"""Unit tests for ``build_asyncapi_controller``.

The controller accepts two shapes:

  * an ``AsyncAPI``-factory shape exposing ``to_specification()`` (production
    path; ``Specification.to_json/to_yaml`` is invoked per request);
  * a duck-typed stub exposing ``to_json``/``to_yaml`` directly (test path).

These tests pin both shapes so the dual surface stays in place; see the
docstring on ``build_asyncapi_controller`` for the rationale.
"""

from litestar_faststream.asyncapi import build_asyncapi_controller


class _FakeSchema:
    """Duck-typed stub exposing the minimum schema surface."""

    title = "T"
    description = "D"
    version = "1.0"

    def to_jsonable(self) -> dict:
        return {"asyncapi": "3.0.0"}

    def to_json(self) -> str:
        return '{"asyncapi":"3.0.0"}'

    def to_yaml(self) -> str:
        return "asyncapi: 3.0.0\n"


class _FakeFactory:
    """Factory-shaped stub: ``to_specification()`` returns a Specification stub."""

    def to_specification(self) -> _FakeSchema:
        return _FakeSchema()


def test_returns_none_when_url_disabled() -> None:
    assert build_asyncapi_controller(schema=_FakeSchema(), asyncapi_url=None) is None


def test_returns_controller_with_three_routes_duck_typed() -> None:
    import inspect

    ctrl = build_asyncapi_controller(schema=_FakeSchema(), asyncapi_url="/asyncapi")
    assert ctrl is not None
    # Litestar's `Controller.get_route_handlers` is an instance method requiring
    # a router owner. Inspect class-level members instead -- each `@get(...)`
    # leaves a route handler with a `paths` set on the class.
    paths: set[str] = set()
    for _name, member in inspect.getmembers(ctrl):
        member_paths = getattr(member, "paths", None)
        if member_paths:
            paths.update(member_paths)
    assert paths == {"/asyncapi", "/asyncapi.json", "/asyncapi.yaml"}


def test_returns_controller_with_three_routes_factory_shape() -> None:
    import inspect

    ctrl = build_asyncapi_controller(schema=_FakeFactory(), asyncapi_url="/asyncapi")
    assert ctrl is not None
    paths: set[str] = set()
    for _name, member in inspect.getmembers(ctrl):
        member_paths = getattr(member, "paths", None)
        if member_paths:
            paths.update(member_paths)
    assert paths == {"/asyncapi", "/asyncapi.json", "/asyncapi.yaml"}
