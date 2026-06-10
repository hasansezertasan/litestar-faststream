"""Mount AsyncAPI HTML viewer + JSON/YAML schema endpoints as a Litestar Controller."""

from typing import Any

from faststream.specification.asyncapi.site import get_asyncapi_html
from litestar import Controller, MediaType, Response, get


def build_asyncapi_controller(
    *,
    schema: Any,
    asyncapi_url: str | None,
    include_in_schema: bool = False,
) -> type[Controller] | None:
    """Build a Litestar Controller that serves AsyncAPI artefacts.

    ``schema`` may be either:

    - an ``AsyncAPI`` factory (production path) — ``to_specification()`` is
      resolved fresh per request so subscribers added during ``on_app_init``
      are reflected in the rendered output;
    - or any object exposing ``to_json``/``to_yaml`` (and optionally
      ``to_specification`` / ``to_jsonable``). The duck-typed surface is
      retained deliberately so unit tests can supply a lightweight stub
      without standing up a full ``AsyncAPI`` factory + ``Specification``
      pair (see ``tests/unit/test_asyncapi.py``).

    For the HTML viewer we prefer ``schema.to_specification()`` because
    ``get_asyncapi_html`` requires the full Specification object. If neither
    ``to_specification`` nor a Specification-shaped object is available, we
    fall back to ``schema`` as-is.

    ``include_in_schema`` controls whether the AsyncAPI routes appear in
    Litestar's OpenAPI schema; defaults to ``False`` because AsyncAPI
    HTML/JSON/YAML are meta-docs about the broker, not part of the HTTP API.
    Pass ``True`` to surface the URLs in the OpenAPI document.
    """
    if not asyncapi_url:
        return None

    json_url = f"{asyncapi_url}.json"
    yaml_url = f"{asyncapi_url}.yaml"

    def _resolve_specification() -> Any:
        # Prefer the ``AsyncAPI``-factory shape: ``to_specification()`` returns a
        # fresh ``Specification`` reflecting the broker's current subscriber
        # set. Fall back to ``schema`` as-is for test stubs that already expose
        # ``to_json``/``to_yaml`` directly.
        if hasattr(schema, "to_specification"):
            return schema.to_specification()
        return schema

    class AsyncAPIController(Controller):
        @get(asyncapi_url, include_in_schema=include_in_schema)
        async def html_view(self) -> Response[str]:
            html = get_asyncapi_html(schema=_resolve_specification())
            return Response(content=html, media_type=MediaType.HTML)

        @get(json_url, include_in_schema=include_in_schema)
        async def json_schema(self) -> Response[str]:
            spec = _resolve_specification()
            return Response(content=spec.to_json(), media_type=MediaType.JSON)

        @get(yaml_url, include_in_schema=include_in_schema)
        async def yaml_schema(self) -> Response[str]:
            spec = _resolve_specification()
            return Response(content=spec.to_yaml(), media_type="application/x-yaml")

    return AsyncAPIController
