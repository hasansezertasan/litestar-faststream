from faststream import Context, Logger


def test_logger_is_annotated_alias() -> None:
    import typing

    assert typing.get_origin(Logger) is not None
    args = typing.get_args(Logger)
    # FastStream's native Context dataclass exposes the resolved name as `name`
    # (the constructor accepts `real_name` but stores it as `self.name`).
    assert any(getattr(a, "name", None) == "logger" for a in args)


def test_context_re_export() -> None:
    assert callable(Context)
