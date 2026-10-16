"""Smoke tests for litestar_faststream package."""


def test_smoke() -> None:
    """Test that the package can be imported."""
    import litestar_faststream

    assert litestar_faststream is not None
