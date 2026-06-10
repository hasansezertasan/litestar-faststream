def test_public_surface_exports() -> None:
    import litestar_faststream as mod

    expected = {
        "BrokerConfig",
        "subscriber",
        "publisher",
    }
    assert expected.issubset(set(mod.__all__))
    for name in expected:
        assert hasattr(mod, name)
