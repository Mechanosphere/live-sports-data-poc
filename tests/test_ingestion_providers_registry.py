from app.ingestion.providers import build_registered_providers, registered_provider_names


def test_built_in_providers_are_registered():
    assert registered_provider_names() == (
        "livescorecore",
        "sportsfeedx",
        "hoopstream",
    )


def test_build_registered_providers_returns_fresh_instances():
    first = build_registered_providers()
    second = build_registered_providers()

    assert [provider.name for provider in first] == list(registered_provider_names())
    assert [provider.name for provider in second] == list(registered_provider_names())
    assert all(a is not b for a, b in zip(first, second, strict=True))
