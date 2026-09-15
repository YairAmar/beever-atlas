from types import SimpleNamespace

import pytest

from beever_atlas.services.unresolved_classifier import UnresolvedClassifier


@pytest.mark.asyncio
async def test_production_classifier_uses_configured_qa_assignment(monkeypatch):
    assignment = object()

    class Provider:
        async def resolve_for_call(self, consumer, stores):
            assert consumer == "qa_agent"
            assert stores is fake_stores
            return assignment

    async def dispatch(**kwargs):
        assert kwargs["assignment"] is assignment
        assert kwargs["response_format"] == {"type": "json_object"}
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content='{"classifications": [{"name": "Widget", "type": "Component", "confidence": 0.9}]}'
                    )
                )
            ]
        )

    fake_stores = object()
    monkeypatch.setattr("beever_atlas.llm.provider.get_llm_provider", lambda: Provider())
    monkeypatch.setattr("beever_atlas.services.llm_dispatch.dispatch_assignment", dispatch)

    classifier = UnresolvedClassifier(stores=fake_stores)
    result = await classifier._dispatch_batch([{"name": "Widget", "contexts": []}], set())
    assert [(item.name, item.type) for item in result] == [("Widget", "Component")]
