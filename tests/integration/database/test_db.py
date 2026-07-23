def test_update_dataset_slug_is_blocked_during_opensearch_maintenance(
    interface,
    slug_protection_dataset,
    monkeypatch,
):
    monkeypatch.setenv("HARVEST_RUNNER_MAX_TASKS", "0")

    dataset, synced, error = interface.update_dataset_slug(
        slug_protection_dataset.id,
        "maintenance-should-not-apply",
    )

    assert dataset is None
    assert synced is False
    assert "paused" in error
    assert slug_protection_dataset.slug != "maintenance-should-not-apply"
