from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from scripts.bulk_update_dataset_slugs import bulk_update, load_changes


def test_load_changes_valid_csv(tmp_path):
    csv_path = tmp_path / "slug_changes.csv"

    csv_path.write_text(
        "dataset_id,old_slug,new_slug\n"
        "id-1,old-slug-1,new-slug-1\n"
        "id-2,old-slug-2,new-slug-2\n",
        encoding="utf-8",
    )

    result = load_changes(csv_path)

    assert len(result) == 2
    assert result[0]["dataset_id"] == "id-1"
    assert result[0]["new_slug"] == "new-slug-1"
    assert result[1]["dataset_id"] == "id-2"
    assert result[1]["new_slug"] == "new-slug-2"


def test_load_changes_rejects_duplicate_target_slug(tmp_path):
    csv_path = tmp_path / "slug_changes.csv"

    csv_path.write_text(
        "dataset_id,old_slug,new_slug\n"
        "id-1,old-slug-1,same-slug\n"
        "id-2,old-slug-2,same-slug\n",
        encoding="utf-8",
    )

    import pytest

    with pytest.raises(ValueError, match="Duplicate target slug"):
        load_changes(csv_path)


def test_dry_run_does_not_apply_updates(tmp_path):
    csv_path = tmp_path / "slug_changes.csv"

    csv_path.write_text(
        "dataset_id,old_slug,new_slug\n"
        "id-1,old-slug-1,new-slug-1\n"
        "id-2,old-slug-2,new-slug-2\n",
        encoding="utf-8",
    )

    datasets = {
        "id-1": SimpleNamespace(id="id-1", slug="old-slug-1"),
        "id-2": SimpleNamespace(id="id-2", slug="old-slug-2"),
    }

    interface = MagicMock()
    interface.get_dataset_by_slug.return_value = None

    with patch(
        "scripts.bulk_update_dataset_slugs.db.session.get",
        side_effect=lambda model, dataset_id: datasets.get(dataset_id),
    ), patch(
        "scripts.bulk_update_dataset_slugs.HarvesterDBInterface",
        return_value=interface,
    ):
        result = bulk_update(csv_path, apply=False)

    assert result == 0
    interface.update_dataset_slug.assert_not_called()


def test_invalid_batch_applies_no_changes(tmp_path):
    csv_path = tmp_path / "slug_changes.csv"

    csv_path.write_text(
        "dataset_id,old_slug,new_slug\n"
        "id-1,old-slug-1,new-slug-1\n"
        "id-2,wrong-old-slug,new-slug-2\n",
        encoding="utf-8",
    )

    datasets = {
        "id-1": SimpleNamespace(id="id-1", slug="old-slug-1"),
        "id-2": SimpleNamespace(id="id-2", slug="old-slug-2"),
    }

    interface = MagicMock()
    interface.get_dataset_by_slug.return_value = None

    with patch(
        "scripts.bulk_update_dataset_slugs.db.session.get",
        side_effect=lambda model, dataset_id: datasets.get(dataset_id),
    ), patch(
        "scripts.bulk_update_dataset_slugs.HarvesterDBInterface",
        return_value=interface,
    ):
        result = bulk_update(csv_path, apply=True)

    assert result == 1
    interface.update_dataset_slug.assert_not_called()


def test_apply_updates_all_valid_datasets(tmp_path):
    csv_path = tmp_path / "slug_changes.csv"

    csv_path.write_text(
        "dataset_id,old_slug,new_slug\n"
        "id-1,old-slug-1,new-slug-1\n"
        "id-2,old-slug-2,new-slug-2\n",
        encoding="utf-8",
    )

    datasets = {
        "id-1": SimpleNamespace(id="id-1", slug="old-slug-1"),
        "id-2": SimpleNamespace(id="id-2", slug="old-slug-2"),
    }

    interface = MagicMock()
    interface.get_dataset_by_slug.return_value = None
    interface.update_dataset_slug.side_effect = [
        (SimpleNamespace(slug="new-slug-1"), True, None),
        (SimpleNamespace(slug="new-slug-2"), True, None),
    ]

    with patch(
        "scripts.bulk_update_dataset_slugs.db.session.get",
        side_effect=lambda model, dataset_id: datasets.get(dataset_id),
    ), patch(
        "scripts.bulk_update_dataset_slugs.HarvesterDBInterface",
        return_value=interface,
    ):
        result = bulk_update(csv_path, apply=True)

    assert result == 0
    assert interface.update_dataset_slug.call_count == 2
    interface.update_dataset_slug.assert_any_call("id-1", "new-slug-1")
    interface.update_dataset_slug.assert_any_call("id-2", "new-slug-2")
