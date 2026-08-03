import json
import os
from datetime import date, datetime
from typing import Any

from database.models import Dataset
from search.config import DEFAULT_CATALOG_BASE_URL, INDEX_NAME
from search.spatial import calc_geometry_centroid
from search.transforms import DcatIndexTransformer


class DatasetDocument:
    DCAT_INDEX_TRANSFORMER = DcatIndexTransformer()

    def __init__(self, dataset: Dataset):
        self.dataset = dataset
        self.INDEX_NAME = INDEX_NAME

    @staticmethod
    def _serialize_dcat_value(value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True)
        return str(value)

    @classmethod
    def _normalize_dcat_metadata_value(cls, value: Any) -> Any:
        # stringify nested objects/lists because
        # OpenSearch expect those fields to be text.
        if isinstance(value, dict):
            return {
                field: (
                    cls._serialize_dcat_value(field_value)
                    if isinstance(field_value, (dict, list))
                    else field_value
                )
                for field, field_value in value.items()
            }
        if isinstance(value, list):
            return [
                (
                    cls._normalize_dcat_metadata_value(item)
                    if isinstance(item, dict)
                    else (
                        cls._serialize_dcat_value(item)
                        if isinstance(item, list)
                        else item
                    )
                )
                for item in value
            ]
        return value

    @classmethod
    def _normalize_dcat_dates(cls, dcat: dict) -> dict:
        """Normalize DCAT values for OpenSearch metadata indexing."""
        normalized_dcat = dcat.copy()
        date_fields = ["modified", "issued", "temporal"]
        for field in date_fields:
            if field in normalized_dcat:
                value = normalized_dcat[field]
                if isinstance(value, (datetime, date)):
                    normalized_dcat[field] = value.isoformat()
                elif value is not None and not isinstance(value, str):
                    normalized_dcat[field] = str(value)
        spatial = normalized_dcat.get("spatial")
        if spatial is not None and not isinstance(spatial, str):
            normalized_dcat["spatial"] = cls._serialize_dcat_value(spatial)

        for field, value in normalized_dcat.items():
            if field in date_fields or field in {"publisher", "spatial"}:
                continue
            normalized_dcat[field] = cls._normalize_dcat_metadata_value(value)
        return normalized_dcat

    def dataset_to_document(self) -> dict:
        """Map a Dataset ORM row into an OpenSearch index document.

        Top-level search fields come from ``DCAT_INDEX_TRANSFORMER``
        (``index_fields``); nested ``dcat`` keeps original DCAT shapes
        (``nested_dcat``), with only date/metadata normalization applied.
        """
        dataset = self.dataset
        # Flat top-level search fields; nested_dcat keeps original DCAT shapes.
        index_fields = self.DCAT_INDEX_TRANSFORMER.transform(dataset.dcat)

        spatial_value = dataset.dcat.get("spatial")
        has_spatial_theme = any(
            label.strip().lower() == "geospatial" for label in index_fields["theme"]
        )
        has_spatial = (
            bool(spatial_value and str(spatial_value).strip())
            or dataset.translated_spatial is not None
            or has_spatial_theme
        )
        nested_dcat = self._normalize_dcat_dates(dataset.dcat)
        spatial_centroid = calc_geometry_centroid(dataset.translated_spatial)
        last_harvested = (
            dataset.last_harvested_date.isoformat()
            if dataset.last_harvested_date
            else None
        )
        organization = dataset.organization.to_dict() if dataset.organization else {}

        popularity = dataset.popularity if dataset.popularity is not None else None

        document = {
            "_index": self.INDEX_NAME,
            "_id": dataset.id,
            "title": index_fields["title"],
            "slug": dataset.slug,
            "last_harvested_date": last_harvested,
            "description": index_fields["description"],
            "publisher": index_fields["publisher"],
            "dcat": nested_dcat,
            "keyword": index_fields["keyword"],
            "theme": index_fields["theme"],
            "identifier": index_fields["identifier"],
            "has_spatial": has_spatial,
            "organization": organization,
            "distribution_titles": index_fields["distribution_titles"],
            "popularity": popularity,
            "spatial_shape": dataset.translated_spatial,
            "spatial_centroid": spatial_centroid,
            "harvest_record": self._create_harvest_record_url(dataset),
            "harvest_record_raw": self._create_harvest_record_raw_url(dataset),
        }
        if self._has_harvest_record_transformed(dataset):
            document["harvest_record_transformed"] = (
                self._create_harvest_record_transformed_url(dataset)
            )
        return document

    @staticmethod
    def _catalog_base_url() -> str:
        return os.getenv("CATALOG_BASE_URL", DEFAULT_CATALOG_BASE_URL).rstrip("/")

    @staticmethod
    def _create_harvest_record_url(dataset) -> str | None:
        if not getattr(dataset, "harvest_record_id", None):
            return None
        return (
            f"{DatasetDocument._catalog_base_url()}/harvest_record/"
            f"{dataset.harvest_record_id}"
        )

    @staticmethod
    def _create_harvest_record_raw_url(dataset) -> str | None:
        if not getattr(dataset, "harvest_record_id", None):
            return None
        return (
            f"{DatasetDocument._catalog_base_url()}/harvest_record/"
            f"{dataset.harvest_record_id}/raw"
        )

    @staticmethod
    def _create_harvest_record_transformed_url(dataset) -> str | None:
        if not getattr(dataset, "harvest_record_id", None):
            return None
        return (
            f"{DatasetDocument._catalog_base_url()}/harvest_record/"
            f"{dataset.harvest_record_id}/transformed"
        )

    @staticmethod
    def _has_harvest_record_transformed(dataset) -> bool:
        record = getattr(dataset, "harvest_record", None)
        if record is None:
            return False

        transformed = getattr(record, "source_transform", None)
        if transformed is None:
            return False

        if isinstance(transformed, str) and not transformed.strip():
            return False

        return True
