# ruff: noqa: F401
from opensearchpy import helpers

from datagov_data_access.search.client import (
    OpenSearchClient as DataAccessOpenSearchClient,
)
from datagov_data_access.search.config import (
    DEFAULT_CLIENT_MAX_RETRIES,
    DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_REFRESH_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_TIMEOUT_BACKOFF_BASE,
    DEFAULT_TIMEOUT_RETRIES,
)
from datagov_data_access.search.documents import DatasetDocument
from datagov_data_access.search.reader import OpenSearchReader
from datagov_data_access.search.writer import (
    OpenSearchWriter as DataAccessOpenSearchWriter,
)


class OpenSearchClient(DataAccessOpenSearchClient):
    """Application OpenSearch client with physical-index management helpers."""

    def _index_definition(self) -> dict:
        body = {"mappings": self.MAPPINGS}
        if self.SETTINGS:
            body["settings"] = self.SETTINGS
        return body

    def create_index(self, index_name: str) -> None:
        """Create an empty physical index with the application schema."""
        if self.client.indices.exists(index=index_name):
            raise ValueError(f"OpenSearch index already exists: {index_name}")
        self.client.indices.create(index=index_name, body=self._index_definition())

    def index_count(self, index_name: str | None = None) -> int:
        """Return the number of documents in an index or alias."""
        response = self.client.count(index=index_name or self.INDEX_NAME)
        return int(response["count"])

    def alias_indices(self, alias_name: str | None = None) -> list[str]:
        """Return physical indexes currently attached to an alias."""
        alias_name = alias_name or self.INDEX_NAME
        if not self.client.indices.exists_alias(name=alias_name):
            return []
        return sorted(self.client.indices.get_alias(name=alias_name))

    def switch_alias(
        self,
        target_index: str,
        alias_name: str | None = None,
    ) -> tuple[list[str], bool]:
        """Atomically point the logical alias at a physical index."""
        alias_name = alias_name or self.INDEX_NAME
        if not self.client.indices.exists(index=target_index):
            raise ValueError(f"OpenSearch target index does not exist: {target_index}")

        old_indices = self.alias_indices(alias_name)
        if old_indices == [target_index]:
            return old_indices, False

        actions = [
            {"remove": {"index": index, "alias": alias_name}} for index in old_indices
        ]
        removed_legacy_index = False
        if not old_indices and self.client.indices.exists(index=alias_name):
            if alias_name == target_index:
                raise ValueError("Alias name and target index must be different")
            actions.append({"remove_index": {"index": alias_name}})
            removed_legacy_index = True

        actions.append(
            {
                "add": {
                    "index": target_index,
                    "alias": alias_name,
                    "is_write_index": True,
                }
            }
        )
        response = self.client.indices.update_aliases(body={"actions": actions})
        if not response.get("acknowledged"):
            raise RuntimeError("OpenSearch did not acknowledge the alias switch")
        return old_indices, removed_legacy_index


class OpenSearchWriter(DataAccessOpenSearchWriter):
    """OpenSearch writer that can target a new physical index during rebuilds."""

    def _refresh(
        self,
        timeout_retries: int = DEFAULT_TIMEOUT_RETRIES,
        timeout_backoff_base: float = DEFAULT_TIMEOUT_BACKOFF_BASE,
        request_timeout: int = DEFAULT_REFRESH_REQUEST_TIMEOUT_SECONDS,
        index_name: str | None = None,
    ):
        def _do_refresh():
            return self.client.indices.refresh(
                index=index_name or self.INDEX_NAME,
                request_timeout=request_timeout,
            )

        self._run_with_timeout_retry(
            _do_refresh,
            action_name="OpenSearch refresh",
            timeout_retries=timeout_retries,
            timeout_backoff_base=timeout_backoff_base,
        )

    def index_datasets(
        self,
        dataset_iter,
        refresh_after=True,
        index_name: str | None = None,
        timeout_retries: int = DEFAULT_TIMEOUT_RETRIES,
        timeout_backoff_base: float = DEFAULT_TIMEOUT_BACKOFF_BASE,
    ):
        if index_name is None:
            return super().index_datasets(
                dataset_iter,
                refresh_after=refresh_after,
                timeout_retries=timeout_retries,
                timeout_backoff_base=timeout_backoff_base,
            )

        datasets = getattr(dataset_iter, "items", dataset_iter)
        documents = [
            DatasetDocument(dataset).dataset_to_document() for dataset in datasets
        ]
        for document in documents:
            document["_index"] = index_name

        def _stream_bulk():
            succeeded = 0
            failed = 0
            errors = []
            for success, item in helpers.streaming_bulk(
                self.client,
                documents,
                raise_on_error=False,
                max_retries=8,
            ):
                index_info = item.get("index")
                index_error = index_info.get("error")
                if success:
                    succeeded += 1
                    if index_info["result"].lower() not in ["created", "updated"]:
                        errors.append(
                            {
                                "dataset_id": index_info.get("_id"),
                                "status_code": index_info["_shards"].get("status"),
                                "error_type": "Silent Error",
                                "error_reason": "Unknown",
                                "caused_by": index_info,
                            }
                        )
                else:
                    failed += 1
                    if index_error:
                        errors.append(
                            {
                                "dataset_id": index_info.get("_id"),
                                "status_code": index_info.get("status"),
                                "error_type": index_error.get("type"),
                                "error_reason": index_error.get("reason"),
                                "caused_by": index_error.get("caused_by"),
                            }
                        )
                    errors.append(item)
            return succeeded, failed, errors

        result = self._run_with_timeout_retry(
            _stream_bulk,
            action_name="OpenSearch bulk index",
            timeout_retries=timeout_retries,
            timeout_backoff_base=timeout_backoff_base,
        )
        if refresh_after:
            self._refresh(index_name=index_name)
        return result
