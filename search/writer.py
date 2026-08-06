import logging
import time
from typing import Callable, TypeVar

from opensearchpy import helpers
from opensearchpy.exceptions import ConnectionTimeout

from search.client import OpenSearchClient
from search.config import (
    DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_REFRESH_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_TIMEOUT_BACKOFF_BASE,
    DEFAULT_TIMEOUT_RETRIES,
    INDEX_NAME,
)
from search.documents import DatasetDocument

logger = logging.getLogger(__name__)

T = TypeVar("T")

OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE = "failed to index in this batch"


class OpenSearchWriter:
    def __init__(self, opensearchclient: OpenSearchClient):
        self.wrapper_client = opensearchclient
        self.client = self.wrapper_client.client  # OpenSearch instance
        self.INDEX_NAME = INDEX_NAME

    def _run_with_timeout_retry(
        self,
        action: Callable[[], T],
        *,
        action_name: str,
        timeout_retries: int,
        timeout_backoff_base: float,
    ) -> T:
        attempt = 0

        while True:
            try:
                return action()
            except ConnectionTimeout as exc:
                attempt += 1
                if attempt > timeout_retries:
                    logger.error(
                        "%s timed out after %s retries; giving up.",
                        action_name,
                        timeout_retries,
                        exc_info=exc,
                    )
                    raise

                wait_seconds = min(timeout_backoff_base**attempt, 60)
                logger.warning(
                    "%s timed out (attempt %s/%s); retrying in %.1f seconds.",
                    action_name,
                    attempt,
                    timeout_retries,
                    wait_seconds,
                    exc_info=exc,
                )
                time.sleep(wait_seconds)

    def _refresh(
        self,
        timeout_retries: int = DEFAULT_TIMEOUT_RETRIES,
        timeout_backoff_base: float = DEFAULT_TIMEOUT_BACKOFF_BASE,
        request_timeout: int = DEFAULT_REFRESH_REQUEST_TIMEOUT_SECONDS,
    ):
        def _do_refresh():
            return self.client.indices.refresh(
                index=self.INDEX_NAME, request_timeout=request_timeout
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
        timeout_retries: int = DEFAULT_TIMEOUT_RETRIES,
        timeout_backoff_base: float = DEFAULT_TIMEOUT_BACKOFF_BASE,
    ):
        """Index an iterator of dataset objects into OpenSearch."""
        datasets = getattr(dataset_iter, "items", dataset_iter)
        documents = [
            DatasetDocument(dataset).dataset_to_document() for dataset in datasets
        ]

        def _stream_bulk():
            succeeded_local = 0
            failed_local = 0
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
                    succeeded_local += 1
                    if item["index"]["result"].lower() not in ["created", "updated"]:
                        if index_info:
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
                    failed_local += 1
                    if index_info and index_error:
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
            return succeeded_local, failed_local, errors

        succeeded, failed, errors = self._run_with_timeout_retry(
            _stream_bulk,
            action_name="OpenSearch bulk index",
            timeout_retries=timeout_retries,
            timeout_backoff_base=timeout_backoff_base,
        )

        if refresh_after:
            self._refresh()

        return (succeeded, failed, errors)

    def delete_dataset_by_id(
        self,
        dataset_id: str,
        refresh_after: bool = True,
        timeout_retries: int = DEFAULT_TIMEOUT_RETRIES,
        timeout_backoff_base: float = DEFAULT_TIMEOUT_BACKOFF_BASE,
    ) -> bool:
        if not dataset_id:
            return False

        def _do_delete():
            return self.client.delete(
                index=self.INDEX_NAME,
                id=dataset_id,
                ignore=[404],
                request_timeout=DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS,
            )

        self._run_with_timeout_retry(
            _do_delete,
            action_name="OpenSearch delete",
            timeout_retries=timeout_retries,
            timeout_backoff_base=timeout_backoff_base,
        )

        if refresh_after:
            self._refresh()

        return True

    def index_dataset_batches(
        self,
        dataset_ids: list[str],
        intro_message: str,
        db_interface,
        sample_size=None,
        log_all_errors=False,
    ):
        logging.info(intro_message)
        batch_size = 1000
        total_batches = (len(dataset_ids) + batch_size - 1) // batch_size
        total_indexed = 0
        total_skipped = 0

        for batch_number, offset in enumerate(
            range(0, len(dataset_ids), batch_size), start=1
        ):
            batch_ids = dataset_ids[offset : offset + batch_size]
            logging.info(
                f"  Batch {batch_number}/{total_batches}: "
                f"indexing {len(batch_ids)} dataset(s)..."
            )
            # Imported lazily so this module can be used without harvester's ORM.
            # index_datasets() below is duck-typed and is the only entry point
            # datagov-catalog uses (see docs/catalog-contract-test.md); a
            # module-level import would drag the whole model tree into its image.
            from database.models import Dataset

            datasets = (
                db_interface.db.query(Dataset).filter(Dataset.id.in_(batch_ids)).all()
            )
            found_ids = {dataset.id for dataset in datasets}
            skipped = [
                dataset_id for dataset_id in batch_ids if dataset_id not in found_ids
            ]
            total_skipped += len(skipped)

            if skipped:
                logging.info(
                    "    Warning: Skipping missing DB IDs: "
                    + ", ".join(skipped[:sample_size])
                )

            if not datasets:
                logging.info("    No datasets found for this batch; skipping.")
                continue

            succeeded, failed, errors = self.index_datasets(
                datasets, refresh_after=False
            )
            total_indexed += succeeded
            if failed:
                logging.info(
                    f"    Warning: {failed} dataset(s) "
                    f"{OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE}."
                )
                if log_all_errors:
                    for error in errors:
                        logging.info(error)

        logging.info(
            f"Indexed {total_indexed} datasets. "
            f"Skipped {total_skipped} missing DB rows."
        )
