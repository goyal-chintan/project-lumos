from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Dict, Optional

from core.common.config_manager import ConfigManager
from core.platform.interface import MetadataPlatformInterface
from datahub.emitter.mce_builder import make_assertion_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionTypeClass,
    AuditStampClass,
    CustomAssertionInfoClass,
)

logger = logging.getLogger(__name__)


class AssertionService:
    """
    Service for creating Data Quality assertions and (optionally) emitting run events to DataHub.

    This is intentionally lightweight and uses Custom Assertions by default so it can support a
    wide range of assertion "types" without hard-coding a specific DQ engine (GE, Soda, dbt, etc).
    """

    def __init__(self, platform_handler: MetadataPlatformInterface, config_manager: ConfigManager):
        self.platform_handler = platform_handler
        self.config_manager = config_manager

    @staticmethod
    def _default_assertion_id(dataset_urn: str, assertion_type: str) -> str:
        """
        Produce a deterministic, DataHub-URN-safe assertion id.

        DataHub URN format: urn:li:assertion:<assertion_id>
        """
        if not dataset_urn or not isinstance(dataset_urn, str):
            raise ValueError("dataset_urn must be a non-empty string")
        if not assertion_type or not isinstance(assertion_type, str):
            raise ValueError("assertion_type must be a non-empty string")

        safe_type = re.sub(r"[^a-zA-Z0-9_]+", "_", assertion_type.strip().lower()).strip("_") or "assertion"
        digest = hashlib.sha256(f"{dataset_urn}|{assertion_type}".encode("utf-8")).hexdigest()[:16]
        return f"lumos_{safe_type}_{digest}"

    def upsert_custom_assertion(
        self,
        *,
        dataset_urn: str,
        assertion_type: str,
        assertion_id: Optional[str] = None,
        description: Optional[str] = None,
        logic: Optional[str] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        source_type: str | AssertionSourceTypeClass = AssertionSourceTypeClass.EXTERNAL,
        actor_urn: str = "urn:li:corpuser:unknown",
        dry_run: bool = False,
    ) -> str:
        """
        Create/update a DataHub Custom Assertion definition.

        Returns:
            The assertion URN.
        """
        if not dataset_urn.startswith("urn:li:dataset:"):
            raise ValueError(f"dataset_urn must be a dataset URN, got: {dataset_urn}")

        assertion_id = assertion_id or self._default_assertion_id(dataset_urn, assertion_type)
        assertion_urn = make_assertion_urn(assertion_id)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        source = AssertionSourceClass(type=source_type, created=AuditStampClass(time=now_ms, actor=actor_urn))

        info = AssertionInfoClass(
            type=AssertionTypeClass.CUSTOM,
            customProperties=custom_properties,
            externalUrl=external_url,
            customAssertion=CustomAssertionInfoClass(
                type=assertion_type,
                entity=dataset_urn,
                logic=logic,
            ),
            source=source,
            description=description,
        )

        mcp = MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=info)

        test_mode = bool(getattr(self.platform_handler, "test_mode", False))
        if dry_run or test_mode:
            logger.info(
                "DQ assertion dry-run/test-mode: would emit AssertionInfo",
                extra={"assertion_urn": assertion_urn, "dataset_urn": dataset_urn, "assertion_type": assertion_type},
            )
            return assertion_urn

        self.platform_handler.emit_mcp(mcp)
        logger.info("✅ Upserted custom assertion", extra={"assertion_urn": assertion_urn})
        return assertion_urn

    @staticmethod
    def _utc_stamp() -> str:
        # Proper ISO-8601 UTC with milliseconds (NOT epoch): 2025-12-01T20:33:00.103Z
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def emit_run_event(
        self,
        *,
        assertion_urn: str,
        dataset_urn: str,
        result_type: str | AssertionResultTypeClass = AssertionResultTypeClass.SUCCESS,
        run_id: Optional[str] = None,
        external_url: Optional[str] = None,
        runtime_context: Optional[Dict[str, str]] = None,
        dry_run: bool = False,
    ) -> None:
        """
        Emit a single Assertion Run Event (timeseries aspect) to DataHub.
        """
        if not assertion_urn.startswith("urn:li:assertion:"):
            raise ValueError(f"assertion_urn must be an assertion URN, got: {assertion_urn}")
        if not dataset_urn.startswith("urn:li:dataset:"):
            raise ValueError(f"dataset_urn must be a dataset URN, got: {dataset_urn}")

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        run_id = run_id or f"lumos-run-{self._utc_stamp()}"

        event = AssertionRunEventClass(
            timestampMillis=now_ms,
            runId=run_id,
            asserteeUrn=dataset_urn,
            status=AssertionRunStatusClass.COMPLETE,
            assertionUrn=assertion_urn,
            result=AssertionResultClass(type=result_type, externalUrl=external_url),
            runtimeContext=runtime_context,
        )

        mcp = MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=event)

        test_mode = bool(getattr(self.platform_handler, "test_mode", False))
        if dry_run or test_mode:
            logger.info(
                "DQ run-event dry-run/test-mode: would emit AssertionRunEvent",
                extra={"assertion_urn": assertion_urn, "dataset_urn": dataset_urn, "run_id": run_id},
            )
            return

        self.platform_handler.emit_mcp(mcp)
        logger.info("✅ Emitted assertion run event", extra={"assertion_urn": assertion_urn, "run_id": run_id})

    def assert_quality(
        self,
        dataset_urn: str,
        assertion: str,
        *,
        assertion_id: Optional[str] = None,
        description: Optional[str] = None,
        logic: Optional[str] = None,
        emit_run_event: bool = True,
        result_type: str | AssertionResultTypeClass = AssertionResultTypeClass.SUCCESS,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        runtime_context: Optional[Dict[str, str]] = None,
        dry_run: bool = False,
    ) -> str:
        """
        High-level helper that (1) upserts a custom assertion definition and (2) emits a run event.

        Returns:
            The assertion URN.
        """
        assertion_urn = self.upsert_custom_assertion(
            dataset_urn=dataset_urn,
            assertion_type=assertion,
            assertion_id=assertion_id,
            description=description,
            logic=logic,
            external_url=external_url,
            custom_properties=custom_properties,
            dry_run=dry_run,
        )

        if emit_run_event:
            self.emit_run_event(
                assertion_urn=assertion_urn,
                dataset_urn=dataset_urn,
                result_type=result_type,
                external_url=external_url,
                runtime_context=runtime_context,
                dry_run=dry_run,
            )

        return assertion_urn