"""Legacy data catalogue interface.

This module originally contained a non-Python placeholder. It is now a small
abstract interface that can be imported safely and linted by our tooling.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class DataCatalogueHandler(ABC):
    """Abstract interface for a data catalogue handler."""

    platform_name: str = "Data Catalogue"

    @abstractmethod
    def ingestion(self) -> None:
        """Run ingestion workflow."""

    @abstractmethod
    def version(self) -> None:
        """Run versioning workflow."""

    @abstractmethod
    def enrich(self) -> None:
        """Run enrichment workflow."""


class Data_Catalogue_Handler(DataCatalogueHandler):
    """Backward-compatible alias for :class:`DataCatalogueHandler`."""
