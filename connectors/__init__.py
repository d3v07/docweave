"""
DocWeave Connectors Package
===========================

This package contains data source connectors for DocWeave.
Connectors are responsible for ingesting documents from various
sources into the DocWeave processing pipeline.

Available Connectors:
    - FilesystemConnector: Reads documents from local filesystem

Usage:
    from connectors import FilesystemConnector

    connector = FilesystemConnector("/path/to/documents")
    async for doc in connector.scan():
        process(doc)
"""

from connectors.base import BaseConnector, ConnectorConfig
from connectors.filesystem.connector import FileSystemConnector

__all__ = [
    "BaseConnector",
    "ConnectorConfig",
    "FileSystemConnector",
]
