"""Abstract base connector for data source integrations."""
from abc import ABC, abstractmethod
from typing import AsyncIterator, Optional
from datetime import datetime
from dataclasses import dataclass

from shared.models.document import Document, DocumentEvent


@dataclass
class ConnectorConfig:
    """Base configuration for connectors."""
    name: str
    poll_interval_seconds: int = 60
    batch_size: int = 100
    enabled: bool = True


class BaseConnector(ABC):
    """Abstract base class for all data source connectors."""

    def __init__(self, config: ConnectorConfig):
        self.config = config
        self._connected = False
        self._last_poll: Optional[datetime] = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the data source."""
        pass

    @abstractmethod
    async def poll_changes(self) -> AsyncIterator[DocumentEvent]:
        """Poll for new or modified documents since last check."""
        pass

    @abstractmethod
    async def fetch_document(self, doc_id: str) -> Document:
        """Fetch a specific document by ID."""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the connector is healthy and can reach its source."""
        pass

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
