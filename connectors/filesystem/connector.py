"""FileSystem connector implementation for watching local directories."""
import os
import hashlib
import asyncio
from pathlib import Path
from typing import AsyncIterator, Dict, Set
from datetime import datetime
from dataclasses import dataclass, field

from connectors.base import BaseConnector, ConnectorConfig
from shared.models.document import Document, DocumentEventModel as DocumentEvent, EventType


@dataclass
class FileSystemConfig(ConnectorConfig):
    """Configuration for filesystem connector."""
    watch_paths: list[str] = field(default_factory=list)
    file_extensions: list[str] = field(default_factory=lambda: [".pdf", ".txt", ".md", ".html", ".json"])
    recursive: bool = True
    ignore_patterns: list[str] = field(default_factory=lambda: [".*", "__pycache__", "node_modules"])


class FileSystemConnector(BaseConnector):
    """Connector for watching and ingesting files from local filesystem."""

    def __init__(self, config: FileSystemConfig):
        super().__init__(config)
        self.config: FileSystemConfig = config
        self._file_hashes: Dict[str, str] = {}
        self._known_files: Set[str] = set()

    async def connect(self) -> None:
        """Initialize filesystem watching."""
        for path in self.config.watch_paths:
            if not os.path.exists(path):
                raise ValueError(f"Watch path does not exist: {path}")
        self._connected = True
        await self._scan_initial_files()

    async def disconnect(self) -> None:
        """Cleanup filesystem connector."""
        self._connected = False
        self._file_hashes.clear()
        self._known_files.clear()

    async def _scan_initial_files(self) -> None:
        """Scan and hash all existing files."""
        for watch_path in self.config.watch_paths:
            for file_path in self._iterate_files(watch_path):
                file_hash = await self._compute_hash(file_path)
                self._file_hashes[file_path] = file_hash
                self._known_files.add(file_path)

    def _iterate_files(self, root_path: str) -> AsyncIterator[str]:
        """Iterate over files matching configured extensions."""
        root = Path(root_path)
        pattern = "**/*" if self.config.recursive else "*"

        for path in root.glob(pattern):
            if not path.is_file():
                continue
            if path.suffix.lower() not in self.config.file_extensions:
                continue
            if any(part.startswith('.') for part in path.parts):
                continue
            yield str(path)

    async def _compute_hash(self, file_path: str) -> str:
        """Compute SHA256 hash of file content."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    async def poll_changes(self) -> AsyncIterator[DocumentEvent]:
        """Poll for file changes since last check."""
        current_files: Set[str] = set()

        for watch_path in self.config.watch_paths:
            for file_path in self._iterate_files(watch_path):
                current_files.add(file_path)
                current_hash = await self._compute_hash(file_path)

                if file_path not in self._known_files:
                    # New file
                    self._file_hashes[file_path] = current_hash
                    self._known_files.add(file_path)
                    yield DocumentEvent(
                        event_type=EventType.CREATED,
                        document_id=file_path,
                        timestamp=datetime.utcnow(),
                        source_connector=self.config.name,
                        metadata={"hash": current_hash}
                    )
                elif self._file_hashes.get(file_path) != current_hash:
                    # Modified file
                    self._file_hashes[file_path] = current_hash
                    yield DocumentEvent(
                        event_type=EventType.MODIFIED,
                        document_id=file_path,
                        timestamp=datetime.utcnow(),
                        source_connector=self.config.name,
                        metadata={"hash": current_hash, "previous_hash": self._file_hashes.get(file_path)}
                    )

        # Check for deleted files
        deleted = self._known_files - current_files
        for file_path in deleted:
            self._known_files.discard(file_path)
            self._file_hashes.pop(file_path, None)
            yield DocumentEvent(
                event_type=EventType.DELETED,
                document_id=file_path,
                timestamp=datetime.utcnow(),
                source_connector=self.config.name
            )

        self._last_poll = datetime.utcnow()

    async def fetch_document(self, doc_id: str) -> dict:
        """Fetch document content from filesystem."""
        file_path = Path(doc_id)
        if not file_path.exists():
            raise FileNotFoundError(f"Document not found: {doc_id}")

        stat = file_path.stat()
        content = file_path.read_text(errors="replace")

        return {
            "id": doc_id,
            "filename": file_path.name,
            "content_type": self._get_content_type(file_path.suffix),
            "raw_text": content,
            "metadata": {
                "path": str(file_path.absolute()),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
            }
        }

    def _get_content_type(self, suffix: str) -> str:
        """Map file extension to content type."""
        mapping = {
            ".pdf": "application/pdf",
            ".txt": "text/plain",
            ".md": "text/markdown",
            ".html": "text/html",
            ".json": "application/json",
        }
        return mapping.get(suffix.lower(), "application/octet-stream")

    async def health_check(self) -> bool:
        """Check if all watch paths are accessible."""
        for path in self.config.watch_paths:
            if not os.path.exists(path) or not os.access(path, os.R_OK):
                return False
        return True
