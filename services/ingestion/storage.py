"""Document storage and deduplication for ingestion service."""
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict


@dataclass
class StoredDocument:
    """Metadata for a stored document."""
    id: str
    filename: str
    content_type: str
    content_hash: str
    size_bytes: int
    storage_path: str
    created_at: str
    source_connector: str
    metadata: Dict[str, Any]


class DocumentStorage:
    """
    Handles document storage with deduplication.

    Documents are stored in a flat structure with hash-based naming
    to enable content-addressable storage and deduplication.
    """

    def __init__(self, base_path: str = "/app/data/documents"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.metadata_path = self.base_path / "metadata"
        self.metadata_path.mkdir(exist_ok=True)
        self.content_path = self.base_path / "content"
        self.content_path.mkdir(exist_ok=True)
        self._hash_index: Dict[str, str] = {}
        self._load_hash_index()

    def _load_hash_index(self) -> None:
        """Load existing document hashes for deduplication."""
        index_file = self.metadata_path / "hash_index.json"
        if index_file.exists():
            with open(index_file, "r") as f:
                self._hash_index = json.load(f)

    def _save_hash_index(self) -> None:
        """Persist hash index to disk."""
        index_file = self.metadata_path / "hash_index.json"
        with open(index_file, "w") as f:
            json.dump(self._hash_index, f)

    def compute_hash(self, content: bytes) -> str:
        """Compute SHA256 hash of content."""
        return hashlib.sha256(content).hexdigest()

    def is_duplicate(self, content_hash: str) -> Optional[str]:
        """
        Check if content already exists.

        Returns document ID if duplicate, None otherwise.
        """
        return self._hash_index.get(content_hash)

    def store(
        self,
        content: bytes,
        filename: str,
        content_type: str,
        source_connector: str = "api",
        metadata: Optional[Dict[str, Any]] = None
    ) -> tuple[StoredDocument, bool]:
        """
        Store document with deduplication.

        Returns:
            Tuple of (StoredDocument, is_new) where is_new indicates
            if this is new content or a duplicate reference.
        """
        content_hash = self.compute_hash(content)

        # Check for duplicate
        existing_id = self.is_duplicate(content_hash)
        if existing_id:
            # Return existing document metadata
            existing_meta = self._load_metadata(existing_id)
            if existing_meta:
                return existing_meta, False

        # Generate document ID
        doc_id = f"doc_{content_hash[:16]}"

        # Store content
        content_file = self.content_path / f"{doc_id}.bin"
        with open(content_file, "wb") as f:
            f.write(content)

        # Create metadata
        doc = StoredDocument(
            id=doc_id,
            filename=filename,
            content_type=content_type,
            content_hash=content_hash,
            size_bytes=len(content),
            storage_path=str(content_file),
            created_at=datetime.utcnow().isoformat(),
            source_connector=source_connector,
            metadata=metadata or {}
        )

        # Store metadata
        self._save_metadata(doc)

        # Update hash index
        self._hash_index[content_hash] = doc_id
        self._save_hash_index()

        return doc, True

    def _save_metadata(self, doc: StoredDocument) -> None:
        """Save document metadata to disk."""
        meta_file = self.metadata_path / f"{doc.id}.json"
        with open(meta_file, "w") as f:
            json.dump(asdict(doc), f, indent=2)

    def _load_metadata(self, doc_id: str) -> Optional[StoredDocument]:
        """Load document metadata from disk."""
        meta_file = self.metadata_path / f"{doc_id}.json"
        if not meta_file.exists():
            return None
        with open(meta_file, "r") as f:
            data = json.load(f)
            return StoredDocument(**data)

    def get(self, doc_id: str) -> Optional[tuple[StoredDocument, bytes]]:
        """Retrieve document metadata and content."""
        meta = self._load_metadata(doc_id)
        if not meta:
            return None

        content_file = Path(meta.storage_path)
        if not content_file.exists():
            return None

        with open(content_file, "rb") as f:
            content = f.read()

        return meta, content

    def delete(self, doc_id: str) -> bool:
        """Delete a document and its metadata."""
        meta = self._load_metadata(doc_id)
        if not meta:
            return False

        # Remove from hash index
        if meta.content_hash in self._hash_index:
            del self._hash_index[meta.content_hash]
            self._save_hash_index()

        # Remove files
        content_file = Path(meta.storage_path)
        meta_file = self.metadata_path / f"{doc_id}.json"

        if content_file.exists():
            content_file.unlink()
        if meta_file.exists():
            meta_file.unlink()

        return True

    def list_documents(self, limit: int = 100, offset: int = 0) -> list[StoredDocument]:
        """List stored documents with pagination."""
        docs = []
        meta_files = sorted(self.metadata_path.glob("doc_*.json"))

        for meta_file in meta_files[offset:offset + limit]:
            doc_id = meta_file.stem
            meta = self._load_metadata(doc_id)
            if meta:
                docs.append(meta)

        return docs
