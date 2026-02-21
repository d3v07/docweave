"""Base parser interface and common data structures."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import Enum


class BlockType(str, Enum):
    """Types of content blocks extracted from documents."""
    TITLE = "title"
    HEADING = "heading"
    PARAGRAPH = "paragraph"
    LIST = "list"
    LIST_ITEM = "list_item"
    TABLE = "table"
    CODE = "code"
    QUOTE = "quote"
    IMAGE = "image"
    LINK = "link"
    KEY_VALUE = "key_value"
    METADATA = "metadata"
    RAW = "raw"


@dataclass
class ContentBlock:
    """A single block of content extracted from a document."""
    type: BlockType
    content: str
    level: int = 0  # For headings, list nesting, etc.
    metadata: Dict[str, Any] = field(default_factory=dict)
    children: List["ContentBlock"] = field(default_factory=list)
    position: int = 0  # Character offset in original document
    page: Optional[int] = None  # Page number for paginated docs


@dataclass
class ParsedContent:
    """Result of parsing a document."""
    blocks: List[ContentBlock]
    metadata: Dict[str, Any] = field(default_factory=dict)
    raw_text: str = ""
    page_count: Optional[int] = None
    word_count: int = 0
    language: Optional[str] = None
    parse_errors: List[str] = field(default_factory=list)

    def get_text(self) -> str:
        """Get all text content concatenated."""
        texts = []
        for block in self.blocks:
            if block.content:
                texts.append(block.content)
            for child in block.children:
                if child.content:
                    texts.append(child.content)
        return "\n".join(texts)

    def get_blocks_by_type(self, block_type: BlockType) -> List[ContentBlock]:
        """Filter blocks by type."""
        return [b for b in self.blocks if b.type == block_type]


class BaseParser(ABC):
    """Abstract base class for document parsers."""

    @property
    @abstractmethod
    def supported_content_types(self) -> List[str]:
        """List of MIME types this parser can handle."""
        pass

    @abstractmethod
    def parse(self, content: bytes, filename: str = "") -> ParsedContent:
        """
        Parse document content into structured blocks.

        Args:
            content: Raw document bytes
            filename: Optional filename for format hints

        Returns:
            ParsedContent with extracted blocks and metadata
        """
        pass

    def can_parse(self, content_type: str) -> bool:
        """Check if this parser can handle the given content type."""
        return content_type.split(";")[0].strip().lower() in self.supported_content_types

    def _count_words(self, text: str) -> int:
        """Count words in text."""
        return len(text.split())
