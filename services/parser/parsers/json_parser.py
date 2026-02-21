"""JSON document parser."""
import json
from typing import List, Any

from .base import BaseParser, ParsedContent, ContentBlock, BlockType


class JSONParser(BaseParser):
    """Parser for JSON documents - extracts structured data as key-value pairs."""

    @property
    def supported_content_types(self) -> List[str]:
        return ["application/json", "text/json"]

    def parse(self, content: bytes, filename: str = "") -> ParsedContent:
        """Parse JSON into structured blocks."""
        try:
            text = content.decode("utf-8")
            data = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            return ParsedContent(
                blocks=[],
                raw_text=content.decode("utf-8", errors="replace"),
                parse_errors=[f"JSON parse error: {str(e)}"]
            )

        blocks = self._extract_blocks(data)

        # Create flattened text representation
        raw_text = self._flatten_to_text(data)

        return ParsedContent(
            blocks=blocks,
            raw_text=raw_text,
            word_count=self._count_words(raw_text),
            metadata={
                "filename": filename,
                "json_type": type(data).__name__
            }
        )

    def _extract_blocks(self, data: Any, prefix: str = "", level: int = 0) -> List[ContentBlock]:
        """Recursively extract content blocks from JSON data."""
        blocks = []

        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key

                if isinstance(value, (dict, list)):
                    # Add heading for nested structure
                    blocks.append(ContentBlock(
                        type=BlockType.HEADING,
                        content=key,
                        level=level + 1,
                        metadata={"json_path": full_key}
                    ))
                    # Recursively process
                    blocks.extend(self._extract_blocks(value, full_key, level + 1))
                else:
                    # Simple key-value pair
                    blocks.append(ContentBlock(
                        type=BlockType.KEY_VALUE,
                        content=f"{key}: {value}",
                        level=level,
                        metadata={
                            "json_path": full_key,
                            "key": key,
                            "value": value,
                            "value_type": type(value).__name__
                        }
                    ))

        elif isinstance(data, list):
            list_items = []
            for i, item in enumerate(data):
                item_path = f"{prefix}[{i}]"

                if isinstance(item, (dict, list)):
                    # Complex list item - process recursively
                    blocks.extend(self._extract_blocks(item, item_path, level))
                else:
                    # Simple list item
                    list_items.append(ContentBlock(
                        type=BlockType.LIST_ITEM,
                        content=str(item),
                        metadata={"json_path": item_path}
                    ))

            if list_items:
                blocks.append(ContentBlock(
                    type=BlockType.LIST,
                    content="",
                    level=level,
                    children=list_items
                ))

        else:
            # Primitive value at root
            blocks.append(ContentBlock(
                type=BlockType.PARAGRAPH,
                content=str(data),
                level=level
            ))

        return blocks

    def _flatten_to_text(self, data: Any, indent: int = 0) -> str:
        """Convert JSON to readable text representation."""
        lines = []
        prefix = "  " * indent

        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    lines.append(f"{prefix}{key}:")
                    lines.append(self._flatten_to_text(value, indent + 1))
                else:
                    lines.append(f"{prefix}{key}: {value}")

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    lines.append(self._flatten_to_text(item, indent))
                else:
                    lines.append(f"{prefix}- {item}")

        else:
            lines.append(f"{prefix}{data}")

        return "\n".join(lines)
