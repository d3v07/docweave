"""Plain text document parser."""
import re
from typing import List

from .base import BaseParser, ParsedContent, ContentBlock, BlockType


class TextParser(BaseParser):
    """Parser for plain text documents."""

    @property
    def supported_content_types(self) -> List[str]:
        return ["text/plain"]

    def parse(self, content: bytes, filename: str = "") -> ParsedContent:
        """Parse plain text into paragraphs."""
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = content.decode("latin-1", errors="replace")

        blocks = []
        position = 0

        # Split into paragraphs (double newline separated)
        paragraphs = re.split(r"\n\s*\n", text)

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            # Detect if it looks like a heading (short, possibly uppercase)
            lines = para.split("\n")
            first_line = lines[0].strip()

            if len(first_line) < 80 and (
                first_line.isupper() or
                first_line.endswith(":") or
                re.match(r"^[A-Z][A-Za-z\s]+$", first_line)
            ):
                # Treat as heading
                blocks.append(ContentBlock(
                    type=BlockType.HEADING,
                    content=first_line,
                    level=1,
                    position=position
                ))
                position += len(first_line) + 1

                # Rest is paragraph
                if len(lines) > 1:
                    rest = "\n".join(lines[1:]).strip()
                    if rest:
                        blocks.append(ContentBlock(
                            type=BlockType.PARAGRAPH,
                            content=rest,
                            position=position
                        ))
                        position += len(rest) + 1
            elif self._is_list(para):
                # Parse as list
                list_items = self._parse_list(para)
                list_block = ContentBlock(
                    type=BlockType.LIST,
                    content="",
                    position=position,
                    children=list_items
                )
                blocks.append(list_block)
                position += len(para) + 1
            else:
                # Regular paragraph
                blocks.append(ContentBlock(
                    type=BlockType.PARAGRAPH,
                    content=para,
                    position=position
                ))
                position += len(para) + 1

        return ParsedContent(
            blocks=blocks,
            raw_text=text,
            word_count=self._count_words(text),
            metadata={"filename": filename}
        )

    def _is_list(self, text: str) -> bool:
        """Check if text looks like a list."""
        lines = text.strip().split("\n")
        list_patterns = [
            r"^\s*[-*•]\s+",  # Bullet points
            r"^\s*\d+[\.\)]\s+",  # Numbered
            r"^\s*[a-zA-Z][\.\)]\s+",  # Lettered
        ]
        matches = 0
        for line in lines:
            for pattern in list_patterns:
                if re.match(pattern, line):
                    matches += 1
                    break
        return matches >= 2 and matches / len(lines) > 0.5

    def _parse_list(self, text: str) -> List[ContentBlock]:
        """Parse text into list items."""
        items = []
        lines = text.strip().split("\n")

        for line in lines:
            # Remove list markers
            cleaned = re.sub(r"^\s*[-*•\d]+[\.\)]*\s*", "", line).strip()
            if cleaned:
                items.append(ContentBlock(
                    type=BlockType.LIST_ITEM,
                    content=cleaned
                ))

        return items
