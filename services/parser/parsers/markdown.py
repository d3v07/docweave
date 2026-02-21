"""Markdown document parser."""
import re
from typing import List, Tuple

from .base import BaseParser, ParsedContent, ContentBlock, BlockType


class MarkdownParser(BaseParser):
    """Parser for Markdown documents."""

    @property
    def supported_content_types(self) -> List[str]:
        return ["text/markdown", "text/x-markdown"]

    def parse(self, content: bytes, filename: str = "") -> ParsedContent:
        """Parse Markdown into structured blocks."""
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = content.decode("latin-1", errors="replace")

        blocks = []
        position = 0
        lines = text.split("\n")
        i = 0

        while i < len(lines):
            line = lines[i]

            # Skip empty lines
            if not line.strip():
                i += 1
                continue

            # Check for headings
            heading_match = re.match(r"^(#{1,6})\s+(.+)$", line)
            if heading_match:
                level = len(heading_match.group(1))
                content_text = heading_match.group(2).strip()
                blocks.append(ContentBlock(
                    type=BlockType.HEADING,
                    content=content_text,
                    level=level,
                    position=position
                ))
                position += len(line) + 1
                i += 1
                continue

            # Check for code blocks
            if line.startswith("```"):
                code_content, end_i = self._parse_code_block(lines, i)
                blocks.append(ContentBlock(
                    type=BlockType.CODE,
                    content=code_content,
                    position=position,
                    metadata={"language": line[3:].strip() or None}
                ))
                for j in range(i, end_i + 1):
                    position += len(lines[j]) + 1
                i = end_i + 1
                continue

            # Check for blockquotes
            if line.startswith(">"):
                quote_content, end_i = self._parse_blockquote(lines, i)
                blocks.append(ContentBlock(
                    type=BlockType.QUOTE,
                    content=quote_content,
                    position=position
                ))
                for j in range(i, end_i + 1):
                    position += len(lines[j]) + 1
                i = end_i + 1
                continue

            # Check for lists
            list_match = re.match(r"^(\s*)([-*+]|\d+\.)\s+", line)
            if list_match:
                list_items, end_i = self._parse_list(lines, i)
                blocks.append(ContentBlock(
                    type=BlockType.LIST,
                    content="",
                    children=list_items,
                    position=position
                ))
                for j in range(i, end_i + 1):
                    position += len(lines[j]) + 1
                i = end_i + 1
                continue

            # Check for horizontal rule
            if re.match(r"^[-*_]{3,}\s*$", line):
                i += 1
                position += len(line) + 1
                continue

            # Regular paragraph
            para_content, end_i = self._parse_paragraph(lines, i)
            if para_content:
                blocks.append(ContentBlock(
                    type=BlockType.PARAGRAPH,
                    content=para_content,
                    position=position
                ))
            for j in range(i, end_i + 1):
                position += len(lines[j]) + 1
            i = end_i + 1

        # Extract plain text (strip markdown syntax)
        raw_text = self._to_plain_text(text)

        return ParsedContent(
            blocks=blocks,
            raw_text=raw_text,
            word_count=self._count_words(raw_text),
            metadata={"filename": filename}
        )

    def _parse_code_block(self, lines: List[str], start: int) -> Tuple[str, int]:
        """Parse a fenced code block."""
        code_lines = []
        i = start + 1

        while i < len(lines):
            if lines[i].startswith("```"):
                return "\n".join(code_lines), i
            code_lines.append(lines[i])
            i += 1

        return "\n".join(code_lines), i - 1

    def _parse_blockquote(self, lines: List[str], start: int) -> Tuple[str, int]:
        """Parse a blockquote."""
        quote_lines = []
        i = start

        while i < len(lines) and lines[i].startswith(">"):
            # Remove leading > and optional space
            content = re.sub(r"^>\s?", "", lines[i])
            quote_lines.append(content)
            i += 1

        return "\n".join(quote_lines), i - 1

    def _parse_list(self, lines: List[str], start: int) -> Tuple[List[ContentBlock], int]:
        """Parse a list (bulleted or numbered)."""
        items = []
        i = start

        while i < len(lines):
            line = lines[i]
            match = re.match(r"^(\s*)([-*+]|\d+\.)\s+(.*)$", line)

            if not match:
                # Check if continuation of previous item (indented)
                if line.startswith("  ") and items:
                    items[-1].content += " " + line.strip()
                    i += 1
                    continue
                break

            indent = len(match.group(1))
            content = match.group(3)

            items.append(ContentBlock(
                type=BlockType.LIST_ITEM,
                content=content,
                level=indent // 2
            ))
            i += 1

        return items, i - 1

    def _parse_paragraph(self, lines: List[str], start: int) -> Tuple[str, int]:
        """Parse a paragraph (until empty line or special block)."""
        para_lines = []
        i = start

        while i < len(lines):
            line = lines[i]

            # Stop at empty line
            if not line.strip():
                break

            # Stop at special blocks
            if (line.startswith("#") or
                line.startswith("```") or
                line.startswith(">") or
                re.match(r"^[-*_]{3,}\s*$", line) or
                re.match(r"^\s*[-*+]\s+", line) or
                re.match(r"^\s*\d+\.\s+", line)):
                break

            para_lines.append(line)
            i += 1

        return " ".join(para_lines), i - 1

    def _to_plain_text(self, markdown: str) -> str:
        """Convert markdown to plain text."""
        text = markdown

        # Remove code blocks
        text = re.sub(r"```[\s\S]*?```", "", text)

        # Remove inline code
        text = re.sub(r"`[^`]+`", "", text)

        # Remove headers markers
        text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)

        # Remove emphasis
        text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
        text = re.sub(r"\*([^*]+)\*", r"\1", text)
        text = re.sub(r"__([^_]+)__", r"\1", text)
        text = re.sub(r"_([^_]+)_", r"\1", text)

        # Remove links, keep text
        text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)

        # Remove images
        text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)

        # Remove blockquote markers
        text = re.sub(r"^>\s?", "", text, flags=re.MULTILINE)

        # Remove list markers
        text = re.sub(r"^\s*[-*+]\s+", "", text, flags=re.MULTILINE)
        text = re.sub(r"^\s*\d+\.\s+", "", text, flags=re.MULTILINE)

        # Clean up whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()
