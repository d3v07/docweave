"""HTML document parser."""
import re
from typing import List, Optional
from html.parser import HTMLParser as StdHTMLParser

from .base import BaseParser, ParsedContent, ContentBlock, BlockType


class HTMLContentExtractor(StdHTMLParser):
    """Extracts content from HTML documents."""

    def __init__(self):
        super().__init__()
        self.blocks: List[ContentBlock] = []
        self.current_text = ""
        self.current_tag = ""
        self.tag_stack: List[str] = []
        self.metadata: dict = {}
        self.in_script = False
        self.in_style = False
        self.list_items: List[ContentBlock] = []
        self.position = 0

    def handle_starttag(self, tag: str, attrs: list):
        self.tag_stack.append(tag)
        tag = tag.lower()

        if tag in ("script", "style"):
            self.in_script = tag == "script"
            self.in_style = tag == "style"
            return

        # Flush previous content
        self._flush_text()
        self.current_tag = tag

        # Extract metadata
        attrs_dict = dict(attrs)
        if tag == "meta":
            name = attrs_dict.get("name", attrs_dict.get("property", ""))
            content = attrs_dict.get("content", "")
            if name and content:
                self.metadata[name] = content
        elif tag == "title":
            pass  # Will capture content
        elif tag == "img":
            alt = attrs_dict.get("alt", "")
            src = attrs_dict.get("src", "")
            if alt or src:
                self.blocks.append(ContentBlock(
                    type=BlockType.IMAGE,
                    content=alt or src,
                    metadata={"src": src, "alt": alt},
                    position=self.position
                ))
        elif tag == "a":
            href = attrs_dict.get("href", "")
            if href:
                self.metadata["_pending_link"] = href

    def handle_endtag(self, tag: str):
        if self.tag_stack:
            self.tag_stack.pop()

        tag = tag.lower()

        if tag == "script":
            self.in_script = False
            return
        if tag == "style":
            self.in_style = False
            return

        self._flush_text()

        # Handle list end
        if tag in ("ul", "ol") and self.list_items:
            self.blocks.append(ContentBlock(
                type=BlockType.LIST,
                content="",
                children=self.list_items.copy(),
                position=self.position
            ))
            self.list_items = []

    def handle_data(self, data: str):
        if self.in_script or self.in_style:
            return

        text = data.strip()
        if text:
            self.current_text += " " + text if self.current_text else text

    def _flush_text(self):
        """Convert accumulated text to a content block."""
        if not self.current_text:
            return

        text = self.current_text.strip()
        self.current_text = ""

        if not text:
            return

        tag = self.current_tag.lower()

        # Map HTML tags to block types
        if tag == "title":
            self.metadata["title"] = text
            self.blocks.append(ContentBlock(
                type=BlockType.TITLE,
                content=text,
                position=self.position
            ))
        elif tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag[1])
            self.blocks.append(ContentBlock(
                type=BlockType.HEADING,
                content=text,
                level=level,
                position=self.position
            ))
        elif tag == "li":
            self.list_items.append(ContentBlock(
                type=BlockType.LIST_ITEM,
                content=text
            ))
        elif tag in ("pre", "code"):
            self.blocks.append(ContentBlock(
                type=BlockType.CODE,
                content=text,
                position=self.position
            ))
        elif tag == "blockquote":
            self.blocks.append(ContentBlock(
                type=BlockType.QUOTE,
                content=text,
                position=self.position
            ))
        elif tag == "a":
            href = self.metadata.pop("_pending_link", "")
            self.blocks.append(ContentBlock(
                type=BlockType.LINK,
                content=text,
                metadata={"href": href},
                position=self.position
            ))
        elif tag in ("p", "div", "span", "td", "th"):
            self.blocks.append(ContentBlock(
                type=BlockType.PARAGRAPH,
                content=text,
                position=self.position
            ))
        elif text:
            # Default to paragraph for any text content
            self.blocks.append(ContentBlock(
                type=BlockType.PARAGRAPH,
                content=text,
                position=self.position
            ))

        self.position += len(text)


class HTMLParser(BaseParser):
    """Parser for HTML documents."""

    @property
    def supported_content_types(self) -> List[str]:
        return ["text/html", "application/xhtml+xml"]

    def parse(self, content: bytes, filename: str = "") -> ParsedContent:
        """Parse HTML into structured blocks."""
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = content.decode("latin-1", errors="replace")

        extractor = HTMLContentExtractor()
        try:
            extractor.feed(text)
        except Exception as e:
            return ParsedContent(
                blocks=[],
                raw_text=self._strip_tags(text),
                parse_errors=[f"HTML parse error: {str(e)}"]
            )

        # Get plain text version
        raw_text = self._strip_tags(text)

        return ParsedContent(
            blocks=extractor.blocks,
            metadata={**extractor.metadata, "filename": filename},
            raw_text=raw_text,
            word_count=self._count_words(raw_text)
        )

    def _strip_tags(self, html: str) -> str:
        """Remove HTML tags and return plain text."""
        # Remove script and style content
        html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)
        # Remove tags
        html = re.sub(r"<[^>]+>", " ", html)
        # Clean whitespace
        html = re.sub(r"\s+", " ", html)
        return html.strip()
