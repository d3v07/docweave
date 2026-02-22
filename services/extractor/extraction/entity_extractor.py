"""Entity extraction using spaCy NER."""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from collections import defaultdict
import re


@dataclass
class ExtractedEntity:
    """Represents an extracted named entity."""
    text: str
    normalized_text: str
    entity_type: str  # PERSON, ORG, PRODUCT, DATE, MONEY, etc.
    start_char: int
    end_char: int
    confidence: float
    aliases: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)

    @property
    def id(self) -> str:
        """Generate entity ID from normalized text and type."""
        clean = re.sub(r'[^a-z0-9]', '_', self.normalized_text.lower())
        return f"ent_{self.entity_type.lower()}_{clean[:20]}"


class EntityExtractor:
    """Extracts and normalizes named entities from text using spaCy."""

    # Entity type mapping from spaCy labels
    TYPE_MAPPING = {
        "PERSON": "PERSON",
        "PER": "PERSON",
        "ORG": "ORGANIZATION",
        "ORGANIZATION": "ORGANIZATION",
        "COMPANY": "ORGANIZATION",
        "GPE": "LOCATION",
        "LOC": "LOCATION",
        "LOCATION": "LOCATION",
        "PRODUCT": "PRODUCT",
        "WORK_OF_ART": "PRODUCT",
        "DATE": "DATE",
        "TIME": "DATE",
        "MONEY": "MONEY",
        "CARDINAL": "NUMBER",
        "QUANTITY": "NUMBER",
        "PERCENT": "PERCENTAGE",
        "EVENT": "EVENT",
    }

    def __init__(self, model_name: str = "en_core_web_sm"):
        """Initialize with spaCy model."""
        import spacy
        try:
            self.nlp = spacy.load(model_name)
        except OSError:
            # Download if not available
            from spacy.cli import download
            download(model_name)
            self.nlp = spacy.load(model_name)

        self._alias_cache: Dict[str, Set[str]] = defaultdict(set)

    def extract(self, text: str) -> List[ExtractedEntity]:
        """Extract entities from text."""
        doc = self.nlp(text)
        entities = []
        seen_spans: Set[Tuple[int, int]] = set()

        for ent in doc.ents:
            # Skip duplicates
            span_key = (ent.start_char, ent.end_char)
            if span_key in seen_spans:
                continue
            seen_spans.add(span_key)

            # Map entity type
            entity_type = self.TYPE_MAPPING.get(ent.label_, ent.label_)

            # Normalize text
            normalized = self._normalize_entity(ent.text, entity_type)

            # Calculate confidence based on context
            confidence = self._calculate_confidence(ent, doc)

            entity = ExtractedEntity(
                text=ent.text,
                normalized_text=normalized,
                entity_type=entity_type,
                start_char=ent.start_char,
                end_char=ent.end_char,
                confidence=confidence,
                metadata={
                    "spacy_label": ent.label_,
                    "sentence": ent.sent.text if ent.sent else ""
                }
            )

            entities.append(entity)

            # Track aliases
            self._alias_cache[normalized].add(ent.text)

        # Resolve coreferences and merge entities
        entities = self._merge_entities(entities)

        return entities

    def _normalize_entity(self, text: str, entity_type: str) -> str:
        """Normalize entity text."""
        normalized = text.strip()

        if entity_type == "PERSON":
            # Remove titles
            normalized = re.sub(r'^(Mr\.|Mrs\.|Ms\.|Dr\.|Prof\.)\s*', '', normalized)
            # Normalize whitespace
            normalized = ' '.join(normalized.split())

        elif entity_type == "ORGANIZATION":
            # Remove common suffixes for matching
            normalized = re.sub(r'\s+(Inc\.?|Corp\.?|LLC|Ltd\.?|Co\.?)$', '', normalized, flags=re.IGNORECASE)
            normalized = ' '.join(normalized.split())

        elif entity_type == "MONEY":
            # Normalize currency format
            normalized = re.sub(r'[$€£]', '', normalized)
            normalized = normalized.replace(',', '')

        elif entity_type == "DATE":
            # Keep as-is for now, could parse to ISO format
            pass

        return normalized

    def _calculate_confidence(self, ent, doc) -> float:
        """Calculate confidence score for entity."""
        base_confidence = 0.7

        # Boost for longer entities (more specific)
        if len(ent.text.split()) > 1:
            base_confidence += 0.1

        # Boost for entities that appear multiple times
        count = sum(1 for e in doc.ents if e.text.lower() == ent.text.lower())
        if count > 1:
            base_confidence += min(0.1, count * 0.02)

        # Boost for entities at start of sentence (often subjects)
        if ent.sent and ent.start_char == ent.sent.start_char:
            base_confidence += 0.05

        # Penalize very short entities
        if len(ent.text) < 3:
            base_confidence -= 0.2

        return min(0.99, max(0.1, base_confidence))

    def _merge_entities(self, entities: List[ExtractedEntity]) -> List[ExtractedEntity]:
        """Merge entities that refer to the same thing."""
        if not entities:
            return entities

        # Group by normalized text
        groups: Dict[str, List[ExtractedEntity]] = defaultdict(list)
        for ent in entities:
            key = f"{ent.entity_type}:{ent.normalized_text.lower()}"
            groups[key].append(ent)

        merged = []
        for key, group in groups.items():
            if len(group) == 1:
                merged.append(group[0])
            else:
                # Take the one with highest confidence
                best = max(group, key=lambda e: e.confidence)
                # Collect aliases
                aliases = list(set(e.text for e in group if e.text != best.text))
                best.aliases = aliases
                # Boost confidence for multiple mentions
                best.confidence = min(0.99, best.confidence + 0.1)
                merged.append(best)

        return merged

    def get_aliases(self, normalized_name: str) -> Set[str]:
        """Get known aliases for an entity."""
        return self._alias_cache.get(normalized_name, set())

    def extract_with_context(self, text: str, context_window: int = 50) -> List[ExtractedEntity]:
        """Extract entities with surrounding context."""
        entities = self.extract(text)

        for entity in entities:
            start = max(0, entity.start_char - context_window)
            end = min(len(text), entity.end_char + context_window)
            entity.metadata["context"] = text[start:end]

        return entities
