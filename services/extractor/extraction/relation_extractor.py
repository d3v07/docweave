"""Relation extraction using dependency parsing and patterns."""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any
import re

from .entity_extractor import ExtractedEntity
from services.extractor.vocabulary import normalize_predicate, RELATION_VERB_PATTERNS


@dataclass
class ExtractedRelation:
    """Represents an extracted relation (subject-predicate-object triple)."""
    subject: ExtractedEntity
    predicate: str
    normalized_predicate: str
    object_value: str
    object_entity: Optional[ExtractedEntity] = None
    confidence: float = 0.5
    source_text: str = ""
    start_char: int = 0
    end_char: int = 0
    extraction_method: str = "unknown"
    metadata: Dict[str, Any] = field(default_factory=dict)


class RelationExtractor:
    """Extracts relations between entities using multiple strategies."""

    # Pattern-based extraction rules
    PATTERNS = [
        # CEO/leadership patterns
        {
            "pattern": r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:the\s+)?(?:CEO|Chief Executive Officer|President|Chairman)\s+(?:of|at)\s+([A-Z][A-Za-z\s&]+)",
            "predicate": "ceo",
            "subject_group": 2,
            "object_group": 1,
        },
        {
            "pattern": r"([A-Z][A-Za-z\s&]+)(?:'s)?\s+(?:CEO|Chief Executive Officer),?\s+([A-Z][a-z]+ [A-Z][a-z]+)",
            "predicate": "ceo",
            "subject_group": 1,
            "object_group": 2,
        },
        # Revenue patterns
        {
            "pattern": r"([A-Z][A-Za-z\s&]+)\s+(?:reported|announced|earned|generated|had)\s+(?:annual\s+)?revenue\s+of\s+\$?([\d.,]+\s*(?:billion|million|B|M)?)",
            "predicate": "has_revenue",
            "subject_group": 1,
            "object_group": 2,
        },
        # Employee count patterns
        {
            "pattern": r"([A-Z][A-Za-z\s&]+)\s+(?:employs?|has|with)\s+(?:approximately\s+|about\s+|around\s+)?([\d,]+)\s+(?:employees|people|workers|staff)",
            "predicate": "employee_count",
            "subject_group": 1,
            "object_group": 2,
        },
        # Location patterns
        {
            "pattern": r"([A-Z][A-Za-z\s&]+)\s+(?:is\s+)?(?:headquartered|based|located)\s+in\s+([A-Z][A-Za-z\s,]+)",
            "predicate": "headquartered_in",
            "subject_group": 1,
            "object_group": 2,
        },
        # Founded patterns
        {
            "pattern": r"([A-Z][A-Za-z\s&]+)\s+(?:was\s+)?founded\s+(?:in\s+)?(\d{4})",
            "predicate": "founded_date",
            "subject_group": 1,
            "object_group": 2,
        },
        {
            "pattern": r"([A-Z][a-z]+ [A-Z][a-z]+)\s+founded\s+([A-Z][A-Za-z\s&]+)",
            "predicate": "founded",
            "subject_group": 1,
            "object_group": 2,
        },
        # Price patterns
        {
            "pattern": r"([A-Z][A-Za-z\s]+)\s+(?:is\s+)?(?:available|priced|sells?)\s+(?:for|at)\s+\$?([\d.,]+)",
            "predicate": "price",
            "subject_group": 1,
            "object_group": 2,
        },
        # Launch date patterns
        {
            "pattern": r"([A-Z][A-Za-z\s]+)\s+(?:will\s+)?(?:launch|release|available)\s+(?:on\s+)?([A-Z][a-z]+ \d+,?\s*\d*)",
            "predicate": "launch_date",
            "subject_group": 1,
            "object_group": 2,
        },
        # Acquisition patterns
        {
            "pattern": r"([A-Z][A-Za-z\s&]+)\s+(?:acquired|bought|purchased)\s+([A-Z][A-Za-z\s&]+)",
            "predicate": "acquired",
            "subject_group": 1,
            "object_group": 2,
        },
    ]

    def __init__(self, model_name: str = "en_core_web_sm"):
        """Initialize with spaCy model for dependency parsing."""
        import spacy
        try:
            self.nlp = spacy.load(model_name)
        except OSError:
            from spacy.cli import download
            download(model_name)
            self.nlp = spacy.load(model_name)

    def extract(
        self,
        text: str,
        entities: Optional[List[ExtractedEntity]] = None
    ) -> List[ExtractedRelation]:
        """Extract relations from text."""
        relations = []

        # Pattern-based extraction
        pattern_relations = self._extract_patterns(text, entities)
        relations.extend(pattern_relations)

        # Dependency-based extraction
        dep_relations = self._extract_dependencies(text, entities)
        relations.extend(dep_relations)

        # Deduplicate
        relations = self._deduplicate_relations(relations)

        return relations

    def _extract_patterns(
        self,
        text: str,
        entities: Optional[List[ExtractedEntity]] = None
    ) -> List[ExtractedRelation]:
        """Extract relations using regex patterns."""
        relations = []
        entity_map = {}

        if entities:
            for ent in entities:
                entity_map[ent.text.lower()] = ent
                entity_map[ent.normalized_text.lower()] = ent

        for rule in self.PATTERNS:
            pattern = rule["pattern"]
            for match in re.finditer(pattern, text, re.IGNORECASE):
                subject_text = match.group(rule["subject_group"]).strip()
                object_text = match.group(rule["object_group"]).strip()

                # Try to find matching entities
                subject_entity = entity_map.get(subject_text.lower())
                object_entity = entity_map.get(object_text.lower())

                # Create placeholder entity if needed
                if not subject_entity:
                    subject_entity = ExtractedEntity(
                        text=subject_text,
                        normalized_text=subject_text,
                        entity_type="UNKNOWN",
                        start_char=match.start(rule["subject_group"]),
                        end_char=match.end(rule["subject_group"]),
                        confidence=0.6
                    )

                relation = ExtractedRelation(
                    subject=subject_entity,
                    predicate=rule["predicate"],
                    normalized_predicate=normalize_predicate(rule["predicate"]),
                    object_value=object_text,
                    object_entity=object_entity,
                    confidence=0.8,
                    source_text=match.group(0),
                    start_char=match.start(),
                    end_char=match.end(),
                    extraction_method="pattern"
                )
                relations.append(relation)

        return relations

    def _extract_dependencies(
        self,
        text: str,
        entities: Optional[List[ExtractedEntity]] = None
    ) -> List[ExtractedRelation]:
        """Extract relations using dependency parsing."""
        relations = []
        doc = self.nlp(text)

        entity_map = {}
        if entities:
            for ent in entities:
                entity_map[ent.text.lower()] = ent

        for sent in doc.sents:
            # Find subject-verb-object patterns
            for token in sent:
                if token.pos_ == "VERB":
                    subject = None
                    obj = None
                    verb = token.lemma_

                    # Find subject
                    for child in token.children:
                        if child.dep_ in ("nsubj", "nsubjpass"):
                            subject = self._get_full_span(child)
                        elif child.dep_ in ("dobj", "attr", "pobj"):
                            obj = self._get_full_span(child)

                    # Also check for prepositional objects
                    for child in token.children:
                        if child.dep_ == "prep":
                            for pobj in child.children:
                                if pobj.dep_ == "pobj" and not obj:
                                    obj = self._get_full_span(pobj)

                    if subject and obj and len(subject) > 1 and len(obj) > 1:
                        # Try to match with known entities
                        subject_entity = entity_map.get(subject.lower())
                        if not subject_entity:
                            subject_entity = ExtractedEntity(
                                text=subject,
                                normalized_text=subject,
                                entity_type="UNKNOWN",
                                start_char=0,
                                end_char=0,
                                confidence=0.5
                            )

                        object_entity = entity_map.get(obj.lower())

                        # Normalize the verb to a predicate
                        predicate = self._verb_to_predicate(verb)

                        relation = ExtractedRelation(
                            subject=subject_entity,
                            predicate=verb,
                            normalized_predicate=predicate,
                            object_value=obj,
                            object_entity=object_entity,
                            confidence=0.6,
                            source_text=sent.text,
                            start_char=sent.start_char,
                            end_char=sent.end_char,
                            extraction_method="dependency"
                        )
                        relations.append(relation)

        return relations

    def _get_full_span(self, token) -> str:
        """Get the full noun phrase for a token."""
        # Get the subtree (all dependents)
        subtree = list(token.subtree)
        if subtree:
            start = min(t.idx for t in subtree)
            end = max(t.idx + len(t.text) for t in subtree)
            return token.doc.text[start:end].strip()
        return token.text

    def _verb_to_predicate(self, verb: str) -> str:
        """Convert a verb to a standard predicate."""
        verb_lower = verb.lower()

        for predicate, verbs in RELATION_VERB_PATTERNS.items():
            if verb_lower in verbs or any(v in verb_lower for v in verbs):
                return predicate

        return normalize_predicate(verb)

    def _deduplicate_relations(
        self,
        relations: List[ExtractedRelation]
    ) -> List[ExtractedRelation]:
        """Remove duplicate relations, keeping highest confidence."""
        seen = {}
        for rel in relations:
            key = (
                rel.subject.normalized_text.lower(),
                rel.normalized_predicate,
                rel.object_value.lower()
            )
            if key not in seen or rel.confidence > seen[key].confidence:
                seen[key] = rel

        return list(seen.values())
