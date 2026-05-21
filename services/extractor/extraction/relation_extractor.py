"""Relation extraction using dependency parsing and patterns."""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from services.extractor.vocabulary import RELATION_VERB_PATTERNS, normalize_predicate

from .entity_extractor import ExtractedEntity


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

    ENTITY_PHRASE = r"[A-Z][A-Za-z0-9&.'-]*(?:\s+[A-Z][A-Za-z0-9&.'-]*){0,6}"
    MONEY_VALUE = r"\$?[\d.,]+\s*(?:billion|million|B|M)?"

    # Pattern-based extraction rules
    PATTERNS = [
        # CEO/leadership patterns
        {
            "pattern": rf"({ENTITY_PHRASE}),?\s+(?:the\s+)?(?:CEO|Chief Executive Officer|President|Chairman)\s+(?:of|at)\s+({ENTITY_PHRASE})",
            "predicate": "ceo",
            "subject_group": 2,
            "object_group": 1,
        },
        {
            "pattern": rf"({ENTITY_PHRASE})(?:'s)?\s+(?:CEO|Chief Executive Officer),?\s+([A-Z][a-z]+ [A-Z][a-z]+)",
            "predicate": "ceo",
            "subject_group": 1,
            "object_group": 2,
        },
        # Revenue patterns
        {
            "pattern": rf"({ENTITY_PHRASE})\s+(?:reported|announced|earned|generated|had)\s+(?:annual\s+)?revenue\s+of\s+({MONEY_VALUE})",
            "predicate": "has_revenue",
            "subject_group": 1,
            "object_group": 2,
        },
        # Employee count patterns
        {
            "pattern": rf"({ENTITY_PHRASE})\s+(?:employs?|has|with)\s+(?:approximately\s+|about\s+|around\s+)?([\d,]+)\s+(?:employees|people|workers|staff)",
            "predicate": "employee_count",
            "subject_group": 1,
            "object_group": 2,
        },
        # Location patterns
        {
            "pattern": rf"({ENTITY_PHRASE})\s+(?:is\s+)?(?:headquartered|based|located)\s+in\s+([A-Z][A-Za-z .'-]+?)(?:\s+with\b|[.,]|$)",
            "predicate": "headquartered_in",
            "subject_group": 1,
            "object_group": 2,
        },
        # Founded patterns
        {
            "pattern": rf"({ENTITY_PHRASE})\s+(?:was\s+)?founded\s+(?:in\s+)?(\d{{4}})",
            "predicate": "founded_date",
            "subject_group": 1,
            "object_group": 2,
        },
        {
            "pattern": rf"Founded\s+in\s+(\d{{4}}),\s+({ENTITY_PHRASE})\s+(?:is|was)\b",
            "predicate": "founded_date",
            "subject_group": 2,
            "object_group": 1,
        },
        {
            "pattern": rf"([A-Z][a-z]+ [A-Z][a-z]+)\s+founded\s+({ENTITY_PHRASE})",
            "predicate": "founded",
            "subject_group": 1,
            "object_group": 2,
        },
        # Price patterns
        {
            "pattern": rf"({ENTITY_PHRASE})\s+(?:is\s+|will\s+be\s+)?(?:available|priced|sells?)\s+(?:for|at)\s+\$?([\d.,]+)",
            "predicate": "price",
            "subject_group": 1,
            "object_group": 2,
        },
        # Launch date patterns
        {
            "pattern": rf"({ENTITY_PHRASE})(?:,\s+[^.\n]{{1,120}},)?\s+(?:will\s+)?(?:launch|release|available)\s+(?:on\s+)?([A-Z][a-z]+ \d+,?\s*\d*)",
            "predicate": "launch_date",
            "subject_group": 1,
            "object_group": 2,
        },
        # Acquisition patterns
        {
            "pattern": rf"({ENTITY_PHRASE})\s+(?:acquired|bought|purchased)\s+({ENTITY_PHRASE})",
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
        self, text: str, entities: Optional[List[ExtractedEntity]] = None
    ) -> List[ExtractedRelation]:
        """Extract relations from text."""
        relations = []

        # Pattern-based extraction
        pattern_relations = self._extract_patterns(text, entities)
        relations.extend(pattern_relations)

        metric_relations = self._extract_metric_lines(text, entities)
        relations.extend(metric_relations)

        # Dependency-based extraction
        dep_relations = self._extract_dependencies(text, entities)
        relations.extend(dep_relations)

        # Deduplicate
        relations = self._deduplicate_relations(relations)

        return relations

    def _extract_patterns(
        self, text: str, entities: Optional[List[ExtractedEntity]] = None
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
            for match in re.finditer(pattern, text, re.MULTILINE):
                subject_text = self._clean_phrase(match.group(rule["subject_group"]))
                object_text = self._clean_phrase(match.group(rule["object_group"]))

                if not self._is_good_subject(subject_text) or not self._is_good_object(
                    object_text
                ):
                    continue

                # Try to find matching entities
                subject_entity = self._find_entity(entity_map, subject_text)
                object_entity = entity_map.get(object_text.lower())

                # Create placeholder entity if needed
                if not subject_entity:
                    subject_entity = ExtractedEntity(
                        text=subject_text,
                        normalized_text=subject_text,
                        entity_type="UNKNOWN",
                        start_char=match.start(rule["subject_group"]),
                        end_char=match.end(rule["subject_group"]),
                        confidence=0.6,
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
                    extraction_method="pattern",
                )
                relations.append(relation)

        return relations

    def _extract_metric_lines(
        self,
        text: str,
        entities: Optional[List[ExtractedEntity]] = None,
    ) -> List[ExtractedRelation]:
        """Extract common document metric lines using the primary organization."""
        subject_entity = self._primary_organization(entities, text)
        if not subject_entity:
            return []

        relations = []
        metric_rules = [
            (
                r"^\s*[-*]?\s*(?:Q[1-4]\s+|Full\s+Year\s+|Annual\s+)?Revenue\s*:\s*("
                + self.MONEY_VALUE
                + r")\b",
                "has_revenue",
            ),
            (
                r"^\s*[-*]?\s*(?:Employee\s+Count|Employees|Headcount)\s*:\s*([\d,]+)\b",
                "employee_count",
            ),
            (
                r"^\s*[-*]?\s*Headquarters\s*:\s*([A-Z][A-Za-z .,'-]+)$",
                "headquartered_in",
            ),
            (
                r"^\s*[-*]?\s*Founded\s*:\s*(\d{4})\b",
                "founded_date",
            ),
        ]

        for line in text.splitlines():
            for pattern, predicate in metric_rules:
                match = re.search(pattern, line)
                if not match:
                    continue

                object_text = self._clean_phrase(match.group(1))
                if not self._is_good_object(object_text):
                    continue

                relations.append(
                    ExtractedRelation(
                        subject=subject_entity,
                        predicate=predicate,
                        normalized_predicate=normalize_predicate(predicate),
                        object_value=object_text,
                        confidence=0.85,
                        source_text=line.strip(),
                        extraction_method="metric_line",
                    )
                )

        return relations

    def _extract_dependencies(
        self, text: str, entities: Optional[List[ExtractedEntity]] = None
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

                    if (
                        subject
                        and obj
                        and self._is_good_subject(subject)
                        and self._is_good_object(obj)
                    ):
                        # Try to match with known entities
                        subject_entity = self._find_entity(entity_map, subject)
                        if not subject_entity:
                            subject_entity = ExtractedEntity(
                                text=self._clean_phrase(subject),
                                normalized_text=self._clean_phrase(subject),
                                entity_type="UNKNOWN",
                                start_char=0,
                                end_char=0,
                                confidence=0.5,
                            )

                        object_entity = entity_map.get(obj.lower())

                        # Normalize the verb to a predicate
                        predicate = self._verb_to_predicate(verb)
                        if predicate in {"announce", "say", "provide"}:
                            continue

                        relation = ExtractedRelation(
                            subject=subject_entity,
                            predicate=verb,
                            normalized_predicate=predicate,
                            object_value=self._clean_phrase(obj),
                            object_entity=object_entity,
                            confidence=0.6,
                            source_text=sent.text,
                            start_char=sent.start_char,
                            end_char=sent.end_char,
                            extraction_method="dependency",
                        )
                        relations.append(relation)

        return relations

    def _clean_phrase(self, value: str) -> str:
        """Clean an extracted subject or object phrase."""
        cleaned = " ".join(value.replace("\n", " ").split())
        cleaned = re.sub(r"^\W+|\W+$", "", cleaned)
        cleaned = re.sub(r",\s+(?:its|the|a|an)\b.*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+\([A-Z]+:[^)]+\)$", "", cleaned)
        cleaned = re.sub(r"^(?:CA|TX|NY)\s*-\s+", "", cleaned)
        cleaned = re.sub(r"\s+(?:is|was|will be)$", "", cleaned, flags=re.IGNORECASE)
        return cleaned.strip()

    def _is_good_subject(self, subject: str) -> bool:
        subject = self._clean_phrase(subject)
        lowered = subject.lower()

        if not subject or len(subject) < 2 or len(subject) > 90:
            return False
        if "\n" in subject:
            return False
        if lowered in {
            "will",
            "for immediate",
            "immediate release",
            "release",
            "this memo",
        }:
            return False
        if len(subject.split()) > 8:
            return False
        if subject.isupper() and len(subject.split()) > 3:
            return False
        return True

    def _is_good_object(self, obj: str) -> bool:
        obj = self._clean_phrase(obj)
        if not obj or len(obj) > 140:
            return False
        if "\n" in obj:
            return False
        return True

    def _find_entity(
        self,
        entity_map: dict[str, ExtractedEntity],
        subject_text: str,
    ) -> Optional[ExtractedEntity]:
        cleaned = self._clean_phrase(subject_text)
        return entity_map.get(cleaned.lower()) or entity_map.get(subject_text.lower())

    def _primary_organization(
        self,
        entities: Optional[List[ExtractedEntity]],
        text: str,
    ) -> Optional[ExtractedEntity]:
        if not entities:
            return None

        candidates = [
            ent
            for ent in entities
            if ent.entity_type == "ORGANIZATION"
            and self._is_good_subject(ent.normalized_text)
            and ent.normalized_text.lower() not in {"media relations"}
        ]
        if not candidates:
            return None

        lowered_text = text.lower()

        return max(
            candidates,
            key=lambda ent: (
                self._has_org_suffix(ent.normalized_text),
                lowered_text.count(self._clean_phrase(ent.normalized_text).lower()),
                ent.confidence,
                len(ent.normalized_text),
            ),
        )

    def _has_org_suffix(self, value: str) -> bool:
        lowered = value.lower()
        return any(
            suffix in lowered
            for suffix in ("corporation", "company", " inc", " corp", " llc", " ltd")
        )

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
            if verb_lower in verbs:
                return predicate

        return normalize_predicate(verb)

    def _deduplicate_relations(
        self, relations: List[ExtractedRelation]
    ) -> List[ExtractedRelation]:
        """Remove duplicate relations, keeping highest confidence."""
        seen = {}
        for rel in relations:
            key = (
                rel.subject.normalized_text.lower(),
                rel.normalized_predicate,
                rel.object_value.lower(),
            )
            if key not in seen or rel.confidence > seen[key].confidence:
                seen[key] = rel

        return list(seen.values())
