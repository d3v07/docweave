"""Claim generation from extracted relations."""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import uuid4
import re

from .entity_extractor import ExtractedEntity
from .relation_extractor import ExtractedRelation
from services.extractor.vocabulary import normalize_predicate, get_predicate_category, PredicateCategory


@dataclass
class GeneratedClaim:
    """A claim ready for insertion into the knowledge graph."""
    id: str
    subject_entity_id: str
    subject_name: str
    predicate: str
    object_value: str
    object_entity_id: Optional[str]
    confidence: float
    source_text: str
    extracted_text: str
    timestamp: datetime
    valid_from: Optional[datetime]
    valid_until: Optional[datetime]
    metadata: Dict[str, Any]


class ClaimGenerator:
    """Generates structured claims from extracted relations."""

    def __init__(self, source_id: str = "unknown"):
        """Initialize claim generator."""
        self.source_id = source_id

    def generate(
        self,
        relations: List[ExtractedRelation],
        source_id: Optional[str] = None
    ) -> List[GeneratedClaim]:
        """Generate claims from extracted relations."""
        claims = []
        used_source = source_id or self.source_id

        for relation in relations:
            claim = self._relation_to_claim(relation, used_source)
            if claim:
                claims.append(claim)

        # Filter low-confidence claims
        claims = [c for c in claims if c.confidence >= 0.4]

        # Deduplicate
        claims = self._deduplicate_claims(claims)

        return claims

    def _relation_to_claim(
        self,
        relation: ExtractedRelation,
        source_id: str
    ) -> Optional[GeneratedClaim]:
        """Convert a relation to a claim."""
        # Normalize the predicate
        normalized_pred = normalize_predicate(relation.predicate)

        # Normalize the object value
        normalized_value = self._normalize_value(
            relation.object_value,
            normalized_pred
        )

        if not normalized_value:
            return None

        # Generate IDs
        claim_id = f"claim_{uuid4().hex[:12]}"
        subject_id = relation.subject.id

        # Extract temporal information
        valid_from, valid_until = self._extract_temporal(
            relation.source_text,
            normalized_pred
        )

        # Calculate final confidence
        confidence = self._calculate_confidence(relation)

        # Build object entity ID if available
        object_entity_id = None
        if relation.object_entity:
            object_entity_id = relation.object_entity.id

        return GeneratedClaim(
            id=claim_id,
            subject_entity_id=subject_id,
            subject_name=relation.subject.normalized_text,
            predicate=normalized_pred,
            object_value=normalized_value,
            object_entity_id=object_entity_id,
            confidence=confidence,
            source_text=relation.source_text,
            extracted_text=relation.source_text,
            timestamp=datetime.utcnow(),
            valid_from=valid_from,
            valid_until=valid_until,
            metadata={
                "extraction_method": relation.extraction_method,
                "original_predicate": relation.predicate,
                "subject_type": relation.subject.entity_type,
                "predicate_category": get_predicate_category(normalized_pred).value,
            }
        )

    def _normalize_value(self, value: str, predicate: str) -> Optional[str]:
        """Normalize the object value based on predicate type."""
        if not value or not value.strip():
            return None

        value = value.strip()
        category = get_predicate_category(predicate)

        # Money normalization
        if predicate in ("has_revenue", "price"):
            return self._normalize_money(value)

        # Count normalization
        if predicate == "employee_count":
            return self._normalize_count(value)

        # Date normalization
        if predicate in ("founded_date", "launch_date"):
            return self._normalize_date(value)

        # Location cleanup
        if category == PredicateCategory.LOCATION:
            # Remove trailing punctuation
            value = re.sub(r'[,.\s]+$', '', value)

        # General cleanup
        value = ' '.join(value.split())

        return value if len(value) > 0 else None

    def _normalize_money(self, value: str) -> str:
        """Normalize monetary values."""
        value = value.replace(',', '').replace('$', '').strip()

        # Handle billion/million abbreviations
        multiplier = 1
        if 'billion' in value.lower() or value.endswith('B'):
            multiplier = 1_000_000_000
            value = re.sub(r'[Bb]illion|B$', '', value).strip()
        elif 'million' in value.lower() or value.endswith('M'):
            multiplier = 1_000_000
            value = re.sub(r'[Mm]illion|M$', '', value).strip()

        try:
            num = float(value)
            total = num * multiplier
            # Format nicely
            if total >= 1_000_000_000:
                return f"${total/1_000_000_000:.1f} billion"
            elif total >= 1_000_000:
                return f"${total/1_000_000:.1f} million"
            else:
                return f"${total:,.0f}"
        except ValueError:
            return f"${value}"

    def _normalize_count(self, value: str) -> str:
        """Normalize count values."""
        value = value.replace(',', '').strip()
        try:
            num = int(float(value))
            return f"{num:,}"
        except ValueError:
            return value

    def _normalize_date(self, value: str) -> str:
        """Normalize date values."""
        # Try to extract year
        year_match = re.search(r'\b(19|20)\d{2}\b', value)
        if year_match:
            return year_match.group(0)

        # Try full date parsing
        from dateutil import parser as date_parser
        try:
            dt = date_parser.parse(value)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return value

    def _extract_temporal(
        self,
        text: str,
        predicate: str
    ) -> tuple[Optional[datetime], Optional[datetime]]:
        """Extract temporal validity from text."""
        valid_from = None
        valid_until = None

        # Look for year mentions
        year_match = re.search(r'\b(20\d{2})\b', text)
        if year_match:
            year = int(year_match.group(1))
            valid_from = datetime(year, 1, 1)

        # Look for quarter mentions
        quarter_match = re.search(r'Q([1-4])\s*(20\d{2})?', text)
        if quarter_match:
            quarter = int(quarter_match.group(1))
            year = int(quarter_match.group(2)) if quarter_match.group(2) else datetime.now().year
            month = (quarter - 1) * 3 + 1
            valid_from = datetime(year, month, 1)

        # Fiscal year indicators suggest annual data
        if "fiscal year" in text.lower() or "annual" in text.lower():
            if valid_from:
                valid_until = datetime(valid_from.year, 12, 31)

        return valid_from, valid_until

    def _calculate_confidence(self, relation: ExtractedRelation) -> float:
        """Calculate final confidence score for a claim."""
        base = relation.confidence

        # Boost for pattern-based extraction (more precise)
        if relation.extraction_method == "pattern":
            base += 0.1

        # Boost if object is also a known entity
        if relation.object_entity:
            base += 0.05

        # Boost for longer source text (more context)
        if len(relation.source_text) > 50:
            base += 0.05

        # Penalize generic predicates
        if relation.normalized_predicate in ("related_to", "has"):
            base -= 0.2

        return min(0.99, max(0.1, base))

    def _deduplicate_claims(
        self,
        claims: List[GeneratedClaim]
    ) -> List[GeneratedClaim]:
        """Remove duplicate claims, keeping highest confidence."""
        seen = {}
        for claim in claims:
            key = (
                claim.subject_entity_id,
                claim.predicate,
                claim.object_value.lower()
            )
            if key not in seen or claim.confidence > seen[key].confidence:
                seen[key] = claim

        return list(seen.values())
