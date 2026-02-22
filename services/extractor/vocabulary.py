"""Standard predicate vocabulary for claim normalization."""
from enum import Enum
from typing import Dict, List, Set


class PredicateCategory(str, Enum):
    """Categories of predicates."""
    PERSON = "person"
    ORGANIZATION = "organization"
    PRODUCT = "product"
    LOCATION = "location"
    TEMPORAL = "temporal"
    GENERAL = "general"


# Standard predicates with aliases for normalization
PREDICATE_VOCABULARY: Dict[str, Dict] = {
    # Person predicates
    "works_at": {
        "category": PredicateCategory.PERSON,
        "aliases": ["employed_by", "works_for", "employee_of", "employed_at"],
        "description": "Person is employed by organization"
    },
    "job_title": {
        "category": PredicateCategory.PERSON,
        "aliases": ["position", "role", "title", "serves_as"],
        "description": "Person's job title or position"
    },
    "reports_to": {
        "category": PredicateCategory.PERSON,
        "aliases": ["managed_by", "supervised_by"],
        "description": "Person reports to another person"
    },
    "founded": {
        "category": PredicateCategory.PERSON,
        "aliases": ["founder_of", "started", "established", "created"],
        "description": "Person founded an organization"
    },

    # Organization predicates
    "has_revenue": {
        "category": PredicateCategory.ORGANIZATION,
        "aliases": ["revenue", "annual_revenue", "yearly_revenue", "earned"],
        "description": "Organization's revenue"
    },
    "employee_count": {
        "category": PredicateCategory.ORGANIZATION,
        "aliases": ["employees", "headcount", "staff_count", "workforce"],
        "description": "Number of employees"
    },
    "headquartered_in": {
        "category": PredicateCategory.ORGANIZATION,
        "aliases": ["hq_location", "based_in", "headquarters"],
        "description": "Organization's headquarters location"
    },
    "ceo": {
        "category": PredicateCategory.ORGANIZATION,
        "aliases": ["chief_executive", "led_by", "headed_by"],
        "description": "Organization's CEO"
    },
    "founded_date": {
        "category": PredicateCategory.ORGANIZATION,
        "aliases": ["established_date", "inception_date", "started_date"],
        "description": "When organization was founded"
    },
    "acquired": {
        "category": PredicateCategory.ORGANIZATION,
        "aliases": ["bought", "purchased", "took_over"],
        "description": "Organization acquired another"
    },

    # Product predicates
    "manufactured_by": {
        "category": PredicateCategory.PRODUCT,
        "aliases": ["made_by", "produced_by", "created_by"],
        "description": "Product manufacturer"
    },
    "price": {
        "category": PredicateCategory.PRODUCT,
        "aliases": ["costs", "priced_at", "sells_for", "available_for"],
        "description": "Product price"
    },
    "launch_date": {
        "category": PredicateCategory.PRODUCT,
        "aliases": ["released", "launched", "available_from", "release_date"],
        "description": "Product launch date"
    },
    "has_feature": {
        "category": PredicateCategory.PRODUCT,
        "aliases": ["features", "includes", "offers"],
        "description": "Product has a feature"
    },

    # Location predicates
    "located_in": {
        "category": PredicateCategory.LOCATION,
        "aliases": ["location", "based_in", "situated_in", "in"],
        "description": "Entity is located in a place"
    },
    "operates_in": {
        "category": PredicateCategory.LOCATION,
        "aliases": ["active_in", "present_in", "serves"],
        "description": "Organization operates in location"
    },

    # General predicates
    "part_of": {
        "category": PredicateCategory.GENERAL,
        "aliases": ["belongs_to", "member_of", "subsidiary_of"],
        "description": "Entity is part of another"
    },
    "owns": {
        "category": PredicateCategory.GENERAL,
        "aliases": ["has", "possesses", "controls"],
        "description": "Entity owns another"
    },
    "related_to": {
        "category": PredicateCategory.GENERAL,
        "aliases": ["associated_with", "connected_to", "linked_to"],
        "description": "General relationship"
    },
}

# Build reverse lookup for normalization
_ALIAS_TO_PREDICATE: Dict[str, str] = {}
for pred, info in PREDICATE_VOCABULARY.items():
    _ALIAS_TO_PREDICATE[pred.lower()] = pred
    for alias in info.get("aliases", []):
        _ALIAS_TO_PREDICATE[alias.lower()] = pred


def normalize_predicate(predicate: str) -> str:
    """Normalize a predicate to standard vocabulary."""
    cleaned = predicate.lower().strip().replace(" ", "_").replace("-", "_")
    return _ALIAS_TO_PREDICATE.get(cleaned, cleaned)


def get_predicate_category(predicate: str) -> PredicateCategory:
    """Get the category of a predicate."""
    normalized = normalize_predicate(predicate)
    if normalized in PREDICATE_VOCABULARY:
        return PREDICATE_VOCABULARY[normalized]["category"]
    return PredicateCategory.GENERAL


def get_all_predicates() -> List[str]:
    """Get all standard predicates."""
    return list(PREDICATE_VOCABULARY.keys())


def get_predicates_by_category(category: PredicateCategory) -> List[str]:
    """Get predicates for a specific category."""
    return [
        pred for pred, info in PREDICATE_VOCABULARY.items()
        if info["category"] == category
    ]


# Verb patterns for relation extraction
RELATION_VERB_PATTERNS: Dict[str, List[str]] = {
    "works_at": ["work", "employ", "join", "hire"],
    "job_title": ["serve", "act", "appointed", "named"],
    "has_revenue": ["report", "earn", "generate", "reach"],
    "employee_count": ["employ", "have", "staff"],
    "ceo": ["lead", "head", "run", "manage"],
    "founded": ["found", "start", "establish", "create"],
    "acquired": ["acquire", "buy", "purchase", "take over"],
    "price": ["cost", "price", "sell", "available"],
    "launch_date": ["launch", "release", "introduce", "announce"],
    "located_in": ["locate", "base", "headquarter", "situate"],
}
