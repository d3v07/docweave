"""
DocWeave Shared Package
=======================

This package contains shared components used across all DocWeave microservices:

- models: Core data models (Claim, Entity, Source, Document, Graph)
- config: Pydantic settings and configuration management
- utils: Utility functions for Kafka, Neo4j, and other common operations

Usage:
    from shared.models import Claim, Entity, Source, Document
    from shared.config import Settings
    from shared.utils import KafkaClient, Neo4jClient
"""

__version__ = "0.1.0"
__author__ = "DocWeave Team"

from shared.models import (
    Claim,
    ClaimStatus,
    Document,
    DocumentEvent,
    DocumentEventType,
    Entity,
    EntityType,
    Source,
    SourceType,
)
from shared.config import Settings

__all__ = [
    "Claim",
    "ClaimStatus",
    "Document",
    "DocumentEvent",
    "DocumentEventType",
    "Entity",
    "EntityType",
    "Settings",
    "Source",
    "SourceType",
]
