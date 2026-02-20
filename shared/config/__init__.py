"""
DocWeave Configuration Module
=============================

This module provides centralized configuration management using Pydantic settings.
Configuration is loaded from environment variables with sensible defaults.

Usage:
    from shared.config import Settings, get_settings

    settings = get_settings()
    print(settings.neo4j_uri)
"""

from shared.config.settings import (
    Settings,
    Neo4jSettings,
    KafkaSettings,
    ServiceSettings,
    LoggingSettings,
    get_settings,
)

__all__ = [
    "Settings",
    "Neo4jSettings",
    "KafkaSettings",
    "ServiceSettings",
    "LoggingSettings",
    "get_settings",
]
