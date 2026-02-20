"""
DocWeave Settings Configuration
===============================

This module defines all configuration settings for DocWeave services
using Pydantic settings management. Settings are loaded from environment
variables with type validation and sensible defaults.

Environment variables can be set directly or via a .env file.
"""

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Neo4jSettings(BaseSettings):
    """
    Neo4j database connection settings.

    Attributes:
        uri: Neo4j connection URI (bolt protocol)
        user: Database username
        password: Database password
        database: Database name
        max_connection_pool_size: Maximum connections in pool
        connection_timeout: Connection timeout in seconds
        encrypted: Whether to use TLS encryption
    """
    model_config = SettingsConfigDict(
        env_prefix="NEO4J_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    uri: str = Field(
        default="bolt://localhost:7687",
        description="Neo4j connection URI"
    )
    user: str = Field(
        default="neo4j",
        description="Neo4j username"
    )
    password: str = Field(
        default="docweave_secure_password",
        description="Neo4j password"
    )
    database: str = Field(
        default="neo4j",
        description="Neo4j database name"
    )
    max_connection_pool_size: int = Field(
        default=50,
        ge=1,
        le=200,
        description="Maximum connection pool size"
    )
    connection_timeout: int = Field(
        default=30,
        ge=5,
        le=120,
        description="Connection timeout in seconds"
    )
    encrypted: bool = Field(
        default=False,
        description="Use TLS encryption"
    )


class KafkaSettings(BaseSettings):
    """
    Apache Kafka connection settings.

    Attributes:
        bootstrap_servers: Kafka broker addresses
        consumer_group_id: Consumer group identifier
        auto_offset_reset: Where to start consuming (earliest/latest)
        enable_auto_commit: Auto-commit consumer offsets
        session_timeout_ms: Consumer session timeout
        heartbeat_interval_ms: Consumer heartbeat interval
        max_poll_interval_ms: Maximum poll interval
        max_poll_records: Maximum records per poll
    """
    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers"
    )
    consumer_group_id: str = Field(
        default="docweave-consumers",
        description="Consumer group ID"
    )
    auto_offset_reset: str = Field(
        default="earliest",
        description="Auto offset reset policy"
    )
    enable_auto_commit: bool = Field(
        default=True,
        description="Enable auto commit"
    )
    session_timeout_ms: int = Field(
        default=30000,
        ge=6000,
        description="Session timeout in milliseconds"
    )
    heartbeat_interval_ms: int = Field(
        default=10000,
        ge=1000,
        description="Heartbeat interval in milliseconds"
    )
    max_poll_interval_ms: int = Field(
        default=300000,
        ge=60000,
        description="Max poll interval in milliseconds"
    )
    max_poll_records: int = Field(
        default=500,
        ge=1,
        le=10000,
        description="Max records per poll"
    )

    @field_validator("auto_offset_reset")
    @classmethod
    def validate_offset_reset(cls, v: str) -> str:
        """Validate auto offset reset value."""
        valid_values = {"earliest", "latest", "none"}
        if v.lower() not in valid_values:
            raise ValueError(f"auto_offset_reset must be one of {valid_values}")
        return v.lower()


class ServiceSettings(BaseSettings):
    """
    Individual service settings.

    Attributes:
        host: Service host address
        port: Service port number
        workers: Number of worker processes
        debug: Enable debug mode
        reload: Enable auto-reload (development only)
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(
        default="0.0.0.0",
        description="Service host"
    )
    port: int = Field(
        default=8000,
        ge=1024,
        le=65535,
        description="Service port"
    )
    workers: int = Field(
        default=1,
        ge=1,
        le=32,
        description="Number of workers"
    )
    debug: bool = Field(
        default=False,
        description="Debug mode"
    )
    reload: bool = Field(
        default=False,
        description="Auto-reload on changes"
    )


class LoggingSettings(BaseSettings):
    """
    Logging configuration settings.

    Attributes:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Log format (json, text)
        include_timestamp: Include timestamp in logs
        include_caller: Include caller information
    """
    model_config = SettingsConfigDict(
        env_prefix="LOG_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    level: str = Field(
        default="INFO",
        description="Log level"
    )
    format: str = Field(
        default="json",
        description="Log format"
    )
    include_timestamp: bool = Field(
        default=True,
        description="Include timestamp"
    )
    include_caller: bool = Field(
        default=True,
        description="Include caller info"
    )

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"level must be one of {valid_levels}")
        return v.upper()

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        """Validate log format."""
        valid_formats = {"json", "text"}
        if v.lower() not in valid_formats:
            raise ValueError(f"format must be one of {valid_formats}")
        return v.lower()


class Settings(BaseSettings):
    """
    Main settings class combining all configuration sections.

    This is the primary settings class that should be used throughout
    the application. It aggregates all component settings and provides
    service-specific configuration through environment variable prefixes.

    Attributes:
        neo4j: Neo4j database settings
        kafka: Kafka message queue settings
        logging: Logging configuration
        service_name: Name of the current service
        environment: Deployment environment (development, staging, production)
        api_key: API key for service authentication
        upload_dir: Directory for file uploads
        max_upload_size_mb: Maximum upload file size
        allowed_extensions: Allowed file extensions for upload
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Nested settings
    neo4j: Neo4jSettings = Field(default_factory=Neo4jSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    # Service identification
    service_name: str = Field(
        default="docweave",
        description="Service name"
    )
    environment: str = Field(
        default="development",
        description="Deployment environment"
    )

    # Security
    api_key: Optional[str] = Field(
        default=None,
        description="API key for authentication"
    )
    api_key_header: str = Field(
        default="X-API-Key",
        description="Header name for API key"
    )

    # File handling
    upload_dir: str = Field(
        default="/app/data/uploads",
        description="Upload directory"
    )
    max_upload_size_mb: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Max upload size in MB"
    )
    allowed_extensions: str = Field(
        default=".pdf,.docx,.txt,.md,.html,.json",
        description="Allowed file extensions"
    )

    # Service ports (for individual services)
    ingestion_service_port: int = Field(default=8001)
    parser_service_port: int = Field(default=8002)
    extractor_service_port: int = Field(default=8003)
    graph_updater_service_port: int = Field(default=8004)
    query_service_port: int = Field(default=8005)

    # Kafka topics
    KAFKA_TOPIC_DOCUMENT_EVENTS: str = Field(default="document-events")
    KAFKA_TOPIC_PARSED_DOCUMENTS: str = Field(default="parsed-documents")
    KAFKA_TOPIC_EXTRACTED_CLAIMS: str = Field(default="extracted-claims")
    KAFKA_TOPIC_GRAPH_UPDATES: str = Field(default="graph-updates")
    KAFKA_TOPIC_CONFLICTS: str = Field(default="conflicts")

    # Document processing
    MAX_DOCUMENT_SIZE_BYTES: int = Field(default=104857600)  # 100MB

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment value."""
        valid_envs = {"development", "staging", "production", "test"}
        if v.lower() not in valid_envs:
            raise ValueError(f"environment must be one of {valid_envs}")
        return v.lower()

    @property
    def allowed_extensions_list(self) -> list[str]:
        """Get allowed extensions as a list."""
        return [ext.strip() for ext in self.allowed_extensions.split(",")]

    @property
    def max_upload_size_bytes(self) -> int:
        """Get max upload size in bytes."""
        return self.max_upload_size_mb * 1024 * 1024

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "production"

    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == "development"


# Global settings instance
settings = Settings()


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses LRU cache to ensure settings are only loaded once
    per process, improving performance and consistency.

    Returns:
        Settings: The application settings instance

    Example:
        settings = get_settings()
        neo4j_uri = settings.neo4j.uri
    """
    return Settings()


def get_service_settings(service_name: str) -> ServiceSettings:
    """
    Get settings for a specific service.

    Args:
        service_name: Name of the service (ingestion, parser, etc.)

    Returns:
        ServiceSettings: Settings for the specified service

    Example:
        settings = get_service_settings("ingestion")
        port = settings.port
    """
    main_settings = get_settings()

    port_mapping = {
        "ingestion": main_settings.ingestion_service_port,
        "parser": main_settings.parser_service_port,
        "extractor": main_settings.extractor_service_port,
        "graph-updater": main_settings.graph_updater_service_port,
        "query": main_settings.query_service_port,
    }

    port = port_mapping.get(service_name, 8000)

    return ServiceSettings(
        port=port,
        debug=main_settings.is_development,
        reload=main_settings.is_development,
    )
