"""Initialize Neo4j schema with indexes and constraints."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neo4j import GraphDatabase


def get_driver():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "docweave123")
    return GraphDatabase.driver(uri, auth=(user, password))


def init_schema(driver):
    """Create indexes and constraints for the DocWeave schema."""

    constraints = [
        # Unique constraints
        "CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE",
        "CREATE CONSTRAINT claim_id IF NOT EXISTS FOR (c:Claim) REQUIRE c.id IS UNIQUE",
        "CREATE CONSTRAINT source_id IF NOT EXISTS FOR (s:Source) REQUIRE s.id IS UNIQUE",
        "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE",
    ]

    indexes = [
        # Entity indexes
        "CREATE INDEX entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name)",
        "CREATE INDEX entity_type IF NOT EXISTS FOR (e:Entity) ON (e.type)",

        # Claim indexes
        "CREATE INDEX claim_subject IF NOT EXISTS FOR (c:Claim) ON (c.subject_entity_id)",
        "CREATE INDEX claim_predicate IF NOT EXISTS FOR (c:Claim) ON (c.predicate)",
        "CREATE INDEX claim_status IF NOT EXISTS FOR (c:Claim) ON (c.status)",

        # Source indexes
        "CREATE INDEX source_uri IF NOT EXISTS FOR (s:Source) ON (s.uri)",
        "CREATE INDEX source_type IF NOT EXISTS FOR (s:Source) ON (s.source_type)",

        # Document indexes
        "CREATE INDEX document_hash IF NOT EXISTS FOR (d:Document) ON (d.content_hash)",
    ]

    with driver.session() as session:
        print("Creating constraints...")
        for constraint in constraints:
            try:
                session.run(constraint)
                print(f"  ✓ {constraint.split('CONSTRAINT ')[1].split(' IF')[0]}")
            except Exception as e:
                print(f"  ⚠ Constraint may already exist: {e}")

        print("\nCreating indexes...")
        for index in indexes:
            try:
                session.run(index)
                print(f"  ✓ {index.split('INDEX ')[1].split(' IF')[0]}")
            except Exception as e:
                print(f"  ⚠ Index may already exist: {e}")

    print("\n✓ Schema initialization complete!")


def verify_schema(driver):
    """Verify the schema was created correctly."""
    with driver.session() as session:
        result = session.run("SHOW INDEXES")
        indexes = list(result)
        print(f"\nVerification: {len(indexes)} indexes found")

        result = session.run("SHOW CONSTRAINTS")
        constraints = list(result)
        print(f"Verification: {len(constraints)} constraints found")


if __name__ == "__main__":
    print("Initializing Neo4j schema for DocWeave...")
    driver = get_driver()
    try:
        init_schema(driver)
        verify_schema(driver)
    finally:
        driver.close()
