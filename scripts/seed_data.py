"""Seed Neo4j with sample data for development and testing."""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neo4j import GraphDatabase


def get_driver():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "docweave123")
    return GraphDatabase.driver(uri, auth=(user, password))


SAMPLE_SOURCES = [
    {
        "id": "src_annual_report_2024",
        "source_type": "document",
        "uri": "file:///data/reports/acme_annual_report_2024.pdf",
        "date": "2024-03-15",
        "reliability_score": 0.95,
        "access_level": "internal"
    },
    {
        "id": "src_press_release_jan",
        "source_type": "press_release",
        "uri": "https://acme.com/press/2024/january-update",
        "date": "2024-01-20",
        "reliability_score": 0.90,
        "access_level": "public"
    },
    {
        "id": "src_internal_memo",
        "source_type": "email",
        "uri": "email://internal/memo-2024-02-01",
        "date": "2024-02-01",
        "reliability_score": 0.85,
        "access_level": "confidential"
    }
]

SAMPLE_ENTITIES = [
    {
        "id": "ent_acme_corp",
        "name": "Acme Corporation",
        "type": "ORGANIZATION",
        "aliases": ["Acme Corp", "Acme Inc", "ACME"]
    },
    {
        "id": "ent_john_smith",
        "name": "John Smith",
        "type": "PERSON",
        "aliases": ["J. Smith", "John D. Smith"]
    },
    {
        "id": "ent_widget_pro",
        "name": "Widget Pro",
        "type": "PRODUCT",
        "aliases": ["Widget Pro 2.0", "WP2"]
    },
    {
        "id": "ent_techstart",
        "name": "TechStart Inc",
        "type": "ORGANIZATION",
        "aliases": ["TechStart", "TS Inc"]
    },
    {
        "id": "ent_jane_doe",
        "name": "Jane Doe",
        "type": "PERSON",
        "aliases": ["J. Doe"]
    }
]

SAMPLE_CLAIMS = [
    # Acme Corporation claims
    {
        "id": "claim_acme_revenue_2024",
        "subject_id": "ent_acme_corp",
        "predicate": "annual_revenue",
        "object_value": "$5.2 billion",
        "source_id": "src_annual_report_2024",
        "confidence": 0.98,
        "status": "CURRENT",
        "extracted_text": "Acme Corporation reported annual revenue of $5.2 billion for fiscal year 2024."
    },
    {
        "id": "claim_acme_employees",
        "subject_id": "ent_acme_corp",
        "predicate": "employee_count",
        "object_value": "12,500",
        "source_id": "src_annual_report_2024",
        "confidence": 0.95,
        "status": "CURRENT",
        "extracted_text": "The company employs approximately 12,500 people worldwide."
    },
    {
        "id": "claim_acme_ceo",
        "subject_id": "ent_acme_corp",
        "predicate": "ceo",
        "object_value": "John Smith",
        "source_id": "src_press_release_jan",
        "confidence": 0.99,
        "status": "CURRENT",
        "extracted_text": "John Smith, CEO of Acme Corporation, announced..."
    },

    # John Smith claims
    {
        "id": "claim_john_title",
        "subject_id": "ent_john_smith",
        "predicate": "job_title",
        "object_value": "Chief Executive Officer",
        "source_id": "src_press_release_jan",
        "confidence": 0.99,
        "status": "CURRENT",
        "extracted_text": "John Smith serves as Chief Executive Officer."
    },
    {
        "id": "claim_john_employer",
        "subject_id": "ent_john_smith",
        "predicate": "employer",
        "object_value": "Acme Corporation",
        "source_id": "src_press_release_jan",
        "confidence": 0.99,
        "status": "CURRENT",
        "extracted_text": "John Smith, CEO of Acme Corporation"
    },

    # Widget Pro claims
    {
        "id": "claim_widget_price",
        "subject_id": "ent_widget_pro",
        "predicate": "price",
        "object_value": "$299",
        "source_id": "src_press_release_jan",
        "confidence": 0.95,
        "status": "CURRENT",
        "extracted_text": "Widget Pro is available for $299."
    },
    {
        "id": "claim_widget_launch",
        "subject_id": "ent_widget_pro",
        "predicate": "launch_date",
        "object_value": "2024-02-15",
        "source_id": "src_press_release_jan",
        "confidence": 0.90,
        "status": "CURRENT",
        "extracted_text": "Widget Pro launches February 15, 2024."
    },

    # CONFLICT EXAMPLE: Two different employee counts from different sources
    {
        "id": "claim_acme_employees_memo",
        "subject_id": "ent_acme_corp",
        "predicate": "employee_count",
        "object_value": "13,200",
        "source_id": "src_internal_memo",
        "confidence": 0.88,
        "status": "CONFLICTING",
        "extracted_text": "Current headcount stands at 13,200 as of Q1."
    }
]

CONFLICT_RELATIONSHIPS = [
    ("claim_acme_employees", "claim_acme_employees_memo", "VALUE_MISMATCH")
]


def seed_sources(session):
    """Create source nodes."""
    print("Seeding sources...")
    for source in SAMPLE_SOURCES:
        session.run("""
            MERGE (s:Source {id: $id})
            SET s.source_type = $source_type,
                s.uri = $uri,
                s.date = date($date),
                s.reliability_score = $reliability_score,
                s.access_level = $access_level,
                s.created_at = datetime()
        """, **source)
        print(f"  ✓ {source['id']}")


def seed_entities(session):
    """Create entity nodes."""
    print("\nSeeding entities...")
    for entity in SAMPLE_ENTITIES:
        session.run("""
            MERGE (e:Entity {id: $id})
            SET e.name = $name,
                e.type = $type,
                e.aliases = $aliases,
                e.created_at = datetime(),
                e.updated_at = datetime()
        """, **entity)
        print(f"  ✓ {entity['name']} ({entity['type']})")


def seed_claims(session):
    """Create claim nodes and relationships."""
    print("\nSeeding claims...")
    for claim in SAMPLE_CLAIMS:
        session.run("""
            MATCH (e:Entity {id: $subject_id})
            MATCH (s:Source {id: $source_id})
            MERGE (c:Claim {id: $id})
            SET c.predicate = $predicate,
                c.object_value = $object_value,
                c.confidence = $confidence,
                c.status = $status,
                c.extracted_text = $extracted_text,
                c.created_at = datetime(),
                c.valid_from = datetime()
            MERGE (e)-[:HAS_CLAIM]->(c)
            MERGE (c)-[:SOURCED_FROM]->(s)
        """, **claim)
        print(f"  ✓ {claim['id']}: {claim['predicate']} = {claim['object_value']}")


def seed_conflicts(session):
    """Create conflict relationships."""
    print("\nSeeding conflict relationships...")
    for claim1_id, claim2_id, conflict_type in CONFLICT_RELATIONSHIPS:
        session.run("""
            MATCH (c1:Claim {id: $claim1_id})
            MATCH (c2:Claim {id: $claim2_id})
            MERGE (c1)-[r:CONFLICTS_WITH]->(c2)
            SET r.conflict_type = $conflict_type,
                r.status = 'UNRESOLVED',
                r.detected_at = datetime()
        """, claim1_id=claim1_id, claim2_id=claim2_id, conflict_type=conflict_type)
        print(f"  ✓ Conflict: {claim1_id} <-> {claim2_id}")


def verify_seed(session):
    """Verify seeded data."""
    print("\n--- Verification ---")

    result = session.run("MATCH (e:Entity) RETURN count(e) as count")
    print(f"Entities: {result.single()['count']}")

    result = session.run("MATCH (c:Claim) RETURN count(c) as count")
    print(f"Claims: {result.single()['count']}")

    result = session.run("MATCH (s:Source) RETURN count(s) as count")
    print(f"Sources: {result.single()['count']}")

    result = session.run("MATCH ()-[r:CONFLICTS_WITH]->() RETURN count(r) as count")
    print(f"Conflicts: {result.single()['count']}")


def clear_database(session):
    """Clear all nodes and relationships (use with caution!)."""
    session.run("MATCH (n) DETACH DELETE n")
    print("Database cleared.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Seed DocWeave database")
    parser.add_argument("--clear", action="store_true", help="Clear database before seeding")
    args = parser.parse_args()

    print("Seeding DocWeave database with sample data...")
    driver = get_driver()

    try:
        with driver.session() as session:
            if args.clear:
                clear_database(session)

            seed_sources(session)
            seed_entities(session)
            seed_claims(session)
            seed_conflicts(session)
            verify_seed(session)

        print("\n✓ Seed data complete!")
    finally:
        driver.close()
