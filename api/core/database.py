"""
api/core/database.py
Connection pool managers for all data sources.
"""

import logging
from typing import Generator
from api.core.config import settings

log = logging.getLogger(__name__)


def get_redshift_pool():
    """
    Returns a database connection.
    Uses local postgres in dev, Redshift in production.
    """
    if settings.use_local_db:
        import psycopg2
        conn = psycopg2.connect(settings.local_db_url)
        try:
            yield conn
        finally:
            conn.close()
    else:
        import redshift_connector
        conn = redshift_connector.connect(
            host     = settings.redshift_host,
            port     = settings.redshift_port,
            database = settings.redshift_db,
            user     = settings.redshift_user,
            password = settings.redshift_password,
        )
        try:
            yield conn
        finally:
            conn.close()


def get_neo4j_driver():
    """Returns a Neo4j driver instance."""
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    try:
        yield driver
    finally:
        driver.close()
