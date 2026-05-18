"""
api/core/config.py — centralised settings
api/core/database.py — connection pool management
"""

# ── config.py ─────────────────────────────────────────────────────
from pydantic_settings import BaseSettings
from functools import lru_cache
import os


class Settings(BaseSettings):
    environment: str = "development"

    # Redshift
    redshift_host:     str = os.environ.get("REDSHIFT_HOST", "localhost")
    redshift_port:     int = 5439
    redshift_db:       str = os.environ.get("REDSHIFT_DB", "project2")
    redshift_user:     str = os.environ.get("REDSHIFT_USER", "admin")
    redshift_password: str = os.environ.get("REDSHIFT_PASSWORD", "")

    # Neo4j
    neo4j_uri:      str = os.environ.get("NEO4J_URI",      "bolt://localhost:7688")
    neo4j_user:     str = os.environ.get("NEO4J_USER",     "neo4j")
    neo4j_password: str = os.environ.get("NEO4J_PASSWORD", "project2neo4j")

    # Snowflake
    snowflake_account:  str = os.environ.get("SNOWFLAKE_ACCOUNT",  "")
    snowflake_user:     str = os.environ.get("SNOWFLAKE_USER",     "")
    snowflake_password: str = os.environ.get("SNOWFLAKE_PASSWORD", "")

    # Anthropic (for RAG)
    anthropic_api_key: str = os.environ.get("ANTHROPIC_API_KEY", "")

    # Local dev — use postgres instead of Redshift
    use_local_db: bool = os.environ.get("USE_LOCAL_DB", "true").lower() == "true"
    local_db_url: str  = os.environ.get(
        "LOCAL_DB_URL",
        "postgresql://airflow:airflow@localhost:5434/airflow"
    )

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
