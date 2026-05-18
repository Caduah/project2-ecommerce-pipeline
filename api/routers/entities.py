"""
api/routers/entities.py
Entity resolution endpoints.
"""
from fastapi import APIRouter, HTTPException, Depends
from api.core.database import get_neo4j_driver
from api.schemas.models import EntityProfile
import logging

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/{entity_id}", response_model=list[EntityProfile])
async def get_entity_profiles(
    entity_id: str,
    neo4j=Depends(get_neo4j_driver),
):
    """
    Returns all customer profiles that resolve to the same real-world entity.
    Uses Neo4j SAME_AS edges from the entity resolution pipeline.
    """
    cypher = """
        MATCH (c:Customer {resolved_entity_id: $entity_id})
        OPTIONAL MATCH (c)-[:SAME_AS]-(linked:Customer)
        RETURN
            c.customer_id           AS customer_id,
            c.source_system         AS source_system,
            c.email_normalised      AS email,
            c.er_confidence         AS er_confidence,
            collect(DISTINCT linked.customer_id) AS linked_profiles
    """
    try:
        with neo4j.session() as session:
            result = session.run(cypher, entity_id=entity_id)
            records = result.data()
            if not records:
                raise HTTPException(
                    status_code=404,
                    detail=f"Entity {entity_id} not found"
                )
            return [
                EntityProfile(
                    customer_id    = r["customer_id"],
                    source_system  = r["source_system"],
                    email          = r["email"],
                    er_confidence  = r["er_confidence"],
                    linked_profiles= r["linked_profiles"] or [],
                )
                for r in records
            ]
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error fetching entity {entity_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
