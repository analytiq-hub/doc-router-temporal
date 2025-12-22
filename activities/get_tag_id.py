"""Activity to get tag ID from tag name."""

from temporalio import activity
from typing import Optional
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def get_tag_id_activity(organization_id: str, tag_name: str) -> Optional[str]:
    """
    Get tag ID from tag name.
    
    Args:
        organization_id: The organization/workspace ID
        tag_name: The name of the tag
        
    Returns:
        Tag ID as string, or None if not found
    """
    # Import dependencies inside the function to avoid determinism issues
    import os
    import httpx
    
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    url = f"{base_url}/v0/orgs/{organization_id}/tags"
    params = {
        "name_search": tag_name,
        "limit": 100
    }
    
    headers = {}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    logger.info(f"Searching for tag '{tag_name}' in organization {organization_id}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            # Find exact match (case-insensitive)
            tags = result.get("tags", [])
            for tag in tags:
                if tag.get("name", "").lower() == tag_name.lower():
                    tag_id = tag.get("id")
                    logger.info(f"Found tag '{tag_name}' with ID: {tag_id}")
                    return tag_id
            
            logger.warning(f"Tag '{tag_name}' not found")
            return None
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise activity.ApplicationError(
            f"Failed to get tag ID: {str(e)}",
            type="DocRouterAPIError",
            non_retryable=False
        )
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error from docrouter API: {e.response.status_code} - {e.response.text}")
        raise activity.ApplicationError(
            f"HTTP error from docrouter API: {e.response.status_code}",
            type="DocRouterAPIError",
            non_retryable=False
        )

