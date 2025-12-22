"""Activity to list documents from docrouter FastAPI."""

from temporalio import activity
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def list_documents_activity(
    organization_id: str,
    skip: int = 0,
    limit: int = 100
) -> Dict[str, Any]:
    """
    Activity that calls the docrouter FastAPI to list documents.
    
    Args:
        organization_id: The organization/workspace ID
        skip: Number of documents to skip (for pagination)
        limit: Maximum number of documents to return
        
    Returns:
        Dictionary containing documents list and metadata
    """
    # Import os inside the function to avoid determinism issues in workflows
    import os
    
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    url = f"{base_url}/v0/orgs/{organization_id}/documents"
    params = {
        "skip": skip,
        "limit": limit
    }
    
    headers = {}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    logger.info(f"Calling docrouter API: {url} with params: {params}")
    
    # Import httpx inside the function to avoid determinism issues in workflows
    import httpx
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Successfully retrieved {result.get('total_count', 0)} documents")
            return result
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise activity.ApplicationError(
            f"Failed to list documents: {str(e)}",
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

