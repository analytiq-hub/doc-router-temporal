"""Activity to retrieve classification result from docrouter."""

from temporalio import activity
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def get_classification_result_activity(
    organization_id: str,
    document_id: str,
    prompt_revid: str
) -> Optional[Dict[str, Any]]:
    """
    Retrieve classification result from docrouter.
    
    Args:
        organization_id: The organization/workspace ID
        document_id: The document ID
        prompt_revid: The prompt revision ID
        
    Returns:
        Dictionary containing the LLM result, or None if not found
    """
    # Import dependencies inside the function to avoid determinism issues
    import os
    import httpx
    
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    url = f"{base_url}/v0/orgs/{organization_id}/llm/result/{document_id}"
    params = {
        "prompt_revid": prompt_revid
    }
    
    headers = {}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    logger.info(f"Retrieving classification result for document {document_id} with prompt {prompt_revid}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params, headers=headers)
            if response.status_code == 404:
                logger.warning(f"Classification result not found for document {document_id}")
                return None
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Successfully retrieved classification result for document {document_id}")
            return result
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise activity.ApplicationError(
            f"Failed to get classification result: {str(e)}",
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

