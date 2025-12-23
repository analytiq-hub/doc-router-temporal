"""Activity to trigger a prompt run on a document."""

from temporalio import activity
from temporalio.exceptions import ApplicationError
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def run_prompt_activity(
    organization_id: str,
    document_id: str,
    prompt_revid: str,
    force: bool = False
) -> Dict[str, Any]:
    """
    Trigger a prompt run on a document.
    
    Args:
        organization_id: The organization/workspace ID
        document_id: The document ID
        prompt_revid: The prompt revision ID
        force: Whether to force a new run even if result exists (default: False)
        
    Returns:
        Dictionary containing the run status
    """
    # Import dependencies inside the function to avoid determinism issues
    import os
    import httpx
    
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    url = f"{base_url}/v0/orgs/{organization_id}/llm/run/{document_id}"
    params = {
        "prompt_revid": prompt_revid,
        "force": force
    }
    
    headers = {}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    logger.info(f"Triggering prompt {prompt_revid} run on document {document_id}")
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(url, params=params, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Successfully triggered prompt run on document {document_id}")
            return result
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise ApplicationError(
            f"Failed to run prompt: {str(e)}",
            type="DocRouterAPIError",
            non_retryable=False
        )
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error from docrouter API: {e.response.status_code} - URL: {url} - {e.response.text}")
        raise ApplicationError(
            f"HTTP error from docrouter API: {e.response.status_code}",
            type="DocRouterAPIError",
            non_retryable=False
        )

