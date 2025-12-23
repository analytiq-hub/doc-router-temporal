"""Activity to wait for prompt completion on a document."""

from temporalio import activity
from typing import Dict, Any
import logging
import asyncio

logger = logging.getLogger(__name__)


@activity.defn
async def wait_for_prompt_activity(
    organization_id: str,
    document_id: str,
    prompt_revid: str,
    max_wait_seconds: int = 300,
    poll_interval_seconds: int = 5
) -> Dict[str, Any]:
    """
    Wait for a prompt to complete running on a document by polling the LLM result endpoint.
    
    Args:
        organization_id: The organization/workspace ID
        document_id: The document ID
        prompt_revid: The prompt revision ID
        max_wait_seconds: Maximum time to wait in seconds (default: 300 = 5 minutes)
        poll_interval_seconds: Interval between polls in seconds (default: 5)
        
    Returns:
        Dictionary containing:
        - status: "completed" or "timeout"
        - result: The LLM result if completed, None if timeout
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
    
    logger.info(f"Waiting for prompt {prompt_revid} to complete on document {document_id}")
    
    start_time = asyncio.get_event_loop().time()
    max_wait_time = start_time + max_wait_seconds
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                current_time = asyncio.get_event_loop().time()
                if current_time >= max_wait_time:
                    logger.warning(f"Timeout waiting for prompt {prompt_revid} on document {document_id}")
                    return {
                        "status": "timeout",
                        "result": None
                    }
                
                try:
                    response = await client.get(url, params=params, headers=headers)
                    if response.status_code == 200:
                        result = response.json()
                        logger.info(f"Prompt {prompt_revid} completed on document {document_id}")
                        return {
                            "status": "completed",
                            "result": result
                        }
                    elif response.status_code == 404:
                        # Result not ready yet, wait and poll again
                        logger.debug(f"Prompt {prompt_revid} not yet complete on document {document_id}, waiting...")
                        await asyncio.sleep(poll_interval_seconds)
                        continue
                    else:
                        response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        # Result not ready yet, wait and poll again
                        logger.debug(f"Prompt {prompt_revid} not yet complete on document {document_id}, waiting...")
                        await asyncio.sleep(poll_interval_seconds)
                        continue
                    else:
                        raise
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise activity.ApplicationError(
            f"Failed to wait for prompt: {str(e)}",
            type="DocRouterAPIError",
            non_retryable=False
        )
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error from docrouter API: {e.response.status_code} - URL: {url} - {e.response.text}")
        raise activity.ApplicationError(
            f"HTTP error from docrouter API: {e.response.status_code}",
            type="DocRouterAPIError",
            non_retryable=False
        )

