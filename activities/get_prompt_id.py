"""Activity to get prompt ID from prompt name."""

from temporalio import activity
from typing import Optional
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def get_prompt_id_activity(organization_id: str, prompt_name: str) -> Optional[str]:
    """
    Get prompt revision ID from prompt name.
    
    Args:
        organization_id: The organization/workspace ID
        prompt_name: The name of the prompt
        
    Returns:
        Prompt revision ID as string, or None if not found
    """
    # Import dependencies inside the function to avoid determinism issues
    import os
    import httpx
    
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    url = f"{base_url}/v0/orgs/{organization_id}/prompts"
    params = {
        "name_search": prompt_name,
        "limit": 100
    }
    
    headers = {}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    logger.info(f"Searching for prompt '{prompt_name}' in organization {organization_id}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            # Find exact match (case-insensitive)
            prompts = result.get("prompts", [])
            for prompt in prompts:
                if prompt.get("name", "").lower() == prompt_name.lower():
                    prompt_revid = prompt.get("prompt_revid")
                    logger.info(f"Found prompt '{prompt_name}' with revision ID: {prompt_revid}")
                    return prompt_revid
            
            logger.warning(f"Prompt '{prompt_name}' not found")
            return None
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise activity.ApplicationError(
            f"Failed to get prompt ID: {str(e)}",
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

