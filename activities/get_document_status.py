"""Activity to get document status from docrouter."""

from temporalio import activity
from temporalio.exceptions import ApplicationError
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def get_document_status_activity(
    organization_id: str,
    document_id: str
) -> Optional[Dict[str, Any]]:
    """
    Get document status from docrouter.
    
    Args:
        organization_id: The organization/workspace ID
        document_id: The document ID
        
    Returns:
        Dictionary containing document status, or None if not found
    """
    # Import dependencies inside the function to avoid determinism issues
    import os
    import httpx
    
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    url = f"{base_url}/v0/orgs/{organization_id}/documents/{document_id}"
    
    headers = {}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    logger.debug(f"Getting document status for document {document_id}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            if response.status_code == 404:
                logger.warning(f"Document not found: {document_id}")
                return None
            
            response.raise_for_status()
            result = response.json()
            
            status = {
                "document_id": document_id,
                "state": result.get("state", ""),
                "document_name": result.get("document_name", "")
            }
            
            logger.debug(f"Document {document_id} status: {status['state']}")
            return status
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise ApplicationError(
            f"Failed to get document status: {str(e)}",
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

