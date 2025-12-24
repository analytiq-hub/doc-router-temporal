"""Activity to poll document status until it reaches a completed state."""

from temporalio import activity
from temporalio.exceptions import ApplicationError
from typing import Dict, Any
import logging
import asyncio

logger = logging.getLogger(__name__)

# Document states
DOCUMENT_STATE_UPLOADED = "uploaded"
DOCUMENT_STATE_OCR_PROCESSING = "ocr_processing"
DOCUMENT_STATE_OCR_COMPLETED = "ocr_completed"
DOCUMENT_STATE_OCR_FAILED = "ocr_failed"
DOCUMENT_STATE_LLM_PROCESSING = "llm_processing"
DOCUMENT_STATE_LLM_COMPLETED = "llm_completed"
DOCUMENT_STATE_LLM_FAILED = "llm_failed"

# Completed states
COMPLETED_STATES = {DOCUMENT_STATE_OCR_COMPLETED, DOCUMENT_STATE_LLM_COMPLETED}

# Failed states that should be retried
FAILED_STATES = {DOCUMENT_STATE_OCR_FAILED, DOCUMENT_STATE_LLM_FAILED}


@activity.defn
async def poll_document_status_activity(
    organization_id: str,
    document_id: str,
    max_wait_seconds: int = 600,
    poll_interval_seconds: int = 5,
    prompt_revid: str = None,
    page_number: int = None
) -> Dict[str, Any]:
    """
    Poll document status until it reaches a completed state.
    
    Args:
        organization_id: The organization/workspace ID
        document_id: The document ID
        max_wait_seconds: Maximum time to wait in seconds (default: 600 = 10 minutes)
        poll_interval_seconds: Interval between polls in seconds (default: 5)
        prompt_revid: The prompt revision ID (optional)
        page_number: The page number for this document (optional, for UI display)
        
    Returns:
        Dictionary containing:
        - status: "completed", "failed", or "timeout"
        - state: The final document state
        - document_id: The document ID
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
    
    logger.info(f"Polling document {document_id} status (max wait: {max_wait_seconds}s)")
    
    start_time = asyncio.get_event_loop().time()
    max_wait_time = start_time + max_wait_seconds
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                current_time = asyncio.get_event_loop().time()
                if current_time >= max_wait_time:
                    logger.warning(f"Timeout waiting for document {document_id} to complete")
                    return {
                        "status": "timeout",
                        "state": "unknown",
                        "document_id": document_id
                    }
                
                try:
                    response = await client.get(url, headers=headers)
                    if response.status_code == 404:
                        logger.warning(f"Document {document_id} not found")
                        return {
                            "status": "failed",
                            "state": "not_found",
                            "document_id": document_id
                        }
                    
                    response.raise_for_status()
                    result = response.json()
                    state = result.get("state", "")
                    
                    logger.debug(f"Document {document_id} state: {state}")
                    
                    # Check if completed
                    if state in COMPLETED_STATES:
                        logger.info(f"Document {document_id} completed with state: {state}")
                        return {
                            "status": "completed",
                            "state": state,
                            "document_id": document_id
                        }
                    
                    # Check if failed - return failure status (retries handled in workflow)
                    if state in FAILED_STATES:
                        logger.warning(f"Document {document_id} failed with state: {state}")
                        return {
                            "status": "failed",
                            "state": state,
                            "document_id": document_id
                        }
                    
                    # If still processing, wait and poll again
                    await asyncio.sleep(poll_interval_seconds)
                    
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        logger.warning(f"Document {document_id} not found")
                        return {
                            "status": "failed",
                            "state": "not_found",
                            "document_id": document_id
                        }
                    else:
                        logger.error(f"HTTP error polling document {document_id}: {e.response.status_code}")
                        await asyncio.sleep(poll_interval_seconds)
                        continue
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise ApplicationError(
            f"Failed to poll document status: {str(e)}",
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

