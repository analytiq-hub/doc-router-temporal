"""Activity to retry a failed document by triggering LLM run."""

from temporalio import activity
from temporalio.exceptions import ApplicationError
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def retry_failed_document_activity(
    organization_id: str,
    document_id: str,
    prompt_revid: str,
    failed_state: str
) -> Dict[str, Any]:
    """
    Retry a failed document by triggering LLM run (for LLM failures) or re-uploading (for OCR failures).
    
    Args:
        organization_id: The organization/workspace ID
        document_id: The document ID
        prompt_revid: The prompt revision ID (for LLM retries)
        failed_state: The failed state (ocr_failed or llm_failed)
        
    Returns:
        Dictionary containing retry status
    """
    # Import dependencies inside the function to avoid determinism issues
    import os
    import httpx
    
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    headers = {}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    # For LLM failures, trigger the LLM run again
    if failed_state == "llm_failed":
        url = f"{base_url}/v0/orgs/{organization_id}/llm/run/{document_id}"
        params = {
            "prompt_revid": prompt_revid,
            "force": True  # Force a new run
        }
        
        logger.info(f"Retrying LLM run for document {document_id}")
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(url, params=params, headers=headers)
                response.raise_for_status()
                result = response.json()
                
                logger.info(f"Successfully triggered LLM retry for document {document_id}")
                return {
                    "status": "retry_triggered",
                    "document_id": document_id
                }
                
        except httpx.RequestError as e:
            logger.error(f"Error calling docrouter API: {e}")
            raise ApplicationError(
                f"Failed to retry document: {str(e)}",
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
    
    # For OCR failures, wait a bit and check again (OCR might retry automatically)
    # If it doesn't work, the document will remain failed
    elif failed_state == "ocr_failed":
        logger.info(f"Document {document_id} has OCR failure - waiting and will check status again")
        import asyncio
        await asyncio.sleep(10)  # Wait 10 seconds for potential automatic retry
        return {
            "status": "wait_and_check",
            "document_id": document_id,
            "reason": "OCR failure - waiting for potential automatic retry"
        }
    
    else:
        logger.warning(f"Unknown failed state: {failed_state}")
        return {
            "status": "unknown_state",
            "document_id": document_id
        }

