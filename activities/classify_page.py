"""Activity to classify a single PDF page using docrouter API."""

from temporalio import activity
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def classify_page_activity(
    organization_id: str,
    page_data: bytes,
    page_number: int,
    classification_prompt: str
) -> Dict[str, Any]:
    """
    Classify a single PDF page using docrouter API.
    
    Args:
        organization_id: The organization/workspace ID
        page_data: PDF bytes for the single page
        page_number: The page number (1-indexed)
        classification_prompt: The prompt to use for classification
        
    Returns:
        Dictionary containing:
        - page_number: The page number
        - classification_result: The result from docrouter API
    """
    # Import dependencies inside the function to avoid determinism issues
    import os
    import httpx
    import base64
    
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    # Encode the PDF page as base64 for the API
    page_base64 = base64.b64encode(page_data).decode('utf-8')
    
    # Construct the API endpoint - adjust this based on your actual docrouter API
    url = f"{base_url}/v0/orgs/{organization_id}/classify"
    
    headers = {
        "Content-Type": "application/json"
    }
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    # Prepare the request payload
    payload = {
        "pdf_data": page_base64,
        "prompt": classification_prompt,
        "page_number": page_number
    }
    
    logger.info(f"Classifying page {page_number} via docrouter API")
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Successfully classified page {page_number}")
            return {
                "page_number": page_number,
                "classification_result": result
            }
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API for page {page_number}: {e}")
        raise activity.ApplicationError(
            f"Failed to classify page {page_number}: {str(e)}",
            type="DocRouterAPIError",
            non_retryable=False
        )
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error from docrouter API for page {page_number}: {e.response.status_code} - {e.response.text}")
        raise activity.ApplicationError(
            f"HTTP error from docrouter API for page {page_number}: {e.response.status_code}",
            type="DocRouterAPIError",
            non_retryable=False
        )

