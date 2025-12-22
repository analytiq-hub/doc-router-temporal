"""Activity to upload a document to docrouter with tags."""

from temporalio import activity
from typing import Dict, Any
import logging
import base64

logger = logging.getLogger(__name__)


@activity.defn
async def upload_document_activity(
    organization_id: str,
    filename: str,
    pdf_data: bytes,
    tag_ids: list[str]
) -> Dict[str, Any]:
    """
    Upload a document to docrouter with tags.
    
    Args:
        organization_id: The organization/workspace ID
        filename: The filename for the document
        pdf_data: The PDF file as bytes
        tag_ids: List of tag IDs to associate with the document
        
    Returns:
        Dictionary containing:
        - document_id: The ID of the uploaded document
        - document_name: The name of the document
    """
    # Import dependencies inside the function to avoid determinism issues
    import os
    import httpx
    
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    url = f"{base_url}/v0/orgs/{organization_id}/documents"
    
    # Encode PDF as base64
    pdf_base64 = base64.b64encode(pdf_data).decode('utf-8')
    
    # Prepare the request payload
    payload = {
        "documents": [
            {
                "name": filename,
                "content": pdf_base64,
                "tag_ids": tag_ids,
                "metadata": {}
            }
        ]
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    logger.info(f"Uploading document '{filename}' to docrouter with {len(tag_ids)} tags")
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            # Extract document info from response
            documents = result.get("documents", [])
            if not documents:
                raise activity.ApplicationError(
                    "No document returned from upload",
                    type="DocRouterAPIError",
                    non_retryable=False
                )
            
            document = documents[0]
            document_id = document.get("document_id")
            
            logger.info(f"Successfully uploaded document '{filename}' with ID: {document_id}")
            return {
                "document_id": document_id,
                "document_name": filename
            }
            
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise activity.ApplicationError(
            f"Failed to upload document: {str(e)}",
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

