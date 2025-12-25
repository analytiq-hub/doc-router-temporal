"""Activity to create a PDF from specific pages and upload it directly, avoiding passing large binary data through Temporal."""

from temporalio import activity
from typing import List, Dict, Any
import logging
import os

logger = logging.getLogger(__name__)


@activity.defn
async def create_and_upload_patient_pdf_activity(
    pdf_path: str,
    page_numbers: List[int],
    organization_id: str,
    filename: str,
    tag_ids: List[str]
) -> Dict[str, Any]:
    """
    Create a PDF from specific pages and upload it directly to docrouter.
    This avoids passing large binary data through Temporal.
    
    Args:
        pdf_path: Path to the original PDF file
        page_numbers: List of 1-indexed page numbers to include
        organization_id: The organization/workspace ID
        filename: The filename for the uploaded document
        tag_ids: List of tag IDs to associate with the document
        
    Returns:
        Dictionary containing:
        - document_id: The ID of the uploaded document
        - document_name: The name of the document
    """
    # Import dependencies inside the function to avoid determinism issues
    import io
    import base64
    import httpx
    from PyPDF2 import PdfReader, PdfWriter
    
    logger.info(f"Creating and uploading PDF from pages {page_numbers} of {pdf_path}")
    
    if not os.path.exists(pdf_path):
        raise activity.ApplicationError(
            f"PDF file not found: {pdf_path}",
            type="FileNotFoundError",
            non_retryable=True
        )
    
    try:
        # Read the PDF file
        with open(pdf_path, "rb") as f:
            pdf_data = f.read()
        
        # Read the PDF from bytes
        pdf_reader = PdfReader(io.BytesIO(pdf_data))
        total_pages = len(pdf_reader.pages)
        
        logger.info(f"Original PDF has {total_pages} pages, extracting pages: {page_numbers}")
        
        # Create a new PDF writer
        pdf_writer = PdfWriter()
        
        # Add the specified pages (convert to 0-indexed)
        for page_num in page_numbers:
            if page_num < 1 or page_num > total_pages:
                logger.warning(f"Page {page_num} is out of range (1-{total_pages}), skipping")
                continue
            pdf_writer.add_page(pdf_reader.pages[page_num - 1])  # Convert to 0-indexed
            logger.debug(f"Added page {page_num} to patient PDF")
        
        # Write the PDF to bytes
        pdf_buffer = io.BytesIO()
        pdf_writer.write(pdf_buffer)
        patient_pdf_data = pdf_buffer.getvalue()
        
        logger.info(f"Created patient PDF with {len(page_numbers)} pages, size: {len(patient_pdf_data)} bytes")
        
        # Upload directly to docrouter (don't return the PDF bytes)
        base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
        api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
        
        url = f"{base_url}/v0/orgs/{organization_id}/documents"
        
        # Encode PDF as base64
        pdf_base64 = base64.b64encode(patient_pdf_data).decode('utf-8')
        
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
        
        logger.info(f"Uploading patient PDF '{filename}' to docrouter with {len(tag_ids)} tags")
        
        async with httpx.AsyncClient(timeout=120.0) as client:
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
            
            logger.info(f"Successfully uploaded patient PDF '{filename}' with ID: {document_id}")
            return {
                "document_id": document_id,
                "document_name": filename
            }
        
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise activity.ApplicationError(
            f"Failed to upload patient PDF: {str(e)}",
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
    except Exception as e:
        logger.error(f"Error creating and uploading patient PDF: {e}")
        raise activity.ApplicationError(
            f"Failed to create and upload patient PDF: {str(e)}",
            type="PDFCreationError",
            non_retryable=False
        )

