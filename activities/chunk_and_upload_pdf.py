"""Activity to read a PDF, chunk it, and upload each page to docrouter."""

from temporalio import activity
from typing import List, Dict, Any
import logging
import os
import base64

logger = logging.getLogger(__name__)


@activity.defn
async def chunk_and_upload_pdf_activity(
    pdf_path: str,
    organization_id: str,
    tag_id: str
) -> List[Dict[str, Any]]:
    """
    Read a PDF file, chunk it into pages, and upload each page to docrouter.
    This avoids passing large binary data through Temporal.
    
    Args:
        pdf_path: Path to the PDF file
        organization_id: The organization/workspace ID
        tag_id: The tag ID to associate with uploaded documents
        
    Returns:
        List of dictionaries, each containing:
        - page_number: 1-indexed page number
        - document_id: The document ID from docrouter
        - filename: The chunk filename (e.g., "document-1.pdf")
    """
    # Import dependencies inside the function to avoid determinism issues
    import io
    import httpx
    from PyPDF2 import PdfReader, PdfWriter
    
    logger.info(f"Reading, chunking, and uploading PDF file: {pdf_path}")
    
    if not os.path.exists(pdf_path):
        raise activity.ApplicationError(
            f"PDF file not found: {pdf_path}",
            type="FileNotFoundError",
            non_retryable=True
        )
    
    # Get docrouter configuration
    base_url = os.getenv("DOCROUTER_BASE_URL", "http://localhost:8000")
    api_token = os.getenv("DOCROUTER_ORG_API_TOKEN")
    
    headers = {
        "Content-Type": "application/json"
    }
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    
    upload_url = f"{base_url}/v0/orgs/{organization_id}/documents"
    
    try:
        # Read the PDF file
        with open(pdf_path, "rb") as f:
            pdf_data = f.read()
        
        logger.info(f"PDF file loaded: {len(pdf_data)} bytes")
        
        # Extract filename from path
        filename = os.path.basename(pdf_path)
        base_filename = os.path.splitext(filename)[0]
        
        # Read the PDF from bytes
        pdf_reader = PdfReader(io.BytesIO(pdf_data))
        total_pages = len(pdf_reader.pages)
        
        logger.info(f"PDF has {total_pages} pages")
        
        uploaded_pages = []
        
        # Process each page: chunk and upload immediately
        for page_num in range(total_pages):
            # Create a new PDF writer for this page
            pdf_writer = PdfWriter()
            pdf_writer.add_page(pdf_reader.pages[page_num])
            
            # Write the single page to bytes
            page_buffer = io.BytesIO()
            pdf_writer.write(page_buffer)
            page_data = page_buffer.getvalue()
            
            # Create filename with naming convention: <filename>-<page_number>.pdf
            chunk_filename = f"{base_filename}-{page_num + 1}.pdf"
            
            # Encode PDF as base64 for upload
            page_base64 = base64.b64encode(page_data).decode('utf-8')
            
            # Prepare the upload payload
            payload = {
                "documents": [
                    {
                        "name": chunk_filename,
                        "content": page_base64,
                        "tag_ids": [tag_id],
                        "metadata": {}
                    }
                ]
            }
            
            # Upload the page immediately
            logger.info(f"Uploading page {page_num + 1}/{total_pages} as '{chunk_filename}'")
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(upload_url, json=payload, headers=headers)
                response.raise_for_status()
                result = response.json()
                
                # Extract document info from response
                documents = result.get("documents", [])
                if not documents:
                    raise activity.ApplicationError(
                        f"No document returned from upload for page {page_num + 1}",
                        type="DocRouterAPIError",
                        non_retryable=False
                    )
                
                document = documents[0]
                document_id = document.get("document_id")
                
                uploaded_pages.append({
                    "page_number": page_num + 1,  # 1-indexed
                    "document_id": document_id,
                    "filename": chunk_filename,
                    "total_pages": total_pages
                })
                
                logger.info(f"Successfully uploaded page {page_num + 1}/{total_pages} with document ID: {document_id}")
        
        # Add base_filename to the first page for easy access
        if uploaded_pages:
            uploaded_pages[0]["base_filename"] = base_filename
        
        logger.info(f"Successfully read, chunked, and uploaded PDF into {len(uploaded_pages)} pages")
        return uploaded_pages
        
    except httpx.RequestError as e:
        logger.error(f"Error calling docrouter API: {e}")
        raise activity.ApplicationError(
            f"Failed to upload documents: {str(e)}",
            type="DocRouterAPIError",
            non_retryable=False
        )
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error from docrouter API: {e.response.status_code} - URL: {upload_url} - {e.response.text}")
        raise activity.ApplicationError(
            f"HTTP error from docrouter API: {e.response.status_code}",
            type="DocRouterAPIError",
            non_retryable=False
        )
    except Exception as e:
        logger.error(f"Error reading, chunking, and uploading PDF: {e}")
        raise activity.ApplicationError(
            f"Failed to process PDF: {str(e)}",
            type="PDFProcessError",
            non_retryable=False
        )

