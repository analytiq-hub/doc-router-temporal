"""Activity to read a PDF file from disk and chunk it into single-page PDFs."""

from temporalio import activity
from typing import List, Dict, Any
import logging
import os

logger = logging.getLogger(__name__)


@activity.defn
async def read_and_chunk_pdf_activity(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Read a PDF file from disk and chunk it into individual page PDFs with naming convention.
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        List of dictionaries, each containing:
        - page_number: 1-indexed page number
        - page_data: PDF bytes for that page
        - filename: The chunk filename (e.g., "document-1.pdf")
        - base_filename: The base filename without extension
    """
    # Import dependencies inside the function to avoid determinism issues
    import io
    from PyPDF2 import PdfReader, PdfWriter
    
    logger.info(f"Reading and chunking PDF file: {pdf_path}")
    
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
        
        logger.info(f"PDF file loaded: {len(pdf_data)} bytes")
        
        # Extract filename from path
        filename = os.path.basename(pdf_path)
        base_filename = os.path.splitext(filename)[0]
        
        # Read the PDF from bytes
        pdf_reader = PdfReader(io.BytesIO(pdf_data))
        total_pages = len(pdf_reader.pages)
        
        logger.info(f"PDF has {total_pages} pages")
        
        pages = []
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
            
            pages.append({
                "page_number": page_num + 1,  # 1-indexed
                "page_data": page_data,
                "filename": chunk_filename,
                "total_pages": total_pages
            })
            
            logger.info(f"Extracted page {page_num + 1}/{total_pages} as '{chunk_filename}'")
        
        # Add base_filename to the first page for easy access
        if pages:
            pages[0]["base_filename"] = base_filename
        
        logger.info(f"Successfully read and chunked PDF into {len(pages)} pages")
        return pages
        
    except Exception as e:
        logger.error(f"Error reading and chunking PDF: {e}")
        raise activity.ApplicationError(
            f"Failed to read and chunk PDF: {str(e)}",
            type="PDFReadChunkError",
            non_retryable=False
        )

