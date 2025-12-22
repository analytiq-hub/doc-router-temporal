"""Activity to chunk a PDF into single-page PDFs with naming convention."""

from temporalio import activity
from typing import List, Dict, Any
import logging
import os

logger = logging.getLogger(__name__)


@activity.defn
async def chunk_pdf_activity(pdf_data: bytes, filename: str) -> List[Dict[str, Any]]:
    """
    Chunk a PDF into individual page PDFs with naming convention <filename>-1.pdf, <filename>-2.pdf, etc.
    
    Args:
        pdf_data: The PDF file as bytes
        filename: The base filename (without extension) for naming chunks
        
    Returns:
        List of dictionaries, each containing:
        - page_number: 1-indexed page number
        - page_data: PDF bytes for that page
        - filename: The chunk filename (e.g., "document-1.pdf")
    """
    # Import PyPDF2 inside the function to avoid determinism issues
    import io
    from PyPDF2 import PdfReader, PdfWriter
    
    logger.info(f"Chunking PDF '{filename}' of {len(pdf_data)} bytes into pages")
    
    try:
        # Read the PDF from bytes
        pdf_reader = PdfReader(io.BytesIO(pdf_data))
        total_pages = len(pdf_reader.pages)
        
        logger.info(f"PDF has {total_pages} pages")
        
        # Remove extension from filename if present
        base_name = os.path.splitext(filename)[0]
        
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
            chunk_filename = f"{base_name}-{page_num + 1}.pdf"
            
            pages.append({
                "page_number": page_num + 1,  # 1-indexed
                "page_data": page_data,
                "filename": chunk_filename,
                "total_pages": total_pages
            })
            
            logger.info(f"Extracted page {page_num + 1}/{total_pages} as '{chunk_filename}'")
        
        logger.info(f"Successfully chunked PDF into {len(pages)} pages")
        return pages
        
    except Exception as e:
        logger.error(f"Error chunking PDF: {e}")
        raise activity.ApplicationError(
            f"Failed to chunk PDF: {str(e)}",
            type="PDFChunkError",
            non_retryable=False
        )

