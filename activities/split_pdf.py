"""Activity to split a PDF into individual page PDFs."""

from temporalio import activity
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def split_pdf_activity(pdf_data: bytes) -> List[Dict[str, Any]]:
    """
    Split a PDF into individual page PDFs.
    
    Args:
        pdf_data: The PDF file as bytes
        
    Returns:
        List of dictionaries, each containing:
        - page_number: 1-indexed page number
        - page_data: PDF bytes for that page
    """
    # Import PyPDF2 inside the function to avoid determinism issues
    import io
    from PyPDF2 import PdfReader, PdfWriter
    
    logger.info(f"Splitting PDF of {len(pdf_data)} bytes into pages")
    
    try:
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
            
            pages.append({
                "page_number": page_num + 1,  # 1-indexed
                "page_data": page_data,
                "total_pages": total_pages
            })
            
            logger.info(f"Extracted page {page_num + 1}/{total_pages}")
        
        logger.info(f"Successfully split PDF into {len(pages)} pages")
        return pages
        
    except Exception as e:
        logger.error(f"Error splitting PDF: {e}")
        raise activity.ApplicationError(
            f"Failed to split PDF: {str(e)}",
            type="PDFSplitError",
            non_retryable=False
        )

