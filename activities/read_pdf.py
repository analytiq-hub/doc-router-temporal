"""Activity to read a PDF file from disk."""

from temporalio import activity
from typing import bytes
import logging

logger = logging.getLogger(__name__)


@activity.defn
async def read_pdf_activity(pdf_path: str) -> bytes:
    """
    Read a PDF file from disk.
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        PDF file as bytes
    """
    # Import os inside the function to avoid determinism issues
    import os
    
    logger.info(f"Reading PDF file: {pdf_path}")
    
    if not os.path.exists(pdf_path):
        raise activity.ApplicationError(
            f"PDF file not found: {pdf_path}",
            type="FileNotFoundError",
            non_retryable=True
        )
    
    try:
        with open(pdf_path, "rb") as f:
            pdf_data = f.read()
        
        logger.info(f"Successfully read PDF file: {len(pdf_data)} bytes")
        return pdf_data
        
    except Exception as e:
        logger.error(f"Error reading PDF file: {e}")
        raise activity.ApplicationError(
            f"Failed to read PDF file: {str(e)}",
            type="FileReadError",
            non_retryable=False
        )

