"""Client script to start the PDF classification workflow."""

import asyncio
import logging
import os
import sys
import uuid
import json
from pathlib import Path
from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

# Load environment variables from .env file
load_dotenv()

from workflows.classify_pdf import ClassifyPDFWorkflowAlias

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Main function to start the PDF classification workflow."""
    # Get configuration from environment
    temporal_host = os.getenv("TEMPORAL_HOST", "10.83.8.98")
    temporal_port = int(os.getenv("TEMPORAL_PORT", "7233"))
    temporal_namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    organization_id = os.getenv("DOCROUTER_ORG_ID")
    
    # Get PDF file path from command line argument
    if len(sys.argv) < 2:
        logger.error("Usage: python client_classify_pdf.py <pdf_file_path> [classification_prompt]")
        logger.error("Example: python client_classify_pdf.py document.pdf 'Classify this document'")
        return
    
    pdf_path = Path(sys.argv[1])
    if not pdf_path.exists():
        logger.error(f"PDF file not found: {pdf_path}")
        return
    
    # Get classification prompt (optional, with default)
    classification_prompt = sys.argv[2] if len(sys.argv) > 2 else "Classify this document page and extract key information."
    
    if not organization_id:
        logger.error("DOCROUTER_ORG_ID environment variable is required")
        return
    
    logger.info(f"Reading PDF file: {pdf_path}")
    
    # Read PDF file as bytes
    try:
        with open(pdf_path, "rb") as f:
            pdf_data = f.read()
        logger.info(f"PDF file loaded: {len(pdf_data)} bytes")
    except Exception as e:
        logger.error(f"Error reading PDF file: {e}")
        return
    
    logger.info(f"Connecting to Temporal at {temporal_host}:{temporal_port}")
    
    # Connect to Temporal
    client = await Client.connect(
        f"{temporal_host}:{temporal_port}",
        namespace=temporal_namespace,
    )
    
    logger.info(f"Starting PDF classification workflow for organization: {organization_id}")
    logger.info(f"Classification prompt: {classification_prompt}")
    
    # Generate a unique workflow ID
    workflow_id = f"classify-pdf-{organization_id}-{uuid.uuid4().hex[:8]}"
    
    # Start the workflow
    try:
        handle = await client.start_workflow(
            ClassifyPDFWorkflowAlias.run,
            organization_id,
            pdf_data,
            classification_prompt,
            max_concurrent_classifications=5,
            id=workflow_id,
            task_queue="doc-router-task-queue",
        )
        logger.info(f"Workflow started with ID: {handle.id}")
        logger.info(f"Workflow run ID: {handle.result_run_id}")
    except WorkflowAlreadyStartedError as e:
        logger.warning(f"Workflow already running with ID: {e.id}, getting existing handle...")
        handle = client.get_workflow_handle(e.id, run_id=e.run_id)
        logger.info(f"Using existing workflow with ID: {handle.id}")
    
    # Wait for the workflow to complete
    logger.info("Waiting for workflow to complete...")
    result = await handle.result()
    
    logger.info("Workflow completed!")
    logger.info(f"Total pages processed: {result.get('total_pages', 0)}")
    logger.info(f"Pages classified: {result.get('summary', {}).get('pages_classified', 0)}")
    
    # Display results as JSON
    print("\n" + "="*80)
    print("CLASSIFICATION RESULTS (JSON)")
    print("="*80)
    print(json.dumps(result, indent=2, default=str))
    print("="*80)
    
    # Also save to file
    output_file = pdf_path.stem + "_classification_results.json"
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2, default=str)
    logger.info(f"Results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())

