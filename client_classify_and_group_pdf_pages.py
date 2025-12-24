"""Client script to start the PDF pages classification and grouping workflow."""

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

from workflows.classify_and_group_pdf_pages import ClassifyAndGroupPDFPagesWorkflowAlias

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Main function to start the PDF pages classification and grouping workflow."""
    # Get configuration from environment
    temporal_host = os.getenv("TEMPORAL_HOST", "10.83.8.98")
    temporal_port = int(os.getenv("TEMPORAL_PORT", "7233"))
    temporal_namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    organization_id = os.getenv("DOCROUTER_ORG_ID")
    
    # Get PDF file path from environment or command line
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
    else:
        pdf_path = os.getenv("TEST_PDF")
    
    if not pdf_path:
        logger.error("Usage: python client_classify_and_group_pdf_pages.py <pdf_file_path> [tag_name] [prompt_name] [max_retries]")
        logger.error("Or set TEST_PDF environment variable")
        logger.error("Example: python client_classify_and_group_pdf_pages.py document.pdf")
        logger.error("Example: python client_classify_and_group_pdf_pages.py document.pdf anesthesia_bundle_page_classifier anesthesia_bundle_page_classifier 3")
        return
    
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        logger.error(f"PDF file not found: {pdf_path}")
        return
    
    # Get tag and prompt names (optional, with defaults)
    tag_name = sys.argv[2] if len(sys.argv) > 2 else "anesthesia_bundle_page_classifier"
    prompt_name = sys.argv[3] if len(sys.argv) > 3 else "anesthesia_bundle_page_classifier"
    max_retries = int(sys.argv[4]) if len(sys.argv) > 4 else 2
    
    if not organization_id:
        logger.error("DOCROUTER_ORG_ID environment variable is required")
        return
    
    logger.info(f"PDF file: {pdf_path}")
    logger.info(f"Tag name: {tag_name}")
    logger.info(f"Prompt name: {prompt_name}")
    logger.info(f"Max retries: {max_retries}")
    
    logger.info(f"Connecting to Temporal at {temporal_host}:{temporal_port}")
    
    # Connect to Temporal
    client = await Client.connect(
        f"{temporal_host}:{temporal_port}",
        namespace=temporal_namespace,
    )
    
    logger.info(f"Starting PDF pages classification and grouping workflow for organization: {organization_id}")
    
    # Generate a unique workflow ID
    workflow_id = f"classify-and-group-pdf-pages-{organization_id}-{uuid.uuid4().hex[:8]}"
    
    # Start the workflow
    try:
        handle = await client.start_workflow(
            ClassifyAndGroupPDFPagesWorkflowAlias.run,
            args=(organization_id, str(pdf_path), tag_name, prompt_name, max_retries),
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
    
    # Format the results with page ranges and classification information
    pages = result.get("pages", [])
    schedule_pages = result.get("schedule", [])
    patients = result.get("patients", {})
    
    # Format patients with page numbers
    formatted_patients = {}
    for patient_key, page_numbers in patients.items():
        formatted_patients[patient_key] = {
            "pages": sorted(page_numbers)
        }
    
    # Format schedule with page numbers
    formatted_schedule = {
        "pages": sorted(schedule_pages)
    }
    
    # Build the formatted result
    formatted_result = {
        "file_name": result.get("file_name", ""),
        "patients": formatted_patients,
        "schedule": formatted_schedule,
        "pages": pages
    }
    
    logger.info(f"Surgery schedule pages: {len(schedule_pages)}")
    logger.info(f"Patients found: {len(patients)}")
    
    # Display results as JSON
    print("\n" + "="*80)
    print("CLASSIFICATION RESULTS WITH PAGE RANGES (JSON)")
    print("="*80)
    print(json.dumps(formatted_result, indent=2, default=str))
    print("="*80)
    
    # Also save to file
    output_file = pdf_path.stem + "_grouped_results.json"
    with open(output_file, "w") as f:
        json.dump(formatted_result, f, indent=2, default=str)
    logger.info(f"Results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())

