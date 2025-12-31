"""Client script to start the list documents workflow."""

import asyncio
import logging
import os
import uuid
from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

# Load environment variables from .env file
load_dotenv()

from workflows.list_documents import ListDocumentsWorkflow, ListDocumentsWorkflowAlias

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Main function to start the workflow."""
    # Get configuration from environment
    temporal_host = os.getenv("TEMPORAL_HOST", "localhost")
    temporal_port = int(os.getenv("TEMPORAL_PORT", "7233"))
    temporal_namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    organization_id = os.getenv("DOCROUTER_ORG_ID")
    
    if not organization_id:
        logger.error("DOCROUTER_ORG_ID environment variable is required")
        return
    
    logger.info(f"Connecting to Temporal at {temporal_host}:{temporal_port}")
    
    # Connect to Temporal
    client = await Client.connect(
        f"{temporal_host}:{temporal_port}",
        namespace=temporal_namespace,
    )
    
    logger.info(f"Starting workflow for organization: {organization_id}")
    
    # Generate a unique workflow ID to avoid conflicts
    workflow_id = f"list-documents-{organization_id}-{uuid.uuid4().hex[:8]}"
    
    # Start the workflow
    try:
        handle = await client.start_workflow(
            ListDocumentsWorkflowAlias.run,
            organization_id,
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
    result = await handle.result()
    
    logger.info("Workflow completed!")
    logger.info(f"Total documents: {result.get('total_count', 0)}")
    logger.info(f"Documents returned: {len(result.get('documents', []))}")
    
    # Print document details
    for doc in result.get('documents', []):
        logger.info(f"  - {doc.get('document_name', 'Unknown')} (ID: {doc.get('id')})")


if __name__ == "__main__":
    asyncio.run(main())

