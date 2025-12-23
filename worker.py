"""Temporal worker to run the list documents workflow."""

import asyncio
import logging
import os
from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

# Load environment variables from .env file
load_dotenv()

from workflows.list_documents import ListDocumentsWorkflow, ListDocumentsWorkflowAlias
from workflows.classify_pdf_pages import ClassifyPDFPagesWorkflow, ClassifyPDFPagesWorkflowAlias
from workflows.classify_and_group_pdf_pages import ClassifyAndGroupPDFPagesWorkflow, ClassifyAndGroupPDFPagesWorkflowAlias
from activities.list_documents import list_documents_activity
from activities.chunk_and_upload_pdf import chunk_and_upload_pdf_activity
from activities.get_tag_id import get_tag_id_activity
from activities.get_prompt_id import get_prompt_id_activity
from activities.upload_document import upload_document_activity
from activities.run_prompt import run_prompt_activity
from activities.wait_for_prompt import wait_for_prompt_activity
from activities.get_classification_result import get_classification_result_activity
from activities.group_classification_results import group_classification_results_activity

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Main function to start the Temporal worker."""
    # Get Temporal connection details from environment
    temporal_host = os.getenv("TEMPORAL_HOST", "10.83.8.98")
    temporal_port = int(os.getenv("TEMPORAL_PORT", "7233"))
    temporal_namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    
    logger.info(f"Connecting to Temporal at {temporal_host}:{temporal_port}")
    
    # Connect to Temporal
    client = await Client.connect(
        f"{temporal_host}:{temporal_port}",
        namespace=temporal_namespace,
    )
    
    logger.info(f"Connected to Temporal namespace: {temporal_namespace}")

    # Configure sandbox with passthrough for standard library modules
    # This is needed to avoid sandbox restrictions on logging and traceback modules
    sandbox_restrictions = SandboxRestrictions.default.with_passthrough_modules(
        "logging",
        "traceback",
        "linecache",
    )

    # Create a worker that knows about our workflows and activities
    worker = Worker(
        client,
        task_queue="doc-router-task-queue",
        workflows=[
            ListDocumentsWorkflow,
            ListDocumentsWorkflowAlias,
            ClassifyPDFPagesWorkflow,
            ClassifyPDFPagesWorkflowAlias,
            ClassifyAndGroupPDFPagesWorkflow,
            ClassifyAndGroupPDFPagesWorkflowAlias,
        ],
        activities=[
            list_documents_activity,
            chunk_and_upload_pdf_activity,
            get_tag_id_activity,
            get_prompt_id_activity,
            upload_document_activity,
            run_prompt_activity,
            wait_for_prompt_activity,
            get_classification_result_activity,
            group_classification_results_activity,
        ],
        workflow_runner=SandboxedWorkflowRunner(restrictions=sandbox_restrictions),
    )
    
    logger.info("Starting worker on task queue: doc-router-task-queue")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

