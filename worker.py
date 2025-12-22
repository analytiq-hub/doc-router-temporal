"""Temporal worker to run the list documents workflow."""

import asyncio
import logging
import os
from temporalio.client import Client
from temporalio.worker import Worker

from workflows.list_documents import ListDocumentsWorkflow, ListDocumentsWorkflowAlias
from activities.list_documents import list_documents_activity

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
    
    # Create a worker that knows about our workflow and activity
    worker = Worker(
        client,
        task_queue="doc-router-task-queue",
        workflows=[ListDocumentsWorkflow, ListDocumentsWorkflowAlias],
        activities=[list_documents_activity],
    )
    
    logger.info("Starting worker on task queue: doc-router-task-queue")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

