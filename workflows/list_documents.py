"""Temporal workflow for listing documents from docrouter."""

from temporalio import workflow
from typing import List, Dict, Any
import logging
from activities.list_documents import list_documents_activity

logger = logging.getLogger(__name__)


@workflow.defn
class ListDocumentsWorkflow:
    """Workflow that lists documents from a docrouter workspace."""

    @workflow.run
    async def run(self, organization_id: str, skip: int = 0, limit: int = 100) -> Dict[str, Any]:
        """
        Execute the workflow to list documents.
        
        Args:
            organization_id: The organization/workspace ID
            skip: Number of documents to skip (for pagination)
            limit: Maximum number of documents to return
            
        Returns:
            Dictionary containing the list of documents and metadata
        """
        logger.info(f"Starting workflow to list documents for organization: {organization_id}")
        
        # Call the activity to list documents
        result = await workflow.execute_activity(
            list_documents_activity,
            organization_id,
            skip,
            limit,
            start_to_close_timeout=60,
        )
        
        logger.info(f"Workflow completed. Found {result.get('total_count', 0)} documents")
        return result


@workflow.defn(name="list-documents")
class ListDocumentsWorkflowAlias:
    """Alias for ListDocumentsWorkflow with a simpler name."""
    
    @workflow.run
    async def run(self, organization_id: str, skip: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Execute the workflow to list documents."""
        workflow_instance = ListDocumentsWorkflow()
        return await workflow_instance.run(organization_id, skip, limit)

