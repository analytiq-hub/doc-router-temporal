"""Temporal workflow for classifying PDF pages and grouping by surgery schedule and patients."""

from datetime import timedelta
from temporalio import workflow
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


@workflow.defn
class ClassifyAndGroupPDFPagesWorkflow:
    """Workflow that classifies PDF pages and groups them by surgery schedule and patients."""

    @workflow.run
    async def run(
        self,
        organization_id: str,
        pdf_path: str,
        tag_name: str = "anesthesia_bundle_page_classifier",
        prompt_name: str = "anesthesia_bundle_page_classifier",
        max_retries: int = 2
    ) -> Dict[str, Any]:
        """
        Execute the workflow to classify PDF pages and group by surgery schedule and patients.
        
        Args:
            organization_id: The organization/workspace ID
            pdf_path: Path to the PDF file
            tag_name: Name of the tag to use (default: "anesthesia_bundle_page_classifier")
            prompt_name: Name of the prompt to use (default: "anesthesia_bundle_page_classifier")
            max_retries: Maximum number of retries for failed documents (default: 2)
            
        Returns:
            Dictionary containing:
            - surgery_schedule: List of page numbers with surgery schedule
            - patients: Dictionary mapping first_last_dob to list of page numbers
        """
        workflow.logger.info(f"Starting classify and group PDF pages workflow for organization: {organization_id}")
        workflow.logger.info(f"PDF path: {pdf_path}, tag: {tag_name}, prompt: {prompt_name}, max_retries: {max_retries}")
        
        # Step 1: Run classify_pdf_pages as a child workflow
        workflow.logger.info("Running classify_pdf_pages workflow as child workflow...")
        from workflows.classify_pdf_pages import ClassifyPDFPagesWorkflowAlias
        
        classification_results = await workflow.execute_child_workflow(
            ClassifyPDFPagesWorkflowAlias.run,
            args=(organization_id, pdf_path, tag_name, prompt_name, max_retries),
            id=f"classify-pdf-pages-{workflow.info().workflow_id}",
            task_queue="doc-router-task-queue",
        )
        
        workflow.logger.info("Classification workflow completed")
        workflow.logger.info(f"Classified {len(classification_results.get('pages', []))} pages")
        
        # Step 2: Group the classification results
        workflow.logger.info("Grouping classification results by surgery schedule and patients...")
        grouped_results = await workflow.execute_activity(
            "group_classification_results_activity",
            args=(classification_results, prompt_name),
            start_to_close_timeout=timedelta(seconds=300),  # 5 minutes for processing
        )
        
        workflow.logger.info(f"Grouping completed: {len(grouped_results.get('surgery_schedule', []))} surgery schedule pages, {len(grouped_results.get('patients', {}))} patients")
        
        return grouped_results


@workflow.defn(name="classify-and-group-pdf-pages")
class ClassifyAndGroupPDFPagesWorkflowAlias:
    """Alias for ClassifyAndGroupPDFPagesWorkflow with a simpler name."""
    
    @workflow.run
    async def run(
        self,
        organization_id: str,
        pdf_path: str,
        tag_name: str = "anesthesia_bundle_page_classifier",
        prompt_name: str = "anesthesia_bundle_page_classifier",
        max_retries: int = 2
    ) -> Dict[str, Any]:
        """Execute the workflow to classify and group PDF pages."""
        workflow_instance = ClassifyAndGroupPDFPagesWorkflow()
        return await workflow_instance.run(organization_id, pdf_path, tag_name, prompt_name, max_retries)

