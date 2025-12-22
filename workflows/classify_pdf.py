"""Temporal workflow for classifying a PDF by splitting it into pages and classifying each."""

from temporalio import workflow
from typing import List, Dict, Any
import logging
from activities.split_pdf import split_pdf_activity
from activities.classify_page import classify_page_activity

logger = logging.getLogger(__name__)


@workflow.defn
class ClassifyPDFWorkflow:
    """Workflow that classifies a PDF by splitting it and classifying each page."""

    @workflow.run
    async def run(
        self,
        organization_id: str,
        pdf_data: bytes,
        classification_prompt: str,
        max_concurrent_classifications: int = 5
    ) -> Dict[str, Any]:
        """
        Execute the workflow to classify a PDF.
        
        Args:
            organization_id: The organization/workspace ID
            pdf_data: The PDF file as bytes
            classification_prompt: The prompt to use for classification
            max_concurrent_classifications: Maximum number of pages to classify concurrently
            
        Returns:
            Dictionary containing:
            - total_pages: Total number of pages
            - classifications: List of classification results for each page
            - summary: Summary statistics
        """
        logger.info(f"Starting PDF classification workflow for organization: {organization_id}")
        logger.info(f"PDF size: {len(pdf_data)} bytes")
        
        # Step 1: Split PDF into pages
        logger.info("Splitting PDF into pages...")
        pages = await workflow.execute_activity(
            split_pdf_activity,
            pdf_data,
            start_to_close_timeout=300,  # 5 minutes for large PDFs
        )
        
        total_pages = len(pages)
        logger.info(f"PDF split into {total_pages} pages")
        
        # Step 2: Classify each page in parallel (with concurrency limit)
        logger.info(f"Classifying {total_pages} pages (max {max_concurrent_classifications} concurrent)...")
        
        # Create tasks for all pages
        classification_tasks = []
        for page_info in pages:
            task = workflow.execute_activity(
                classify_page_activity,
                organization_id,
                page_info["page_data"],
                page_info["page_number"],
                classification_prompt,
                start_to_close_timeout=120,  # 2 minutes per page
            )
            classification_tasks.append(task)
        
        # Execute classifications with concurrency control
        # We'll process them in batches to respect max_concurrent_classifications
        all_results = []
        for i in range(0, len(classification_tasks), max_concurrent_classifications):
            batch = classification_tasks[i:i + max_concurrent_classifications]
            batch_results = await workflow.gather(*batch)
            all_results.extend(batch_results)
            logger.info(f"Completed classification batch {i // max_concurrent_classifications + 1}")
        
        # Sort results by page number to ensure consistent ordering
        all_results.sort(key=lambda x: x["page_number"])
        
        # Step 3: Aggregate results
        logger.info(f"All {total_pages} pages classified successfully")
        
        result = {
            "total_pages": total_pages,
            "classifications": all_results,
            "summary": {
                "pages_classified": len(all_results),
                "organization_id": organization_id
            }
        }
        
        logger.info("PDF classification workflow completed")
        return result


@workflow.defn(name="classify-pdf")
class ClassifyPDFWorkflowAlias:
    """Alias for ClassifyPDFWorkflow with a simpler name."""
    
    @workflow.run
    async def run(
        self,
        organization_id: str,
        pdf_data: bytes,
        classification_prompt: str,
        max_concurrent_classifications: int = 5
    ) -> Dict[str, Any]:
        """Execute the workflow to classify a PDF."""
        workflow_instance = ClassifyPDFWorkflow()
        return await workflow_instance.run(
            organization_id,
            pdf_data,
            classification_prompt,
            max_concurrent_classifications
        )

