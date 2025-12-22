"""Temporal workflow for classifying PDF pages by chunking, uploading, and running prompts."""

from datetime import timedelta
from temporalio import workflow
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


@workflow.defn
class ClassifyPDFPagesWorkflow:
    """Workflow that chunks a PDF, uploads pages, and classifies them."""

    @workflow.run
    async def run(
        self,
        organization_id: str,
        pdf_path: str,
        tag_name: str = "anesthesia_bundle_page_classifier",
        prompt_name: str = "anesthesia_bundle_page_classifier"
    ) -> Dict[str, Any]:
        """
        Execute the workflow to classify PDF pages.
        
        Args:
            organization_id: The organization/workspace ID
            pdf_path: Path to the PDF file
            tag_name: Name of the tag to use (default: "anesthesia_bundle_page_classifier")
            prompt_name: Name of the prompt to use (default: "anesthesia_bundle_page_classifier")
            
        Returns:
            Dictionary containing:
            - file_name: The original filename
            - pages: Array of page classification results
        """
        workflow.logger.info(f"Starting PDF pages classification workflow for organization: {organization_id}")
        workflow.logger.info(f"PDF path: {pdf_path}, tag: {tag_name}, prompt: {prompt_name}")
        
        # Step 1: Get tag ID from tag name
        workflow.logger.info(f"Getting tag ID for tag: {tag_name}")
        tag_id = await workflow.execute_activity(
            "get_tag_id_activity",
            args=(organization_id, tag_name),
            start_to_close_timeout=timedelta(seconds=30),
        )
        
        if not tag_id:
            raise ValueError(f"Tag '{tag_name}' not found")
        
        # Step 2: Get prompt ID from prompt name
        workflow.logger.info(f"Getting prompt ID for prompt: {prompt_name}")
        prompt_revid = await workflow.execute_activity(
            "get_prompt_id_activity",
            args=(organization_id, prompt_name),
            start_to_close_timeout=timedelta(seconds=30),
        )
        
        if not prompt_revid:
            raise ValueError(f"Prompt '{prompt_name}' not found")
        
        # Step 3: Read, chunk PDF, and upload each page to docrouter (all in one activity to avoid passing large data)
        workflow.logger.info("Reading, chunking, and uploading PDF pages...")
        uploaded_pages = await workflow.execute_activity(
            "chunk_and_upload_pdf_activity",
            args=(pdf_path, organization_id, tag_id),
            start_to_close_timeout=timedelta(seconds=600),  # 10 minutes for large PDFs with uploads
        )
        
        # Extract filename from uploaded pages (activity already provides base_filename)
        # Use simple string operations instead of os.path to avoid sandbox restrictions
        if uploaded_pages:
            base_filename = uploaded_pages[0].get("base_filename", "")
            # Extract filename from path using string operations (no os.path)
            filename = pdf_path.split("/")[-1] if "/" in pdf_path else pdf_path.split("\\")[-1] if "\\" in pdf_path else pdf_path
        else:
            # Fallback: extract filename from path using string operations
            filename = pdf_path.split("/")[-1] if "/" in pdf_path else pdf_path.split("\\")[-1] if "\\" in pdf_path else pdf_path
            base_filename = filename.rsplit(".", 1)[0] if "." in filename else filename
        
        total_pages = len(uploaded_pages)
        workflow.logger.info(f"PDF chunked and uploaded into {total_pages} pages")
        
        # Prepare upload results in the format expected by the rest of the workflow
        upload_results = [
            (page["page_number"], {"document_id": page["document_id"]})
            for page in uploaded_pages
        ]
        
        # Step 6: Trigger prompt runs for all documents
        workflow.logger.info("Triggering prompt runs for all documents...")
        run_tasks = []
        for page_number, upload_result in upload_results:
            document_id = upload_result["document_id"]
            run_task = workflow.execute_activity(
                "run_prompt_activity",
                args=(
                    organization_id,
                    document_id,
                    prompt_revid,
                    False,  # force=False
                ),
                start_to_close_timeout=timedelta(seconds=60),
            )
            run_tasks.append((page_number, document_id, run_task))
        
        # Wait for all prompt runs to be triggered
        for page_number, document_id, run_task in run_tasks:
            await run_task
        
        workflow.logger.info("All prompt runs triggered")
        
        # Step 7: Wait for all prompts to complete and retrieve results
        workflow.logger.info("Waiting for all prompts to complete...")
        classification_tasks = []
        for page_number, document_id, _ in run_tasks:
            # First wait for completion
            wait_task = workflow.execute_activity(
                "wait_for_prompt_activity",
                args=(
                    organization_id,
                    document_id,
                    prompt_revid,
                    600,  # max_wait_seconds: 10 minutes
                    5,    # poll_interval_seconds: 5 seconds
                ),
                start_to_close_timeout=timedelta(seconds=630),  # Slightly more than max_wait
            )
            classification_tasks.append((page_number, document_id, wait_task))
        
        # Wait for all prompts to complete
        page_results = []
        for page_number, document_id, wait_task in classification_tasks:
            wait_result = await wait_task
            if wait_result["status"] == "completed":
                # Get the classification result
                result = await workflow.execute_activity(
                    "get_classification_result_activity",
                    args=(
                        organization_id,
                        document_id,
                        prompt_revid,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                )
                # Extract the classification result (updated_llm_result contains the JSON)
                classification_data = result.get("updated_llm_result", {}) if result else {}
                page_results.append({
                    "page_num": page_number,
                    prompt_name: classification_data
                })
            else:
                workflow.logger.warning(f"Prompt timeout for page {page_number}, document {document_id}")
                page_results.append({
                    "page_num": page_number,
                    prompt_name: {}
                })
        
        # Sort by page number
        page_results.sort(key=lambda x: x["page_num"])
        
        # Step 8: Build final result in the requested format
        result = {
            "file_name": filename,
            "pages": page_results
        }
        
        workflow.logger.info(f"PDF pages classification workflow completed for {total_pages} pages")
        return result


@workflow.defn(name="classify-pdf-pages")
class ClassifyPDFPagesWorkflowAlias:
    """Alias for ClassifyPDFPagesWorkflow with a simpler name."""
    
    @workflow.run
    async def run(
        self,
        organization_id: str,
        pdf_path: str,
        tag_name: str = "anesthesia_bundle_page_classifier",
        prompt_name: str = "anesthesia_bundle_page_classifier"
    ) -> Dict[str, Any]:
        """Execute the workflow to classify PDF pages."""
        workflow_instance = ClassifyPDFPagesWorkflow()
        return await workflow_instance.run(organization_id, pdf_path, tag_name, prompt_name)

