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
        prompt_name: str = "anesthesia_bundle_page_classifier",
        max_retries: int = 2
    ) -> Dict[str, Any]:
        """
        Execute the workflow to classify PDF pages.
        
        Args:
            organization_id: The organization/workspace ID
            pdf_path: Path to the PDF file
            tag_name: Name of the tag to use (default: "anesthesia_bundle_page_classifier")
            prompt_name: Name of the prompt to use (default: "anesthesia_bundle_page_classifier")
            max_retries: Maximum number of retries for failed documents (default: 2)
            
        Returns:
            Dictionary containing:
            - file_name: The original filename
            - pages: Array of page classification results
        """
        workflow.logger.info(f"Starting PDF pages classification workflow for organization: {organization_id}")
        workflow.logger.info(f"PDF path: {pdf_path}, tag: {tag_name}, prompt: {prompt_name}, max_retries: {max_retries}")
        
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
        
        # Step 6: Poll document status until all documents reach a completed state
        workflow.logger.info("Polling document status until all documents are completed...")
        poll_tasks = []
        for page_number, upload_result in upload_results:
            document_id = upload_result["document_id"]
            poll_task = workflow.execute_activity(
                "poll_document_status_activity",
                args=(
                    organization_id,
                    document_id,
                    600,  # max_wait_seconds: 10 minutes
                    5,    # poll_interval_seconds: 5 seconds
                    prompt_revid,  # For retries if needed
                ),
                start_to_close_timeout=timedelta(seconds=630),  # Slightly more than max_wait
            )
            poll_tasks.append((page_number, document_id, poll_task))
        
        # Wait for all documents to complete (with retry handling for failures)
        completed_documents = []
        failed_documents = []
        
        for page_number, document_id, poll_task in poll_tasks:
            poll_result = await poll_task
            if poll_result["status"] == "completed":
                completed_documents.append((page_number, document_id))
            elif poll_result["status"] == "failed":
                failed_state = poll_result.get("state", "")
                failed_documents.append((page_number, document_id, failed_state))
        
        workflow.logger.info(f"Polling completed: {len(completed_documents)} completed, {len(failed_documents)} failed")
        
        # Step 7: Retry failed documents (with configurable max retries)
        retry_attempt = 0
        while failed_documents and retry_attempt < max_retries:
            retry_attempt += 1
            workflow.logger.info(f"Retry attempt {retry_attempt}/{max_retries} for {len(failed_documents)} failed documents...")
            
            retry_tasks = []
            for page_number, document_id, failed_state in failed_documents:
                # Retry both OCR and LLM failures
                retry_task = workflow.execute_activity(
                    "retry_failed_document_activity",
                    args=(
                        organization_id,
                        document_id,
                        prompt_revid,
                        failed_state,
                    ),
                    start_to_close_timeout=timedelta(seconds=60),
                )
                retry_tasks.append((page_number, document_id, failed_state, retry_task))
            
            # Wait for retries to be triggered, then poll again
            still_failed = []
            for page_number, document_id, failed_state, retry_task in retry_tasks:
                try:
                    retry_result = await retry_task
                    # Poll again after retry (for both LLM retries and OCR wait-and-check)
                    if retry_result.get("status") in ["retry_triggered", "wait_and_check"]:
                        poll_result = await workflow.execute_activity(
                            "poll_document_status_activity",
                            args=(
                                organization_id,
                                document_id,
                                600,  # max_wait_seconds: 10 minutes
                                5,    # poll_interval_seconds: 5 seconds
                                prompt_revid,
                            ),
                            start_to_close_timeout=timedelta(seconds=630),
                        )
                        if poll_result["status"] == "completed":
                            completed_documents.append((page_number, document_id))
                            workflow.logger.info(f"Document {document_id} completed after retry attempt {retry_attempt}")
                        else:
                            # Still failed, add to list for next retry attempt
                            new_failed_state = poll_result.get("state", failed_state)
                            still_failed.append((page_number, document_id, new_failed_state))
                            workflow.logger.warning(f"Document {document_id} still failed after retry attempt {retry_attempt}: {new_failed_state}")
                    else:
                        # Could not retry, add to still_failed list
                        still_failed.append((page_number, document_id, failed_state))
                        workflow.logger.warning(f"Could not retry document {document_id}: {retry_result.get('reason', 'unknown')}")
                except Exception as e:
                    # Exception during retry, add to still_failed list
                    still_failed.append((page_number, document_id, failed_state))
                    workflow.logger.warning(f"Retry attempt {retry_attempt} failed for page {page_number}, document {document_id}: {e}")
            
            # Update failed_documents for next iteration
            failed_documents = still_failed
        
        if failed_documents:
            workflow.logger.warning(f"After {max_retries} retry attempts, {len(failed_documents)} documents still failed")
        
        # Step 8: Retrieve classification results for all completed documents
        workflow.logger.info("Retrieving classification results...")
        page_results = []
        for page_number, document_id in completed_documents:
            try:
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
            except Exception as e:
                workflow.logger.warning(f"Failed to get classification result for page {page_number}, document {document_id}: {e}")
                page_results.append({
                    "page_num": page_number,
                    prompt_name: {}
                })
        
        # Add empty results for any documents that didn't complete
        completed_page_numbers = {page_num for page_num, _ in completed_documents}
        for page_number, document_id, _ in failed_documents:
            if page_number not in completed_page_numbers:
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
        prompt_name: str = "anesthesia_bundle_page_classifier",
        max_retries: int = 2
    ) -> Dict[str, Any]:
        """Execute the workflow to classify PDF pages."""
        workflow_instance = ClassifyPDFPagesWorkflow()
        return await workflow_instance.run(organization_id, pdf_path, tag_name, prompt_name, max_retries)

