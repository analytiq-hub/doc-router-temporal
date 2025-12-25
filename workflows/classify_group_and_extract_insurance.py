"""Temporal workflow for classifying PDF pages, grouping by patients, and extracting insurance card info."""

from datetime import timedelta
from temporalio import workflow
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


@workflow.defn
class ClassifyGroupAndExtractInsuranceWorkflow:
    """Workflow that classifies PDF pages, groups by patients, and extracts insurance card info for each patient."""

    @workflow.run
    async def run(
        self,
        organization_id: str,
        pdf_path: str,
        tag_name: str = "anesthesia_bundle_page_classifier",
        prompt_name: str = "anesthesia_bundle_page_classifier",
        max_retries: int = 2,
        insurance_tag_name: str = "insurance_card",
        insurance_prompt_name: str = "insurance_card"
    ) -> Dict[str, Any]:
        """
        Execute the workflow to classify PDF pages, group by patients, and extract insurance card info.
        
        Args:
            organization_id: The organization/workspace ID
            pdf_path: Path to the PDF file
            tag_name: Name of the tag to use for classification (default: "anesthesia_bundle_page_classifier")
            prompt_name: Name of the prompt to use for classification (default: "anesthesia_bundle_page_classifier")
            max_retries: Maximum number of retries for failed documents (default: 2)
            insurance_tag_name: Name of the tag to use for insurance card extraction (default: "insurance_card")
            insurance_prompt_name: Name of the prompt to use for insurance card extraction (default: "insurance_card")
            
        Returns:
            Dictionary containing:
            - file_name: Original PDF filename
            - pages: Classification results for all pages
            - schedule: List of surgery schedule page numbers
            - patients: Dictionary mapping patient keys to dict with "pages" and "patient_insurance_card"
        """
        workflow.logger.info(f"Starting classify, group, and extract insurance workflow for organization: {organization_id}")
        workflow.logger.info(f"PDF path: {pdf_path}, tag: {tag_name}, prompt: {prompt_name}, max_retries: {max_retries}")
        
        # Step 1: Run classify_and_group_pdf_pages as a child workflow
        workflow.logger.info("Running classify_and_group_pdf_pages workflow as child workflow...")
        from workflows.classify_and_group_pdf_pages import ClassifyAndGroupPDFPagesWorkflowAlias
        
        grouped_results = await workflow.execute_child_workflow(
            ClassifyAndGroupPDFPagesWorkflowAlias.run,
            args=(organization_id, pdf_path, tag_name, prompt_name, max_retries),
            id=f"classify-and-group-{workflow.info().workflow_id}",
            task_queue="doc-router-task-queue",
        )
        
        workflow.logger.info("Classification and grouping workflow completed")
        workflow.logger.info(f"Found {len(grouped_results.get('patients', {}))} patients")
        
        # Step 2: Get insurance_card tag ID and prompt revision ID
        workflow.logger.info(f"Getting tag ID for '{insurance_tag_name}'...")
        insurance_tag_id = await workflow.execute_activity(
            "get_tag_id_activity",
            args=(organization_id, insurance_tag_name),
            start_to_close_timeout=timedelta(seconds=30),
        )
        
        if not insurance_tag_id:
            workflow.logger.warning(f"Tag '{insurance_tag_name}' not found, skipping insurance card extraction")
            # Return results without insurance card data
            return grouped_results
        
        workflow.logger.info(f"Getting prompt revision ID for '{insurance_prompt_name}'...")
        insurance_prompt_revid = await workflow.execute_activity(
            "get_prompt_id_activity",
            args=(organization_id, insurance_prompt_name),
            start_to_close_timeout=timedelta(seconds=30),
        )
        
        if not insurance_prompt_revid:
            workflow.logger.warning(f"Prompt '{insurance_prompt_name}' not found, skipping insurance card extraction")
            # Return results without insurance card data
            return grouped_results
        
        # Step 3: Process each patient - create PDF, upload, wait, and retrieve insurance card
        patients = grouped_results.get("patients", {})
        patients_with_insurance = {}
        
        for patient_key, patient_data in patients.items():
            workflow.logger.info(f"Processing patient: {patient_key}")
            page_numbers = patient_data.get("pages", [])
            
            if not page_numbers:
                workflow.logger.warning(f"Patient {patient_key} has no pages, skipping")
                patients_with_insurance[patient_key] = patient_data
                continue
            
            # Create PDF from patient pages and upload directly (avoids passing large binary data through Temporal)
            patient_filename = f"{patient_key}.pdf"
            workflow.logger.info(f"Creating and uploading PDF for patient {patient_key} from pages {page_numbers}")
            upload_result = await workflow.execute_activity(
                "create_and_upload_patient_pdf_activity",
                args=(pdf_path, page_numbers, organization_id, patient_filename, [insurance_tag_id]),
                start_to_close_timeout=timedelta(seconds=420),  # 7 minutes for PDF creation and upload
            )
            
            document_id = upload_result.get("document_id")
            workflow.logger.info(f"Patient PDF created and uploaded with document ID: {document_id}")
            
            # Wait for document to reach llm_completed state
            workflow.logger.info(f"Waiting for document {document_id} to reach llm_completed state...")
            max_wait_seconds = 600  # 10 minutes total
            poll_interval_seconds = 5  # Poll every 5 seconds
            max_polls = max_wait_seconds // poll_interval_seconds  # 120 polls
            reached_llm_completed = False
            
            for poll_num in range(max_polls):
                status_result = await workflow.execute_activity(
                    "get_document_status_activity",
                    args=(organization_id, document_id),
                    start_to_close_timeout=timedelta(seconds=30),
                )
                
                if not status_result:
                    workflow.logger.warning(f"Document {document_id} not found")
                    break
                
                state = status_result.get("state", "")
                
                if state == "llm_completed":
                    workflow.logger.info(f"Document {document_id} reached llm_completed state")
                    reached_llm_completed = True
                    break
                elif state in ["llm_failed", "ocr_failed"]:
                    workflow.logger.warning(f"Document {document_id} failed with state: {state}")
                    break
                elif state == "ocr_completed":
                    # OCR completed but LLM not done yet, continue polling
                    workflow.logger.debug(f"Document {document_id} at ocr_completed (poll {poll_num + 1}/{max_polls}), waiting for llm_completed...")
                elif state in ["llm_processing", "ocr_processing", "uploaded"]:
                    # Still processing, continue polling
                    workflow.logger.debug(f"Document {document_id} still processing: {state} (poll {poll_num + 1}/{max_polls})")
                else:
                    workflow.logger.warning(f"Document {document_id} in unexpected state: {state}")
                
                # Sleep before next poll
                await workflow.sleep(timedelta(seconds=poll_interval_seconds))
            
            if not reached_llm_completed:
                workflow.logger.warning(f"Document {document_id} did not reach llm_completed state after {max_polls} polls")
                # Continue without insurance card data for this patient
                patients_with_insurance[patient_key] = patient_data
                continue
            
            # Retrieve the insurance_card result
            workflow.logger.info(f"Retrieving insurance card result for document {document_id}...")
            insurance_result = await workflow.execute_activity(
                "get_classification_result_activity",
                args=(organization_id, document_id, insurance_prompt_revid, None),
                start_to_close_timeout=timedelta(seconds=30),
            )
            
            # Add insurance card data to patient data
            patient_data_with_insurance = patient_data.copy()
            patient_data_with_insurance["patient_insurance_card"] = insurance_result
            
            patients_with_insurance[patient_key] = patient_data_with_insurance
            workflow.logger.info(f"Completed processing patient {patient_key}")
        
        # Return results with insurance card data
        result = {
            "file_name": grouped_results.get("file_name", ""),
            "pages": grouped_results.get("pages", []),
            "schedule": grouped_results.get("schedule", []),
            "patients": patients_with_insurance
        }
        
        workflow.logger.info(f"Workflow completed: processed {len(patients_with_insurance)} patients")
        return result


@workflow.defn(name="classify-group-and-extract-insurance")
class ClassifyGroupAndExtractInsuranceWorkflowAlias:
    """Alias for ClassifyGroupAndExtractInsuranceWorkflow with a simpler name."""
    
    @workflow.run
    async def run(
        self,
        organization_id: str,
        pdf_path: str,
        tag_name: str = "anesthesia_bundle_page_classifier",
        prompt_name: str = "anesthesia_bundle_page_classifier",
        max_retries: int = 2,
        insurance_tag_name: str = "insurance_card",
        insurance_prompt_name: str = "insurance_card"
    ) -> Dict[str, Any]:
        """Execute the workflow to classify, group, and extract insurance card info."""
        workflow_instance = ClassifyGroupAndExtractInsuranceWorkflow()
        return await workflow_instance.run(
            organization_id, pdf_path, tag_name, prompt_name, max_retries,
            insurance_tag_name, insurance_prompt_name
        )

