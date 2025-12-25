"""Activity to group classification results by surgery schedule and patients."""

from temporalio import activity
from typing import Dict, Any, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


@activity.defn
async def group_classification_results_activity(
    classification_results: Dict[str, Any],
    prompt_name: str
) -> Dict[str, Any]:
    """
    Group classification results by surgery schedule and patients.
    
    Args:
        classification_results: The results from classify_pdf_pages workflow
        prompt_name: The prompt name used (to extract the classification data)
        
    Returns:
        Dictionary containing:
        - surgery_schedule: List of page numbers with surgery schedule
        - patients: Dictionary mapping first_last_dob to dict with "pages" key containing list of page numbers
    """
    logger.info("Grouping classification results by surgery schedule and patients")
    
    surgery_schedule_pages = []
    patients = {}  # Maps patient_key -> {"pages": [page_numbers]}
    pages_without_dob = []  # List of (page_num, first_name, last_name) tuples
    
    # Extract pages from results
    pages = classification_results.get("pages", [])
    
    # Recursively search for patient fields in the classification data
    def find_field(data, field_names, current_path=""):
        """Recursively search for a field in nested dict/list structures."""
        if isinstance(data, dict):
            for key, value in data.items():
                key_lower = str(key).lower()
                # Check if this key matches any of the field names
                for field_name in field_names:
                    if field_name in key_lower:
                        return value
                # Recursively search nested structures
                result = find_field(value, field_names, f"{current_path}.{key}")
                if result is not None:
                    return result
        elif isinstance(data, list):
            for item in data:
                result = find_field(item, field_names, current_path)
                if result is not None:
                    return result
        return None
    
    # Common field names to check (case-insensitive)
    field_mappings = {
        "first_name": ["firstname", "first_name", "first name", "fname", "patient first name"],
        "last_name": ["lastname", "last_name", "last name", "lname", "patient last name", "surname"],
        "dob": ["date of birth", "dob", "birthdate", "birth date", "date_of_birth", "birth_date"]
    }
    
    # First pass: collect all pages and categorize them
    for page_data in pages:
        page_num = page_data.get("page_num")
        classification_data = page_data.get(prompt_name, {})
        
        if not classification_data:
            continue
        
        # Check if this is a surgery schedule page
        classification_str = str(classification_data).lower()
        if any(keyword in classification_str for keyword in ["surgery schedule", "schedule", "surgical schedule", "operating room", "or schedule"]):
            surgery_schedule_pages.append(page_num)
            continue
        
        # Extract patient information
        first_name = find_field(classification_data, field_mappings["first_name"])
        last_name = find_field(classification_data, field_mappings["last_name"])
        dob = find_field(classification_data, field_mappings["dob"])
        
        # If we found patient information, normalize
        if first_name or last_name or dob:
            # Normalize names (lowercase, strip whitespace)
            first_name_normalized = str(first_name).strip().lower() if first_name else ""
            last_name_normalized = str(last_name).strip().lower() if last_name else ""
            
            # Normalize DOB
            dob_normalized = None
            if dob:
                dob_str = str(dob).strip()
                # Try to parse various date formats
                date_formats = [
                    "%Y-%m-%d",
                    "%m/%d/%Y",
                    "%d/%m/%Y",
                    "%Y/%m/%d",
                    "%m-%d-%Y",
                    "%d-%m-%Y",
                    "%B %d, %Y",
                    "%b %d, %Y",
                    "%d %B %Y",
                    "%d %b %Y",
                    "%Y%m%d",
                ]
                
                parsed_date = None
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(dob_str, fmt)
                        break
                    except (ValueError, TypeError):
                        continue
                
                # If parsing failed, try dateutil parser as fallback
                if parsed_date is None:
                    try:
                        from dateutil import parser
                        parsed_date = parser.parse(dob_str)
                    except (ImportError, ValueError, TypeError):
                        logger.warning(f"Could not parse DOB: {dob_str}")
                
                if parsed_date:
                    # Format as YYYY_MM_DD
                    dob_normalized = parsed_date.strftime("%Y_%m_%d")
            
            # Create patient key: first_last_dob (lowercase, underscores)
            patient_key_parts = []
            if first_name_normalized:
                patient_key_parts.append(first_name_normalized)
            if last_name_normalized:
                patient_key_parts.append(last_name_normalized)
            if dob_normalized:
                patient_key_parts.append(dob_normalized)
            
            if patient_key_parts:
                if dob_normalized:
                    # Page has DOB - add directly to patients dict
                    patient_key = "_".join(patient_key_parts)
                    if patient_key not in patients:
                        patients[patient_key] = {"pages": []}
                    patients[patient_key]["pages"].append(page_num)
                    logger.debug(f"Page {page_num} assigned to patient with DOB: {patient_key}")
                else:
                    # Page without DOB - store for later matching
                    pages_without_dob.append((page_num, first_name_normalized, last_name_normalized))
                    logger.debug(f"Page {page_num} stored for DOB matching: {first_name_normalized} {last_name_normalized}")
        else:
            # If no patient info found, check if it might be a surgery schedule page
            classification_str = str(classification_data).lower()
            if any(keyword in classification_str for keyword in ["schedule", "surgery", "operating"]):
                surgery_schedule_pages.append(page_num)
    
    # Second pass: match pages without DOB to pages with DOB based on name
    for page_num, first_name, last_name in pages_without_dob:
        matched = False
        
        # Try to find a matching patient with DOB
        for patient_key, patient_data in patients.items():
            # Extract name parts from patient_key (format: first_last_dob)
            key_parts = patient_key.split("_")
            # Last part is DOB, everything before is name
            if len(key_parts) >= 3:  # Has DOB
                key_first = key_parts[0] if len(key_parts) > 0 else ""
                key_last = key_parts[1] if len(key_parts) > 1 else ""
                
                # Match if names match (case-insensitive, already normalized)
                if first_name and key_first and first_name == key_first:
                    if last_name and key_last and last_name == key_last:
                        # Found a match - add page to this patient group
                        patient_data["pages"].append(page_num)
                        matched = True
                        logger.debug(f"Page {page_num} matched to patient {patient_key} by name")
                        break
                    elif not last_name and not key_last:
                        # Both missing last name, match on first name only
                        patient_data["pages"].append(page_num)
                        matched = True
                        logger.debug(f"Page {page_num} matched to patient {patient_key} by first name only")
                        break
                elif not first_name and not key_first and last_name and key_last and last_name == key_last:
                    # Match on last name only
                    patient_data["pages"].append(page_num)
                    matched = True
                    logger.debug(f"Page {page_num} matched to patient {patient_key} by last name only")
                    break
        
        # If no match found, create a new patient entry without DOB
        if not matched:
            patient_key_parts = []
            if first_name:
                patient_key_parts.append(first_name)
            if last_name:
                patient_key_parts.append(last_name)
            if patient_key_parts:
                patient_key = "_".join(patient_key_parts)
                if patient_key not in patients:
                    patients[patient_key] = {"pages": []}
                patients[patient_key]["pages"].append(page_num)
                logger.debug(f"Page {page_num} assigned to new patient without DOB: {patient_key}")
    
    # Third pass: Fuzzy matching - merge standalone patients with similar names (up to 2 letter difference)
    def levenshtein_distance(s1, s2):
        """Calculate Levenshtein distance between two strings."""
        if len(s1) < len(s2):
            return levenshtein_distance(s2, s1)
        if len(s2) == 0:
            return len(s1)
        
        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        return previous_row[-1]
    
    def extract_name_string(patient_key):
        """Extract name portion from patient key (format: first_last or first_last_dob).
        Returns the name part without DOB as a single string.
        DOB format is YYYY_MM_DD, so:
        - first_last_YYYY_MM_DD (5 parts) -> name is first 2 parts
        - first_YYYY_MM_DD (4 parts) -> name is first 1 part
        - first_last (2 parts) -> name is all parts
        - first (1 part) -> name is all parts"""
        parts = patient_key.split("_")
        # Check if last 3 parts look like a date (YYYY, MM, DD - all numeric)
        if len(parts) >= 4:
            last_three = parts[-3:]
            if all(part.isdigit() for part in last_three) and len(last_three[0]) == 4:  # YYYY format
                # Has DOB - return name parts (everything except last 3)
                return "_".join(parts[:-3])
        # No DOB - return all parts as name
        return "_".join(parts)
    
    logger.info("Third pass: Fuzzy matching standalone patients with similar names...")
    patients_to_remove = []
    
    # Find standalone patients (only 1 page) and compare with others
    for standalone_key, standalone_data in patients.items():
        if len(standalone_data["pages"]) != 1:
            continue  # Only process standalone patients
        
        standalone_name = extract_name_string(standalone_key).lower()
        best_match = None
        best_distance = float('inf')
        
        # Compare with all other patients
        for other_key, other_data in patients.items():
            if other_key == standalone_key:
                continue
            
            other_name = extract_name_string(other_key).lower()
            
            # Calculate Levenshtein distance between the full name strings (case-insensitive)
            distance = levenshtein_distance(standalone_name, other_name)
            
            # Only consider if distance <= 2
            if distance <= 2 and distance < best_distance:
                best_distance = distance
                best_match = other_key
        
        # If we found a match, merge the standalone patient into the matched patient
        if best_match:
            logger.info(f"Merging standalone patient '{standalone_key}' (name: '{standalone_name}', distance: {best_distance}) into '{best_match}' (name: '{extract_name_string(best_match)}')")
            patients[best_match]["pages"].extend(standalone_data["pages"])
            patients_to_remove.append(standalone_key)
    
    # Remove merged patients
    for key_to_remove in patients_to_remove:
        del patients[key_to_remove]
        logger.debug(f"Removed merged patient: {key_to_remove}")
    
    logger.info(f"Fuzzy matching completed: merged {len(patients_to_remove)} standalone patients")
    
    # Sort page numbers in all lists
    surgery_schedule_pages.sort()
    for patient_key in patients:
        patients[patient_key]["pages"].sort()
    
    result = {
        "surgery_schedule": surgery_schedule_pages,
        "patients": patients
    }
    
    logger.info(f"Grouped results: {len(surgery_schedule_pages)} surgery schedule pages, {len(patients)} patients")
    return result

