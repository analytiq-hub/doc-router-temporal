#! /usr/bin/env python3

"""Unit tester for group_classification_results activity.

Reads a JSON file with classification results, re-runs the grouping algorithm,
and writes the results to an output file.
"""

import asyncio
import argparse
import json
import sys
import logging
from pathlib import Path

# Add parent directory to path so we can import activities
sys.path.insert(0, str(Path(__file__).parent.parent))

from activities.group_classification_results import group_classification_results_activity

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Test the group_classification_results algorithm by re-running it on a JSON file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tests/test_group_classification.py results.json
  python tests/test_group_classification.py results.json --prompt-name anesthesia_bundle_page_classifier
  python tests/test_group_classification.py results.json --output output.json
  python tests/test_group_classification.py results.json --prompt-name anesthesia_bundle_page_classifier --output output.json
        """
    )
    
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the input JSON file with classification results"
    )
    
    parser.add_argument(
        "--prompt-name",
        default="anesthesia_bundle_page_classifier",
        help="Name of the prompt used for classification (default: anesthesia_bundle_page_classifier)"
    )
    
    parser.add_argument(
        "--output",
        type=Path,
        help="Path to the output JSON file (default: {input_file}_regrouped.json)"
    )
    
    return parser.parse_args()


async def main():
    """Main function to test the grouping algorithm."""
    args = parse_arguments()
    
    input_file = args.input_file
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        sys.exit(1)
    
    prompt_name = args.prompt_name
    
    # Get output file (default: input_file_stem + "_regrouped.json")
    if args.output:
        output_file = args.output
    else:
        output_file = input_file.parent / f"{input_file.stem}_regrouped.json"
    
    logger.info(f"Reading input file: {input_file}")
    logger.info(f"Using prompt name: {prompt_name}")
    logger.info(f"Output file: {output_file}")
    
    # Read the input JSON file
    try:
        with open(input_file, 'r') as f:
            input_data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        sys.exit(1)
    
    # Extract pages and file_name from input
    pages = input_data.get("pages", [])
    file_name = input_data.get("file_name", "")
    
    if not pages:
        logger.error("No pages found in input file")
        sys.exit(1)
    
    logger.info(f"Found {len(pages)} pages in input file")
    
    # Prepare classification_results in the format expected by the activity
    classification_results = {
        "file_name": file_name,
        "pages": pages
    }
    
    # Run the grouping algorithm
    logger.info("Running group_classification_results_activity...")
    try:
        result = await group_classification_results_activity(
            classification_results,
            prompt_name
        )
        logger.info("Grouping completed successfully")
    except Exception as e:
        logger.error(f"Error running grouping algorithm: {e}", exc_info=True)
        sys.exit(1)
    
    # Prepare output with original file_name and pages for comparison
    output_data = {
        "file_name": file_name,
        "pages": pages,  # Include original pages for reference
        "schedule": result.get("surgery_schedule", []),
        "patients": result.get("patients", {})
    }
    
    # Write results to output file
    logger.info(f"Writing results to: {output_file}")
    try:
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        logger.info(f"Results written successfully to {output_file}")
    except Exception as e:
        logger.error(f"Error writing output file: {e}")
        sys.exit(1)
    
    # Print summary
    logger.info("=" * 80)
    logger.info("GROUPING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Surgery schedule pages: {len(output_data['schedule'])}")
    logger.info(f"Patients found: {len(output_data['patients'])}")
    
    # Print patient details
    for patient_key, patient_data in output_data['patients'].items():
        page_count = len(patient_data.get('pages', []))
        logger.info(f"  {patient_key}: {page_count} pages")
    
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

