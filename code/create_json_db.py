# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
#     "rich",
# ]
# ///

import argparse
import json
import logging
import os
from datetime import datetime

import pandas as pd
from rich.logging import RichHandler

# Set up logging
FORMAT = "%(message)s"

logging.basicConfig(
    format=FORMAT,
    level="INFO",
    handlers=[RichHandler(show_time=False, show_path=False, markup=False)],
)

log_text = logging.getLogger("rich")
log_text.setLevel(20)


def process_doi_integration(csv_file_path, scholar_results_folder):
    """
    Process CSV file and update/create JSON files with DOI and PMID information

    Args:
        csv_file_path (str): Path to the CSV file containing BioProject, doi_results, and PMIDs columns
        scholar_results_folder (str): Path to the folder containing existing JSON files
    """

    # Read the CSV file
    df = pd.read_csv(csv_file_path, header=0, low_memory=False)

    # Process each row in the CSV
    for index, row in df.iterrows():
        bioproject_id = row["BioProject"]
        doi_results_str = row.get("doi_results", "")
        pmids_str = row.get("PMIDs", "")

        # Parse the doi_results JSON string (handle blank/empty values)
        doi_results = []
        if (
            doi_results_str
            and pd.notna(doi_results_str)
            and str(doi_results_str).strip()
        ):
            try:
                doi_results = json.loads(doi_results_str)
            except (json.JSONDecodeError, TypeError) as e:
                log_text.info(
                    f"Warning: Could not parse doi_results for {bioproject_id}: {e}"
                )

        # Parse PMIDs (handle blank/empty values)
        pmids = []
        if pmids_str and pd.notna(pmids_str) and str(pmids_str).strip():
            pmids = [pmid.strip() for pmid in str(pmids_str).split(";") if pmid.strip()]

        # Skip if no data to process
        if not doi_results and not pmids:
            log_text.info(
                f"No DOI results or PMIDs found for {bioproject_id}, skipping..."
            )
            continue

        # Define the JSON file path
        json_file_path = os.path.join(
            scholar_results_folder, f"{bioproject_id}_articles.json"
        )

        # Check if JSON file exists
        if os.path.exists(json_file_path):
            # Update existing file
            update_existing_json(json_file_path, doi_results, pmids, bioproject_id)
        else:
            # Create new file
            create_new_json(json_file_path, doi_results, pmids, bioproject_id)


def update_existing_json(json_file_path, doi_results, pmids, bioproject_id):
    """
    Update existing JSON file with DOI and PMID information
    """
    try:
        # Read existing JSON file
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Create a mapping of link to DOI for quick lookup
        link_to_doi = {}
        if doi_results:
            link_to_doi = {
                item["link"]: item["doi"]
                for item in doi_results
                if item.get("status") == "success" and "link" in item and "doi" in item
            }

        # Update articles with DOI information
        for article in data.get("articles", []):
            if article["link"] in link_to_doi:
                article["doi"] = link_to_doi[article["link"]]

        # Add PMIDs to the main JSON structure if they exist
        if pmids:
            data["PubMedIDs"] = pmids

        # Write updated data back to file
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        log_text.info(f"Updated existing file: {json_file_path}")

    except Exception as e:
        log_text.error(f"Error updating {json_file_path}: {str(e)}")


def create_new_json(json_file_path, doi_results, pmids, bioproject_id):
    """
    Create new JSON file with DOI and PMID information
    """
    try:
        # Create articles list from doi_results
        articles = []
        if doi_results:
            for doi_item in doi_results:
                if (
                    doi_item.get("status") == "success"
                    and "link" in doi_item
                    and "doi" in doi_item
                ):
                    article = {
                        "title": None,
                        "link": doi_item["link"],
                        "citations": None,
                        "bioproject_id": bioproject_id,
                        "doi": doi_item["doi"],
                    }
                    articles.append(article)

        # Create the JSON structure
        json_data = {
            "bioproject_id": bioproject_id,
            "articles": articles,
            "total_articles": len(articles),
            "scrape_timestamp": datetime.now().isoformat(),
        }

        # Add PMIDs if they exist
        if pmids:
            json_data["PubMedIDs"] = pmids

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(json_file_path), exist_ok=True)

        # Write new JSON file
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)

        log_text.info(f"Created new file: {json_file_path}")

    except Exception as e:
        log_text.error(f"Error creating {json_file_path}: {str(e)}")


def validate_data_structure(csv_file_path):
    """
    Validate the CSV file structure and show sample data
    """
    try:
        df = pd.read_csv(csv_file_path, header=0, low_memory=False)
        log_text.info("CSV file structure:")
        log_text.info(f"Columns: {list(df.columns)}")
        log_text.info(f"Total rows: {len(df)}")
        log_text.info("\nSample data (first 3 rows):")

        for i, row in df.head(3).iterrows():
            log_text.info(f"\nRow {i + 1}:")
            log_text.info(f"  BioProject: {row.get('BioProject', 'N/A')}")
            log_text.info(
                f"  doi_results: {str(row.get('doi_results', 'N/A'))[:100]}..."
            )
            log_text.info(f"  PMIDs: {row.get('PMIDs', 'N/A')}")

        # Check for blank/empty values
        blank_doi = df["doi_results"].isna().sum() + (df["doi_results"] == "").sum()
        blank_pmids = df["PMIDs"].isna().sum() + (df["PMIDs"] == "").sum()

        log_text.info(f"\nData quality check:")
        log_text.info(f"  Blank/empty doi_results: {blank_doi}")
        log_text.info(f"  Blank/empty PMIDs: {blank_pmids}")

    except Exception as e:
        log_text.error(f"Error validating CSV file: {str(e)}")


def get_parser() -> argparse.ArgumentParser:
    """
    Parse command-line arguments
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """

    parser = argparse.ArgumentParser(
        description="Searches DOI from scholar_links and JSON files",
        add_help=True,
    )
    parser.add_argument(
        "--input", type=str, required=True, help="Path to the input CSV file"
    )
    parser.add_argument(
        "--json_dir", type=str, required=True, help="Directory with JSON files"
    )
    return parser


# Example usage
if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    csv_file_path = args.input  # Path to your CSV file
    scholar_results_folder = args.json_dir  # Path to your JSON files directory

    # # Validate data first (optional but recommended)
    # log_text.info("Validating CSV file structure...")
    # validate_data_structure(csv_file_path)
    # log_text.info("\n" + "="*50 + "\n")

    # Process the integration
    process_doi_integration(csv_file_path, scholar_results_folder)
    log_text.info("DOI and PMID integration completed!")
