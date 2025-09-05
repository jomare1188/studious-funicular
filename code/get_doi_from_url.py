# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "numpy",
#     "pandas",
#     "regex",
#     "rich",
#     "selenium",
# ]
# ///
import argparse
import ast
import json
import logging
import os
import time
from fnmatch import fnmatch
from random import randint

import numpy as np
import pandas as pd
import regex as re
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.firefox.options import Options

# Set up logging
FORMAT = "%(message)s"

logging.basicConfig(
    format=FORMAT,
    level="INFO",
    handlers=[RichHandler(show_time=False, show_path=False, markup=False)],
)

log_text = logging.getLogger("rich")
log_text.setLevel(20)


def parse_links(x: str) -> list[str]:
    """
    Parse a string representation of a list into an actual list.
    If parsing fails, return an empty list.

    Args:
        x (str): String representation of a list.

    Returns:
        list: Parsed list or empty list if parsing fails.
    """
    if isinstance(x, str):
        # Correct issue from double single quotes
        x = x.replace("''", "'")
        try:
            return ast.literal_eval(x)
        except:
            return []
    return []


def get_doi_from_url(url: str, timeout: int = 10) -> str | None:
    """
    Improved DOI extraction function with better error handling

    Args:
        url: URL to extract DOI from
        timeout: Timeout for HTTP requests in seconds

    Returns:
        Extracted DOI or None if not found
    """
    try:
        # First, try to extract DOI directly from URL patterns (before making HTTP request)
        url_patterns = [
            # bioRxiv: https://www.biorxiv.org/content/10.1101/2021.09.19.460957.abstract
            r"biorxiv\.org/content/(10\.1101/\d{4}\.\d{2}\.\d{2}\.\d{6})",
            # OUP/Oxford: https://academic.oup.com/gigascience/article-abstract/doi/10.1093/gigascience/giac035/6575386
            r"academic\.oup\.com/[^/]+/article[^/]*/doi/(10\.\d{4,}/[^/?&#]+)",
            # Wiley: https://onlinelibrary.wiley.com/doi/abs/10.1111/tpj.16519
            r"onlinelibrary\.wiley\.com/doi/(?:abs|full|pdf)?/?(?:10\.1001/)?(?:10\.1111/|10\.1002/)?(10\.\d{4,}/[^/?&#]+)",
            # Nature: https://www.nature.com/articles/10.1038/s41586-021-03819-2
            r"nature\.com/articles/(10\.\d{4,}/[^/?&#]+)",
            # Springer: https://link.springer.com/article/10.1186/s12864-023-09185-9
            r"link\.springer\.com/(?:article|chapter|book)/(10\.\d{4,}/[^/?&#]+)",
            # Frontiers: https://www.frontiersin.org/articles/10.3389/fmicb.2021.685937/full
            r"frontiersin\.org/articles/(10\.\d{4,}/[^/?&#]+)",
            # BMC: https://bmcgenomics.biomedcentral.com/articles/10.1186/s12864-023-09185-9
            r"biomedcentral\.com/articles/(10\.\d{4,}/[^/?&#]+)",
            # Generic DOI in URL
            r"(?:dx\.)?doi\.org/(10\.\d{4,}/[^/?&#]+)",
            # Generic pattern for any URL containing DOI (moved to end as fallback)
            r"/(?:doi/)?(?:abs/|full/|pdf/)?(10\.\d{4,}/[^/?&#\s\.]+)(?:\.[^/?&#]*)?",
        ]

        for pattern in url_patterns:
            url_match = re.search(pattern, url, re.I)
            if url_match:
                potential_doi = url_match.group(1)
                # Validate DOI format
                if re.match(r"^10\.\d{4,}/.+", potential_doi):
                    return potential_doi

        # If no DOI found in URL, proceed with HTTP request
        # headers = {
        #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        # }
        # response = requests.get(url, headers=headers, timeout=timeout)
        # response.raise_for_status()

        options = Options()
        # options = webdriver.ChromeOptions()
        # options.add_argument("--headless")  # optional: run in headless mode
        driver = webdriver.Firefox(options=options)
        driver.implicitly_wait(10)
        driver.set_page_load_timeout(timeout * randint(1, 2))

        driver.get(url)
        html = driver.page_source
        driver.quit()

        # Look for DOI in meta tags - comprehensive patterns
        meta_patterns = [
            r'<meta[^>]*(?:name|property)=["\']?(?:DC\.Identifier|citation_doi|dc\.identifier|doi|prism\.doi)["\']?[^>]*content=["\']?(10\.\d{4,}/[^"\'>\s]+)',
            r'<meta[^>]*content=["\']?(10\.\d{4,}/[^"\'>\s]+)["\']?[^>]*(?:name|property)=["\']?(?:DC\.Identifier|citation_doi|dc\.identifier|doi)',
        ]
        for pattern in meta_patterns:
            doi_meta = re.search(pattern, html, re.I)
            if doi_meta:
                doi = doi_meta.group(1)
                doi = re.sub(r'["\'/>\s]+$', "", doi)
                return doi

        # Look for DOI in JSON-LD structured data
        json_ld_patterns = [
            r'"doi":\s*"(10\.\d{4,}/[^"]+)"',
            r'"@id":\s*"(?:https?://(?:dx\.)?doi\.org/)?(10\.\d{4,}/[^"]+)"',
        ]
        for pattern in json_ld_patterns:
            json_ld_doi = re.search(pattern, html, re.I)
            if json_ld_doi:
                return json_ld_doi.group(1)

        # Look for DOI in text with various formats
        text_patterns = [
            r'doi[:\s]*(?:https?://(?:dx\.)?doi\.org/)?(10\.\d{4,}/[^\s<)"\']+)',
            r'DOI[:\s]*(?:https?://(?:dx\.)?doi\.org/)?(10\.\d{4,}/[^\s<)"\']+)',
            r'https?://(?:dx\.)?doi\.org/(10\.\d{4,}/[^\s<)"\']+)',
            r'Digital Object Identifier[:\s]*(?:https?://(?:dx\.)?doi\.org/)?(10\.\d{4,}/[^\s<)"\']+)',
        ]
        for pattern in text_patterns:
            match = re.search(pattern, html, re.I)
            if match:
                doi = match.group(1)
                doi = re.sub(r"[.,;)\]}\s]+$", "", doi)
                return doi

        return None
    except TimeoutException:
        log_text.warning(f"Timeout for {url}")
        return "TIMEOUT"
    except WebDriverException as e:
        log_text.warning(f"Selenium WebDriver error for {url}: {e}")
        return "WEBDRIVER_ERROR"
    except Exception as e:
        log_text.error(f"Unexpected error for {url}: {e}")
        return "ERROR"


def process_scholar_links(links_list: pd.Series) -> list[dict]:
    """
    Process scholar_links list and extract DOIs for each link

    Args:
        links_list: Series of links (can be a single string or list of strings)
    Returns:
        List of dicts with link, doi, and status
    """
    if pd.isna(links_list).any() or not links_list:
        return []

    # links_list = list(links_list)

    # Handle both list and string inputs
    if isinstance(links_list, str):
        # If it's a string, split by common delimiters
        links = re.split(r"[,;\n|]", links_list)
        links = [link.strip() for link in links if link.strip()]
    elif isinstance(links_list, list):
        # If it's already a list, use directly
        links = [
            link.strip()
            for link in links_list
            if link and isinstance(link, str) and link.strip()
        ]
    else:
        return []

    results = []
    for link in links:
        # log_text.info("================================")
        # log_text.info(f"Processing link: {link}")
        if link.startswith("http"):
            doi = get_doi_from_url(link)
            # log_text.info(f"Extracted DOI: {doi}")
            results.append(
                {
                    "link": link,
                    "doi": doi,
                    "status": "success"
                    if doi and not doi.startswith(("TIMEOUT", "REQUEST_ERROR", "ERROR"))
                    else "failed",
                }
            )
            # Be respectful with requests
            time.sleep(randint(1, 3))

    return results


def process_json_articles(json_file_path: str) -> list[dict]:
    """
    Process JSON file with articles and extract DOIs

    Args:
        json_file_path: Path to the JSON file
    Returns:
        List of dicts with bioproject_id, title, link, citations, doi, and status
    """
    try:
        with open(json_file_path, "r") as f:
            data = json.load(f)

        results = []
        for article in data.get("articles", []):
            link = article.get("link")
            if link:
                doi = get_doi_from_url(link)
                result = {
                    "bioproject_id": article.get("bioproject_id"),
                    "title": article.get("title"),
                    "link": link,
                    "citations": article.get("citations"),
                    "doi": doi,
                    "status": "success"
                    if doi and not doi.startswith(("TIMEOUT", "REQUEST_ERROR", "ERROR"))
                    else "failed",
                }
                results.append(result)
                time.sleep(randint(1, 3))  # Be respectful

        return results
    except Exception as e:
        log_text.error(f"Error processing JSON file {json_file_path}: {e}")
        return []


def add_dois_to_dataframe(
    df: pd.DataFrame, json_files_dict: dict[str, str] = None
) -> pd.DataFrame:
    """
    Add DOI information to dataframe

    Args:
        df: DataFrame with 'scholar_links' column
        json_files_dict: Optional dict mapping bioproject_id to JSON file paths

    Returns:
        DataFrame with added DOI information columns
    """

    progress = Progress(
        SpinnerColumn(),
        TaskProgressColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        MofNCompleteColumn(),
    )

    df_copy = df.copy()

    # Initialize new columns
    df_copy["doi_results"] = None
    df_copy["doi_count"] = 0
    df_copy["failed_links"] = None
    df_copy["success_rate"] = 0.0

    total_tasks = len(df_copy)

    with progress:
        task = progress.add_task("[cyan] Processing rows...", total=total_tasks)
        for idx, row in df_copy.iterrows():
            # log_text.info(f"Processing row {idx + 1}/{len(df_copy)}")

            all_results = []

            # if pd.notna(row['scholar_links']):
            #     break

            # Process scholar_links
            if (
                "scholar_links" in df_copy.columns
                and pd.notna(row["scholar_links"]).any()
            ):
                scholar_results = process_scholar_links(row["scholar_links"])
                all_results.extend(scholar_results)

            # Process JSON file if available
            if json_files_dict and "bioproject_id" in df_copy.columns:
                bioproject_id = row["bioproject_id"]
                if bioproject_id in json_files_dict:
                    json_results = process_json_articles(json_files_dict[bioproject_id])
                    # Convert to same format as scholar_results
                    for result in json_results:
                        all_results.append(
                            {
                                "link": result["link"],
                                "doi": result["doi"],
                                "status": result["status"],
                                "title": result["title"],
                                "citations": result["citations"],
                            }
                        )

            # Store results
            if all_results:
                df_copy.at[idx, "doi_results"] = json.dumps(all_results)

                # Count successful DOIs
                successful_dois = [r for r in all_results if r["status"] == "success"]
                df_copy.at[idx, "doi_count"] = len(successful_dois)

                # Store failed links for later processing
                failed_links = [
                    r["link"] for r in all_results if r["status"] == "failed"
                ]
                if failed_links:
                    df_copy.at[idx, "failed_links"] = json.dumps(failed_links)

                # Calculate success rate
                if all_results:
                    df_copy.at[idx, "success_rate"] = len(successful_dois) / len(
                        all_results
                    )
            progress.update(task, advance=1)
    return df_copy


def get_failed_links_for_reprocessing(df: pd.DataFrame) -> list[str]:
    """
    Extract all failed links for later reprocessing

    Args:
        df: DataFrame with 'failed_links' column
    Returns:
        List of unique failed links
    """
    failed_links = []
    for _, row in df.iterrows():
        if pd.notna(row["failed_links"]):
            try:
                links = json.loads(row["failed_links"])
                failed_links.extend(links)
            except json.JSONDecodeError:
                continue

    return list(set(failed_links))  # Remove duplicates


def create_doi_summary_report(df: pd.DataFrame) -> dict:
    """
    Create a summary report of DOI extraction results

    Args:
        df: DataFrame with DOI extraction results
    Returns:
        Summary report as a dictionary
    """
    total_rows = len(df)
    rows_with_dois = len(df[df["doi_count"] > 0])
    total_dois_found = df["doi_count"].sum()

    # Calculate average success rate
    avg_success_rate = df[df["success_rate"] > 0]["success_rate"].mean()

    return {
        "total_rows_processed": total_rows,
        "rows_with_dois_found": rows_with_dois,
        "total_dois_extracted": int(total_dois_found),
        "average_success_rate": f"{avg_success_rate:.2%}"
        if pd.notna(avg_success_rate)
        else "0%",
        "rows_with_failures": len(df[pd.notna(df["failed_links"])]),
    }


def get_parser() -> argparse.ArgumentParser:
    """
    Parse command-line arguments
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """

    parser = argparse.ArgumentParser(
        description="Searches PMID Database for SRA Runs",
        add_help=True,
    )
    parser.add_argument(
        "--input", type=str, required=True, help="Path to the input CSV file"
    )
    parser.add_argument(
        "--json_dir", type=str, required=True, help="Directory with JSON files"
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    df = pd.read_csv(args.input, header=0, low_memory=False)

    df["scholar_links"] = df["scholar_links"].apply(parse_links)

    bioprojects = set(np.unique(df["BioProject"].dropna()))

    # Optional: Define JSON files for specific bioprojects
    json_path = args.json_dir
    json_list = [name for name in os.listdir(json_path) if fnmatch(name, "*.json")]
    json_set = set([name.split("_articles.json")[0] for name in json_list])

    jsons_intersection = bioprojects.intersection(json_set)

    json_files = {
        k: os.path.join(args.json_dir, f"{k}_articles.json") for k in jsons_intersection
    }

    # Filter dataframe to only those with scholar_processed = True
    filtered_df = df[df["scholar_processed"]]

    # Get unique BioProject IDs from that filtered set
    bioproject_ids = filtered_df["BioProject"].dropna().unique().tolist()
    log_text.info(f"Found {len(bioproject_ids)} unique bioprojects to process")

    # Keep only the first value for each bioproject
    df_filter = filtered_df.drop_duplicates(subset=["BioProject"], keep="first")

    # Process the dataframe
    df_with_dois = add_dois_to_dataframe(df_filter, json_files)

    # Merge back DOI info into the full dataframe based on BioProject
    df_merged = df.merge(
        df_with_dois[
            ["BioProject", "doi_count", "doi_results", "failed_links", "success_rate"]
        ],
        on="BioProject",
        how="left",
    )

    # Get summary report
    report = create_doi_summary_report(df_merged)
    log_text.info("DOI Extraction Report:")
    for key, value in report.items():
        log_text.info(f"  {key}: {value}")

    # Get failed links for reprocessing
    failed_links = get_failed_links_for_reprocessing(df_merged)
    log_text.info(f"\nFound {len(failed_links)} failed links for later reprocessing")

    # Save results
    output_name = args.input.replace(".csv", "_with_dois.csv")
    df_merged.to_csv(output_name, index=False)

    # Save failed links for later
    with open("failed_links.json", "w") as f:
        json.dump(failed_links, f, indent=2)
