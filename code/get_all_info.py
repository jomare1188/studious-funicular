# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
#     "requests",
#     "rich",
# ]
# ///

# This is a modified version of get_metadata.py (https://github.com/labbces/SpliceScape/blob/metadata/metadata/get_metadata.py) script.
# We're trying to get all information for a specific SRA accession.

import pandas as pd
import requests
import time
import logging
from typing import Optional
from xml.etree import ElementTree as ET
from random import randint
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)
from rich.logging import RichHandler
import argparse

# Set up logging
FORMAT = "%(message)s"

logging.basicConfig(
    format=FORMAT,
    level="INFO",
    handlers=[RichHandler(show_time=False, show_path=False, markup=True)],
)

log_text = logging.getLogger("rich")
log_text.setLevel(20)


class SRAInfoExtractor:
    def __init__(self, email: str, tool_name: str = "sra_pmid_mapper"):
        """
        Initialize the extractor and populate the necessary attributes.

        Args:
            email (str): User's email for NCBI API.
            tool_name (str): Name of the tool using the API.
        """
        self.email = email
        self.tool_name = tool_name

        self.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

        # Rate limits
        self.ncbi_delay = 0.3

    def _make_ncbi_request(self, endpoint: str, params: dict) -> requests.Response:
        """
        Make a request to NCBI E-utilities with proper parameters.

        Args:
            endpoint (str): The API endpoint to call.
            params (dict): The query parameters for the request.

        Returns:
            requests.Response: The response from the NCBI API.
        """
        base_params = {"email": self.email, "tool": self.tool_name}

        params.update(base_params)

        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.get(url, params=params, timeout=30)
            # Rate limiting
            time.sleep(self.ncbi_delay * randint(1, 3))
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 500:
                log_text.warning(f"NCBI server error (500) - retrying after delay...")
                time.sleep(5)  # Wait longer on server error
                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    return response
                except:
                    log_text.error(f"NCBI request failed twice for {url}")
                    raise
            else:
                raise

    def get_sra_uid(self, run_accession: str) -> Optional[str]:
        """Convert SRA run accession to UID.
        Args:
            run_accession (str): The SRA run accession to convert.

        Returns:
            Optional[str]: The UID corresponding to the SRA run accession, or None if not found.
        """
        try:
            params = {"db": "sra", "term": run_accession, "retmode": "xml"}

            response = self._make_ncbi_request("esearch.fcgi", params)
            root = ET.fromstring(response.content)

            uid_list = root.find("IdList")
            if uid_list is not None and len(uid_list) > 0:
                return uid_list.find("Id").text
            return None

        except Exception as e:
            log_text.warning(f"Error getting UID for {run_accession}: {e}")
            return None

    def get_linked_pmids(self, sra_uid: str) -> list[str]:
        """Get PMIDs linked to an SRA UID via NCBI.

        Args:
            sra_uid (str): The SRA UID to retrieve linked PMIDs for.

        Returns:
            List[str]: A list of PMIDs linked to the SRA UID.
        """
        try:
            # Use linkname parameter to be more specific about the link type
            params = {
                "dbfrom": "sra",
                "db": "pubmed",
                "id": sra_uid,
                "retmode": "xml",
                "cmd": "neighbor",
            }

            response = self._make_ncbi_request("elink.fcgi", params)
            root = ET.fromstring(response.content)

            pmids = []

            # Parse the elink results with better error handling
            for linkset in root.findall(".//LinkSet"):
                # Check if there are any errors in the response
                error_list = linkset.find("ERROR")
                if error_list is not None:
                    log_text.warning(
                        f"NCBI elink error for UID {sra_uid}: {error_list.text}"
                    )
                    continue

                for linksetdb in linkset.findall(".//LinkSetDb"):
                    dbto = linksetdb.find("DbTo")
                    if dbto is not None and dbto.text == "pubmed":
                        for link in linksetdb.findall(".//Link/Id"):
                            pmids.append(link.text)

            # If no direct links, try going through BioProject
            if not pmids:
                pmids.extend(self._get_pmids_via_bioproject(sra_uid))

            return list(set(pmids))  # Remove duplicates

        except Exception as e:
            log_text.warning(f"Error getting PMIDs for SRA UID {sra_uid}: {e}")
            return []

    def _get_pmids_via_bioproject(self, sra_uid: str) -> list[str]:
        """
        Try to get PMIDs via BioProject linkage with improved error handling.

        Args:
            sra_uid (str): The SRA UID to retrieve PMIDs for.

        Returns:
            List[str]: A list of PMIDs linked to the SRA UID via BioProject.
        """
        try:
            # Link SRA to BioProject
            params = {
                "dbfrom": "sra",
                "db": "bioproject",
                "id": sra_uid,
                "retmode": "xml",
                "cmd": "neighbor",
            }

            response = self._make_ncbi_request("elink.fcgi", params)
            root = ET.fromstring(response.content)

            bioproject_ids = []
            for linkset in root.findall(".//LinkSet"):
                for linksetdb in linkset.findall(".//LinkSetDb"):
                    dbto = linksetdb.find("DbTo")
                    if dbto is not None and dbto.text == "bioproject":
                        for link in linksetdb.findall(".//Link/Id"):
                            bioproject_ids.append(link.text)

            # Now link BioProject to PubMed
            all_pmids = []
            for bp_id in bioproject_ids:
                try:
                    params = {
                        "dbfrom": "bioproject",
                        "db": "pubmed",
                        "id": bp_id,
                        "retmode": "xml",
                        "cmd": "neighbor",
                    }

                    response = self._make_ncbi_request("elink.fcgi", params)
                    root = ET.fromstring(response.content)

                    for linkset in root.findall(".//LinkSet"):
                        for linksetdb in linkset.findall(".//LinkSetDb"):
                            dbto = linksetdb.find("DbTo")
                            if dbto is not None and dbto.text == "pubmed":
                                for link in linksetdb.findall(".//Link/Id"):
                                    all_pmids.append(link.text)
                except Exception as e:
                    log_text.warning(f"Error linking BioProject {bp_id} to PubMed: {e}")
                    continue

            return all_pmids

        except Exception as e:
            log_text.warning(
                f"Error getting PMIDs via BioProject for SRA UID {sra_uid}: {e}"
            )
            return []

    def get_pmid_for_run(
        self, run_accession: str, bioproject_id: str | None = None
    ) -> dict:
        """
        Get PMID(s) for a single SRA run accession with Scholar fallback.

        Returns:
            Dictionary with pmids, source, and additional info
        """
        result = {
            "pmids": [],
            "source": "none",
            "scholar_results": [],
            "bioproject_used": bioproject_id,
        }

        # First try NCBI direct linking
        sra_uid = self.get_sra_uid(run_accession)
        if sra_uid:
            pmids = self.get_linked_pmids(sra_uid)
            if pmids:
                result["pmids"] = pmids
                result["source"] = "ncbi_direct"
                return result

        return result

    def process_dataframe(
        self,
        df: pd.DataFrame,
        run_column: str = "Run",
        bioproject_column: str = "BioProject",
    ) -> pd.DataFrame:
        """
        Process a DataFrame with SRA run accessions and add PMID information.

        Args:
            df (pd.DataFrame): Input DataFrame containing SRA run accessions.
            run_column (str): Name of the column containing run accessions.
            bioproject_column (str): Name of the column containing BioProject IDs.

        Returns:
            pd.DataFrame: The processed DataFrame with added PMID information.
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

        df = df.copy()

        # Add new columns
        df["PMIDs"] = None
        df["PMID_count"] = 0
        df["Primary_PMID"] = None
        df["PMID_source"] = "none"

        total_runs = len(df)

        with progress:
            progress.add_task("[cyan]Processing SRA runs", total=total_runs)
            for i, row in df.iterrows():
                run_accession = row[run_column]
                bioproject_id = (
                    row.get(bioproject_column, None)
                    if bioproject_column in df.columns
                    else None
                )

                if pd.isna(run_accession):
                    continue

                # Get PMIDs using enhanced method
                result = self.get_pmid_for_run(run_accession, bioproject_id)

                pmids = result["pmids"]
                df.at[i, "PMIDs"] = ";".join(pmids) if pmids else None
                df.at[i, "PMID_count"] = len(pmids)
                df.at[i, "Primary_PMID"] = pmids[0] if pmids else None
                df.at[i, "PMID_source"] = result["source"]

                progress.update(0, advance=1)

        # Summary statistics
        total_pmids = df["PMID_count"].sum()
        ncbi_count = (df["PMID_source"] == "ncbi_direct").sum()

        log_text.info("Final summary")
        log_text.info(f"Total PMIDs found: {total_pmids}")
        log_text.info(f"From NCBI direct: {ncbi_count} runs")

        return df


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Searches PMID Database for SRA Runs",
        add_help=True,
    )
    parser.add_argument(
        "--email", type=str, required=True, help="Email address for NCBI Entrez"
    )
    parser.add_argument(
        "--input", type=str, required=True, help="Path to the input CSV file"
    )
    parser.add_argument(
        "--output", type=str, required=True, help="Path to the output CSV file"
    )
    return parser


def main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    # Initialize the enhanced mapper
    mapper = SRAInfoExtractor(
        email=args.email,
        tool_name="enhanced_sra_pmid_mapper",
    )

    df = pd.read_csv(args.input, header=0, low_memory=False)
    result_df = mapper.process_dataframe(
        df, run_column="Run", bioproject_column="BioProject"
    )
    result_df.to_csv(args.output, index=False)


if __name__ == "__main__":
    main(get_parser())
