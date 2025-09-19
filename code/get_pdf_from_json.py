# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "regex",
#     "requests",
#     "rich",
#     "wiley-tdm",
# ]
# ///
import argparse
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from random import randint
from xml.etree import ElementTree as ET

import regex as re
import requests
import wiley_tdm as wiley_client
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


class ProcessSpringerXML:
    def __init__(self, content: bytes):
        """
        Initialize the ProcessSpringerXML with XML content.

        Args:
            content (bytes): XML content as bytes.
        """
        try:
            self.root = ET.fromstring(content)
        except ET.ParseError as e:
            log_text.error(f"Error parsing XML: {e}")
            self.root = None

    def extract_text_content(self, element: ET.Element) -> str:
        """
        Extract all text content from an element, including nested text.
        """
        if element is None:
            return ""

        text_parts = []
        if element.text:
            text_parts.append(element.text.strip())

        for child in element:
            child_text = self.extract_text_content(child)
            if child_text:
                text_parts.append(child_text)
            if child.tail:
                text_parts.append(child.tail.strip())

        return " ".join(part for part in text_parts if part)

    def extract_publication_info(self, front: ET.Element) -> dict:
        """
        Extract publication metadata.

        Args:
            front (ET.Element): The <front> element of the XML.
        Returns:
            dict: A dictionary containing publication metadata fields.
        """
        pub_info = {}

        # Journal info
        journal_meta = front.find(".//journal-meta")
        if journal_meta is not None:
            journal_title = journal_meta.find(".//journal-title")
            if journal_title is not None:
                pub_info["journal"] = journal_title.text

            issn = journal_meta.find(".//issn")
            if issn is not None:
                pub_info["issn"] = issn.text

            publisher = journal_meta.find(".//publisher-name")
            if publisher is not None:
                pub_info["publisher"] = publisher.text

        # Article metadata
        article_meta = front.find(".//article-meta")
        if article_meta is not None:
            # DOI
            doi = article_meta.find('.//article-id[@pub-id-type="doi"]')
            if doi is not None:
                pub_info["doi"] = doi.text

            # Volume, issue
            volume = article_meta.find(".//volume")
            issue = article_meta.find(".//issue")
            if volume is not None:
                pub_info["volume"] = volume.text
            if issue is not None:
                pub_info["issue"] = issue.text

            # Publication dates
            pub_dates = {}
            for pub_date in article_meta.findall(".//pub-date"):
                date_type = pub_date.get("date-type")
                day = pub_date.find("day")
                month = pub_date.find("month")
                year = pub_date.find("year")

                if year is not None:
                    date_str = year.text
                    if month is not None:
                        date_str = f"{year.text}-{month.text.zfill(2)}"
                        if day is not None:
                            date_str = (
                                f"{year.text}-{month.text.zfill(2)}-{day.text.zfill(2)}"
                            )

                    pub_dates[date_type] = date_str

            if pub_dates:
                pub_info["publication_dates"] = pub_dates

        return pub_info

    def extract_authors(self, contrib_group: ET.Element) -> list:
        """
        Extract author information in a structured format.
        """
        if contrib_group is None:
            return []

        authors = []
        for contrib in contrib_group.findall('.//contrib[@contrib-type="author"]'):
            author = {}

            # Basic info
            name_elem = contrib.find(".//name")
            if name_elem is not None:
                surname = name_elem.find("surname")
                given_names = name_elem.find("given-names")
                author["surname"] = surname.text if surname is not None else ""
                author["given_names"] = (
                    given_names.text if given_names is not None else ""
                )
                author["full_name"] = (
                    f"{author['given_names']} {author['surname']}".strip()
                )

            # Email
            email_elem = contrib.find(".//email")
            if email_elem is not None:
                author["email"] = email_elem.text

            # Corresponding author
            author["is_corresponding"] = contrib.get("corresp") == "yes"

            # Affiliations (reference IDs)
            aff_refs = [
                xref.get("rid") for xref in contrib.findall('.//xref[@ref-type="aff"]')
            ]
            author["affiliation_refs"] = aff_refs

            authors.append(author)

        return authors

    def extract_affiliations(self, front: ET.Element) -> dict:
        """
        Extract affiliation information.
        Args:
            front (ET.Element): The <front> element of the XML.
        Returns:
            dict: A dictionary mapping affiliation IDs to their details.
        """
        affiliations = {}

        for aff in front.findall(".//aff"):
            aff_id = aff.get("id")
            if aff_id:
                affiliation = {}

                # Institution name
                institution = aff.find('.//institution[@content-type="org-name"]')
                if institution is not None:
                    affiliation["institution"] = institution.text

                # Department/Division
                division = aff.find('.//institution[@content-type="org-division"]')
                if division is not None:
                    affiliation["department"] = division.text

                # Address
                city = aff.find('.//addr-line[@content-type="city"]')
                state = aff.find('.//addr-line[@content-type="state"]')
                country = aff.find(".//country")

                address = {}
                if city is not None:
                    address["city"] = city.text
                if state is not None:
                    address["state"] = state.text
                if country is not None:
                    address["country"] = country.text

                if address:
                    affiliation["address"] = address

                affiliations[aff_id] = affiliation

        return affiliations

    def extract_abstract(self, front: ET.Element) -> dict:
        """
        Extract structured abstract content.
        Args:
            front (ET.Element): The <front> element of the XML.
        Returns:
            dict: A dictionary containing abstract sections and full text.
        """
        abstract_elem = front.find(".//abstract")
        if abstract_elem is None:
            return {}

        abstract = {}

        # Abstract sections
        sections = {}
        for sec in abstract_elem.findall(".//sec"):
            title_elem = sec.find("title")
            if title_elem is not None:
                section_title = title_elem.text.lower()
                section_content = []

                for p in sec.findall(".//p"):
                    paragraph_text = self.extract_text_content(p)
                    if paragraph_text:
                        section_content.append(paragraph_text)

                sections[section_title] = " ".join(section_content)

        if sections:
            abstract["sections"] = sections

        # Full abstract text
        abstract["full_text"] = self.extract_text_content(abstract_elem)

        return abstract

    def extract_keywords(self, front: ET.Element) -> list:
        """
        Extract keywords from the article.
        Args:
            front (ET.Element): The <front> element of the XML.
        Returns:
            list: A list of keywords with their language if available.
        """
        keywords = []

        for kwd_group in front.findall(".//kwd-group"):
            group_keywords = []
            for kwd in kwd_group.findall(".//kwd"):
                keyword_text = self.extract_text_content(kwd)
                if keyword_text:
                    group_keywords.append(keyword_text)

            if group_keywords:
                lang = kwd_group.get("{http://www.w3.org/XML/1998/namespace}lang", "en")
                keywords.append({"language": lang, "keywords": group_keywords})

        return keywords

    def extract_funding(self, front: ET.Element) -> list:
        """
        Extract funding information.
        Args:
            front (ET.Element): The <front> element of the XML.
        Returns:
            list: A list of funding sources and award IDs.
        """
        funding_group = front.find(".//funding-group")
        if funding_group is None:
            return []

        funding_info = []
        for award_group in funding_group.findall(".//award-group"):
            funding = {}

            # Funding source
            funding_source = award_group.find(".//funding-source//institution")
            if funding_source is not None:
                funding["source"] = funding_source.text

            # Award ID
            award_id = award_group.find(".//award-id")
            if award_id is not None:
                funding["award_id"] = award_id.text

            funding_info.append(funding)

        return funding_info

    def extract_body_content(self, body: ET.Element) -> dict:
        """
        Extract the main body content in a structured way.
        Args:
            body (ET.Element): The <body> element of the XML.
        Returns:
            dict: A dictionary containing sections, subsections, and full text.
        """
        if body is None:
            return {}

        content = {}
        sections = []

        for sec in body.findall(".//sec"):
            section = {}

            # Section ID and title
            section["id"] = sec.get("id", "")
            title_elem = sec.find("title")
            if title_elem is not None:
                section["title"] = title_elem.text

            # Section content (paragraphs)
            paragraphs = []
            for p in sec.findall(".//p"):
                para_text = self.extract_text_content(p)
                if para_text:
                    paragraphs.append({"id": p.get("id", ""), "text": para_text})

            if paragraphs:
                section["paragraphs"] = paragraphs

            # Subsections
            subsections = []
            for subsec in sec.findall("./sec"):  # Direct child sections only
                subsection = {}
                subsection["id"] = subsec.get("id", "")
                sub_title = subsec.find("title")
                if sub_title is not None:
                    subsection["title"] = sub_title.text

                sub_paragraphs = []
                for p in subsec.findall(".//p"):
                    para_text = self.extract_text_content(p)
                    if para_text:
                        sub_paragraphs.append(
                            {"id": p.get("id", ""), "text": para_text}
                        )

                if sub_paragraphs:
                    subsection["paragraphs"] = sub_paragraphs

                subsections.append(subsection)

            if subsections:
                section["subsections"] = subsections

            sections.append(section)

        content["sections"] = sections
        content["full_text"] = self.extract_text_content(body)

        return content

    def extract_metadata(self) -> dict:
        """
        Extract metadata from the XML content.

        Returns:
            dict: A dictionary containing metadata fields in a structured form.
        """
        article_element = self.root.find(".//article")
        if article_element is None:
            return log_text.error("No <article> element found in XML.")

        # Extract main sections
        front = article_element.find("front")
        body = article_element.find("body")
        back = article_element.find("back")

        # Build structured article data
        structured_article = {
            "metadata": {
                "extraction_timestamp": datetime.now().isoformat(),
                "article_type": article_element.get("article-type", ""),
                "language": article_element.get(
                    "{http://www.w3.org/XML/1998/namespace}lang", "en"
                ),
            }
        }

        if front is not None:
            # Publication information
            structured_article["publication_info"] = self.extract_publication_info(
                front
            )

            # Title
            title_elem = front.find(".//article-title")
            if title_elem is not None:
                structured_article["title"] = self.extract_text_content(title_elem)

            # Authors and affiliations
            contrib_group = front.find(".//contrib-group")
            structured_article["authors"] = self.extract_authors(contrib_group)
            structured_article["affiliations"] = self.extract_affiliations(front)

            # Abstract
            structured_article["abstract"] = self.extract_abstract(front)

            # Keywords
            structured_article["keywords"] = self.extract_keywords(front)

            # Funding
            structured_article["funding"] = self.extract_funding(front)

        # Body content
        if body is not None:
            structured_article["content"] = self.extract_body_content(body)

        # References (from back matter)
        if back is not None:
            ref_list = back.find(".//ref-list")
            if ref_list is not None:
                structured_article["references_count"] = len(ref_list.findall(".//ref"))

        return structured_article


class TXTDownloader:
    def __init__(self, api_keys_file: str, email: str, output_dir: str, file_name: str):
        """
        Initialize the TXTDownloader with API keys, email, and output directory.

        Args:
            api_keys (str): Path of API keys.
            email (str): User email for API.
            output_dir (str): Directory to save downloaded TXT files.
        """
        self.email = email
        self.output_dir = Path(output_dir)
        self.output_dir = output_dir / "files" / file_name
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Load API keys
        with open(api_keys_file, "r") as f:
            self.api_keys = json.load(f)

        self.session = requests.Session()

        # TODO WILEY API KEYS

    def identify_publisher_and_type(self, doi: str) -> tuple:
        """
        Identify the publisher and type of the document based on DOI.

        Args:
            doi (str): Document DOI.
        Returns:
            tuple: (publisher, doc_type)
        """
        doi_lower = doi.lower()

        # Check for preprint servers first
        if "biorxiv" in doi_lower or "10.1101" in doi_lower:
            return "biorxiv", "preprint"
        elif "arxiv" in doi_lower or "arxiv.org" in doi_lower:
            return "arxiv", "preprint"

        # Publisher identification by DOI prefix
        if "10.1016" in doi_lower or "10.1006" in doi_lower:
            return "elsevier", "journal"
        elif "10.1007" in doi_lower or "10.1038" in doi_lower or "10.1186" in doi_lower:
            return "springer", "journal"
        elif "10.1002" in doi_lower:
            return "wiley", "journal"
        elif "10.1371" in doi_lower:
            return "plos", "journal"
        elif "10.3389" in doi_lower:
            return "frontiers", "journal"
        elif "10.1109" in doi_lower:
            return "ieee", "journal"
        else:
            return "unknown", "unknown"

    def get_springer_txt(self, doi: str) -> dict | tuple:
        """
        Download TXT from Springer.

        Args:
            doi (str): Document DOI.
        Returns:
            dict: An structured dictionary with metadata and full text.
        """
        if "springer-nature" not in self.api_keys:
            return None, "No Springer Nature API key"

        # Method 1: Try OpenAccess API
        url = "https://api.springernature.com/openaccess/jats"
        params = {"api_key": self.api_keys["springer-nature"], "q": f'doi:"{doi}"'}

        try:
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                text = ProcessSpringerXML(response.content).extract_metadata()
                return text, None
        except Exception as e:
            log_text.error(f"Error fetching Springer OpenAccess for DOI {doi}: {e}")

    def get_unpaywall_pdf(self, doi: str) -> tuple:
        """Fallback: Try Unpaywall for open access versions
        Args:
            doi (str): Document DOI.
        Returns:
            tuple: (pdf_content as bytes or None, error message or None)
        """
        url = f"https://api.unpaywall.org/v2/{doi}?email={self.email}"

        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get("is_oa") and data.get("best_oa_location"):
                    pdf_url = data["best_oa_location"].get("url_for_pdf")
                    if pdf_url:
                        return pdf_url, None

            return None, log_text.info("No open access version found")
        except Exception as e:
            return None, log_text.error(f"Unpaywall request failed: {str(e)}")

    def download_pdf(self, pdf_url: str, filename: Path) -> bool:
        """Download PDF from URL
        Args:
            pdf_url (str): URL of the PDF file.
            filename (Path): Path to save the PDF file.
        Returns:
            bool: True if download succeeded, False otherwise.
        """
        try:
            response = requests.get(pdf_url, timeout=30, stream=True)
            if response.status_code == 200:
                with open(filename, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                log_text.info(f"Downloaded PDF: {filename}")
                return True
        except:
            pass
        return False

    def download_txt(self, doi: str) -> dict | None | str:
        """
        Download TXT file based on DOI.

        Args:
            doi (str): Document DOI.
        Returns:
            dict: A structured dictionary with metadata and full text, or None if failed.
        """
        safe_doi = re.sub(r"[^\w\-.]", "_", doi)
        publisher, doc_type = self.identify_publisher_and_type(doi)
        log_text.info(
            f"Processing DOI: {doi} | Publisher: {publisher} | Type: {doc_type}"
        )

        pdf_content = None
        error_msg = None
        source_used = None

        # Try publisher-specific APIs first
        if publisher == "springer":
            pdf_content, error_msg = self.get_springer_txt(doi)
            source_used = "Springer Nature API"

        # elif publisher == 'elsevier':
        #     pdf_content, error_msg = self.get_elsevier_pdf(doi)
        #     source_used = "Elsevier API"

        # elif publisher == 'wiley':
        #     pdf_content, error_msg = self.get_wiley_pdf(doi)
        #     source_used = "Wiley TDM API"

        # elif publisher == 'plos':
        #     pdf_content, error_msg = self.get_plos_pdf(doi)
        #     source_used = "PLOS Direct"

        # elif publisher == 'biorxiv':
        #     pdf_content, error_msg = self.get_biorxiv_pdf(doi)
        #     source_used = "bioRxiv Direct"

        # elif publisher == 'arxiv':
        #     pdf_content, error_msg = self.get_arxiv_pdf(doi)
        #     source_used = "arXiv Direct"

        # elif publisher == 'frontiers':
        #     pdf_content, error_msg = self.get_frontiers_pdf(doi)
        #     source_used = "Frontiers Direct"
        # ONLY use Unpaywall as fallback if publisher APIs failed
        if pdf_content is None:
            log_text.info(f"Publisher API failed: {error_msg}")
            log_text.info(f"Trying Unpaywall fallback...")
            pdf_content, fallback_error = self.get_unpaywall_pdf(doi)
            if pdf_content is not None:
                source_used = "Unpaywall"
            else:
                error_msg = f"All methods failed. Publisher: {error_msg}, Unpaywall: {fallback_error}"

        # Save PDF if found - check if is .pdf or structured dict to save as .json file format
        if isinstance(pdf_content, str|None):
            try:
                filename = self.output_dir / f"{safe_doi}.pdf"
                if self.download_pdf(pdf_content, filename):
                    log_text.info(f"Saved PDF: {filename} | Source: {source_used}")
                    return None
                else:
                    log_text.error(f"Failed to download PDF from URL: {pdf_content}")
                    return doi
            except Exception as e:
                log_text.error(f"Error saving PDF for DOI {doi}: {str(e)}")
                return doi
        else:
            filename = self.output_dir / f"{safe_doi}.json"

            with open(filename, "w", encoding="utf-8") as f:
                json.dump(pdf_content, f, indent=2, ensure_ascii=False)
            log_text.info(
                f"Saved structured TXT JSON: {filename} | Source: {source_used}"
            )
            return None


def get_parser() -> argparse.ArgumentParser:
    """
    Parse command-line arguments
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """

    parser = argparse.ArgumentParser(
        description="Download TXT or PDF files based on DOIs from a json folder",
        add_help=True,
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to the json folder with BioProject files with DOIs",
    )
    parser.add_argument(
        "--apikeys", type=str, required=True, help="Path to the API keys JSON file"
    )
    parser.add_argument(
        "--email",
        type=str,
        required=True,
        help="User email for API access and Unpaywall",
    )
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    input_dir = Path(args.input)
    api_keys_file = args.apikeys
    email = args.email

    if not input_dir.is_dir():
        log_text.error(f"Input path is not a directory: {input_dir}")
        return

    all_errors = defaultdict()
    # Process each JSON file in the input directory
    for json_file in input_dir.glob("*.json"):
        logger_dict = defaultdict()
        log_text.info(f"Processing file: {json_file}")
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            articles = data.get("articles")
            for article in articles:
                doi = article.get("doi")
                if doi:
                    bioproject_name = (
                        str(json_file).split("/")[-1].split("_articles.json")[0]
                    )
                    downloader = TXTDownloader(
                        api_keys_file, email, input_dir, bioproject_name
                    )
                    result = downloader.download_txt(doi)

                    if result is None:
                        log_text.info(f"Successfully processed DOI: {doi}")
                    else:
                        log_text.error(
                            f"Failed to process DOI: {doi} | Error: {result}"
                        )
                        logger_dict[bioproject_name] = logger_dict.get(
                            bioproject_name, []
                        ) + [doi]

            # To avoid hitting rate limits
            time.sleep(randint(1, 3))

        except Exception as e:
            log_text.error(f"Error processing file {json_file}: {str(e)}")
            
        # Savinf the log of failed DOIs in the folder
        error_file = bioproject_name + "/failed_dois.json"
        output_dir = input_dir / "files" / error_file
        with open(output_dir, "w", encoding="utf-8") as f:
            json.dump(logger_dict, f, indent=2, ensure_ascii=False)
        log_text.info(f"Saved error log to: {output_dir}")
        log_text.info('')
        all_errors.update(logger_dict)
    # Save a combined log of all failed DOIs
    combined_error_file = input_dir / "files" / "all_failed_dois.json"
    with open(combined_error_file, "w", encoding="utf-8") as f:
        json.dump(all_errors, f, indent=2, ensure_ascii=False)
    log_text.info(f"Saved combined error log to: {combined_error_file}")

if __name__ == "__main__":
    main()