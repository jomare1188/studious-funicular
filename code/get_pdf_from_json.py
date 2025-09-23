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
import threading
import time
from collections import defaultdict
from datetime import datetime
from functools import wraps
from pathlib import Path
from random import randint
from urllib.parse import parse_qs, unquote, urlparse
from xml.etree import ElementTree as ET

import regex as re
import requests
from rich.logging import RichHandler
from wiley_tdm import TDMClient

# Set up logging
FORMAT = "%(message)s"

logging.basicConfig(
    format=FORMAT,
    level="INFO",
    handlers=[RichHandler(show_time=False, show_path=False, markup=False)],
)

log_text = logging.getLogger("rich")
log_text.setLevel(20)


class APIRateLimiter:
    """Thread-safe API rate limiter with request counting and automatic sleep."""

    def __init__(self, request_limit: int = 450, sleep_duration: int = 90000):
        """
        Initialize the rate limiter.

        Args:
            request_limit: Maximum requests per API before triggering sleep
            sleep_duration: Sleep duration in seconds when limit is reached
        """
        self.request_limit = request_limit
        self.sleep_duration = sleep_duration
        self.download_counts = {
            "springer": 0,
            "elsevier": 0,
            "wiley": 0,
            "frontiers": 0,
            "aps": 0,
            "unpaywall": 0,
        }
        self._lock = threading.Lock()
        self._sleeping_apis = set()

    def track_request(self, api_name: str):
        """
        Decorator to track API requests and handle rate limiting.

        Args:
            api_name: Name of the API (must match keys in download_counts)
        """

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Check if this API is currently sleeping
                if api_name in self._sleeping_apis:
                    log_text.info(
                        f"{api_name} API is currently rate-limited, waiting..."
                    )
                    while api_name in self._sleeping_apis:
                        time.sleep(10)  # Check every 10 seconds

                # Increment counter before making request
                with self._lock:
                    self.download_counts[api_name] += 1
                    current_count = self.download_counts[api_name]

                    log_text.info(f"{api_name} request #{current_count}")

                    # Check if we've reached the limit
                    if current_count >= self.request_limit:
                        self._sleeping_apis.add(api_name)
                        log_text.warning(
                            f"{api_name} has reached {self.request_limit} requests. "
                            f"Sleeping for {self.sleep_duration} seconds..."
                        )

                        # Reset counter
                        self.download_counts[api_name] = 0

                        # Sleep in a separate thread to avoid blocking other APIs
                        def sleep_and_wake():
                            time.sleep(self.sleep_duration)
                            with self._lock:
                                if api_name in self._sleeping_apis:
                                    self._sleeping_apis.remove(api_name)
                                    log_text.info(
                                        f"{api_name} API is now available again."
                                    )

                        threading.Thread(target=sleep_and_wake, daemon=True).start()

                        # Block this request until sleep is over
                        while api_name in self._sleeping_apis:
                            time.sleep(1)

                # Execute the original function
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # If request failed, decrement counter
                    with self._lock:
                        if self.download_counts[api_name] > 0:
                            self.download_counts[api_name] -= 1
                    raise e

            return wrapper

        return decorator

    def get_counts(self) -> dict[str, int]:
        """Get current request counts for all APIs."""
        with self._lock:
            return self.download_counts.copy()

    def reset_count(self, api_name: str):
        """Manually reset count for a specific API."""
        with self._lock:
            self.download_counts[api_name] = 0
            if api_name in self._sleeping_apis:
                self._sleeping_apis.remove(api_name)

    def reset_all_counts(self):
        """Reset all API counts."""
        with self._lock:
            for api in self.download_counts:
                self.download_counts[api] = 0
            self._sleeping_apis.clear()


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


class ProcessElsevierXML:
    def __init__(self, content: bytes):
        """
        Initialize the ProcessElsevierXML with XML content.

        Args:
            content (bytes): XML content as bytes.
        """
        try:
            # Register namespaces to handle prefixed tags and the default namespace
            self.namespaces = {
                "svapi": "http://www.elsevier.com/xml/svapi/article/dtd",
                "ce": "http://www.elsevier.com/xml/common/dtd",
                "dc": "http://purl.org/dc/elements/1.1/",
                "prism": "http://prismstandard.org/namespaces/basic/2.0/",
                "ja": "http://www.elsevier.com/xml/ja/dtd",
                "xocs": "http://www.elsevier.com/xml/xocs/dtd",
                "dcterms": "http://purl.org/dc/terms/",
                "sb": "http://www.elsevier.com/xml/common/struct-bib/dtd",
            }
            self.root = ET.fromstring(content)
        except ET.ParseError as e:
            log_text.error(f"Error parsing XML: {e}")
            self.root = None

    def extract_text_content(self, element: ET.Element) -> str:
        """
        Extract all text content from an element, including nested text.

        Args:
            element (ET.Element): The XML element to extract text from.
        Returns:
            str: The extracted text content.
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

    def extract_publication_info(self, coredata: ET.Element) -> dict:
        """
        Extract publication metadata from the <coredata> element.
        Args:
            coredata (ET.Element): The <coredata> element of the XML.
        Returns:
            dict: A dictionary containing publication metadata fields.
        """
        if coredata is None:
            return {}

        pub_info = {}
        pub_info["journal"] = coredata.findtext(
            "prism:publicationName", namespaces=self.namespaces
        )
        pub_info["issn"] = coredata.findtext("prism:issn", namespaces=self.namespaces)
        pub_info["publisher"] = coredata.findtext(
            "prism:publisher", namespaces=self.namespaces
        )
        pub_info["doi"] = coredata.findtext("prism:doi", namespaces=self.namespaces)
        pub_info["volume"] = coredata.findtext(
            "prism:volume", namespaces=self.namespaces
        )
        pub_info["cover_date"] = coredata.findtext(
            "prism:coverDate", namespaces=self.namespaces
        )

        return pub_info

    def extract_authors(self, head: ET.Element, coredata: ET.Element) -> list:
        """
        Extract author information from the <head> element, with a fallback to <coredata>.
        Args:
            head (ET.Element): The <head> element of the XML.
            coredata (ET.Element): The <coredata> element of the XML.
        Returns:
            list: A list of authors with their details.
        """
        authors = []
        if head is not None:
            # Primary method: structured author data
            author_group = head.find("ce:author-group", self.namespaces)
            if author_group is not None:
                for author_elem in author_group.findall("ce:author", self.namespaces):
                    author = {}
                    given_name = author_elem.findtext(
                        "ce:given-name", namespaces=self.namespaces
                    )
                    surname = author_elem.findtext(
                        "ce:surname", namespaces=self.namespaces
                    )
                    author["given_names"] = given_name if given_name else ""
                    author["surname"] = surname if surname else ""
                    author["full_name"] = f"{given_name} {surname}".strip()

                    corr_ref = author_elem.find(
                        'ce:cross-ref[@refid="cor0001"]', self.namespaces
                    )
                    author["is_corresponding"] = corr_ref is not None

                    authors.append(author)

        # Fallback method: simple author list from coredata
        if not authors and coredata is not None:
            for creator_elem in coredata.findall("dc:creator", self.namespaces):
                authors.append({"full_name": creator_elem.text})

        return authors

    def extract_affiliations(self, head: ET.Element) -> dict:
        """
        Extract affiliation information from the <head> element.
        Args:
            head (ET.Element): The <head> element of the XML.
        Returns:
            dict: A dictionary mapping affiliation IDs to their details.
        """
        affiliations = {}
        if head is None:
            return affiliations

        author_group = head.find("ce:author-group", self.namespaces)
        if author_group is not None:
            for aff_elem in author_group.findall("ce:affiliation", self.namespaces):
                aff_id = aff_elem.get("id")
                if aff_id:
                    affiliations[aff_id] = self.extract_text_content(
                        aff_elem.find("ce:textfn", self.namespaces)
                    )
        return affiliations

    def extract_abstract(self, head: ET.Element) -> dict:
        """
        Extract abstract content from the <head> element.
        Args:
            head (ET.Element): The <head> element of the XML.
        Returns:
            dict: A dictionary containing abstract sections and full text.
        """
        if head is None:
            return {}
        abstract_elem = head.find("ce:abstract", self.namespaces)
        if abstract_elem is None:
            return {}

        return {"full_text": self.extract_text_content(abstract_elem)}

    def extract_keywords(self, head: ET.Element, coredata: ET.Element) -> list:
        """
        Extract keywords from <head> with a fallback to <coredata>.
        Args:
            head (ET.Element): The <head> element of the XML.
            coredata (ET.Element): The <coredata> element of the XML.
        Returns:
            list: A list of keywords.
        """
        keywords = []
        if head is not None:
            kwd_group = head.find("ce:keywords", self.namespaces)
            if kwd_group is not None:
                for kwd in kwd_group.findall("ce:keyword/ce:text", self.namespaces):
                    keywords.append(self.extract_text_content(kwd))

        if not keywords and coredata is not None:
            for subject in coredata.findall("dcterms:subject", self.namespaces):
                if subject.text:
                    keywords.append(subject.text.strip())

        return keywords

    def _recursive_section_extract(self, section_element: ET.Element) -> dict:
        """
        Helper function to recursively extract sections and subsections.

        Args:
            section_element (ET.Element): The section XML element.
        Returns:
            dict: A dictionary representing the section and its subsections.
        """
        section_data = {}
        title_elem = section_element.find("ce:section-title", self.namespaces)
        if title_elem is not None:
            section_data["title"] = self.extract_text_content(title_elem)

        # Extract paragraphs directly under this section
        section_data["paragraphs"] = [
            self.extract_text_content(p)
            for p in section_element.findall("ce:para", self.namespaces)
        ]

        # Recursively find subsections
        subsections = []
        for subsec_elem in section_element.findall("ce:section", self.namespaces):
            subsections.append(self._recursive_section_extract(subsec_elem))

        if subsections:
            section_data["subsections"] = subsections

        return section_data

    def extract_body_content(self, body: ET.Element) -> dict:
        """
        Extract the main body content recursively.

        Args:
            body (ET.Element): The <body> element of the XML.
        Returns:
            dict: A dictionary containing sections, subsections, and full text.
        """
        if body is None:
            return {}

        content = {}
        sections = []

        sections_container = body.find("ce:sections", self.namespaces)
        if sections_container is not None:
            for sec in sections_container.findall("ce:section", self.namespaces):
                sections.append(self._recursive_section_extract(sec))

        content["sections"] = sections
        content["full_text"] = self.extract_text_content(body)

        return content

    def extract_references(self, tail: ET.Element) -> list:
        """
        Extract bibliographic references from the <tail> element.
        Args:
            tail (ET.Element): The <tail> element of the XML.
        Returns:
            list: A list of references in text form.
        """
        if tail is None:
            return []

        references = []
        bib_section = tail.find(".//ce:bibliography", self.namespaces)
        if bib_section is not None:
            for ref_elem in bib_section.findall(".//ce:bib-reference", self.namespaces):
                ref_text = ref_elem.find("ce:source-text", self.namespaces)
                if ref_text is not None and ref_text.text:
                    references.append(ref_text.text.strip())
                else:  # Fallback for a different structure
                    ref_text = self.extract_text_content(ref_elem)
                    if ref_text:
                        references.append(ref_text)
        return references

    def extract_metadata(self) -> dict:
        """
        Extract all metadata from the Elsevier XML content.
        Returns:
            dict: A dictionary containing metadata fields in a structured form.
        """
        if self.root is None:
            return {"error": "XML could not be parsed."}

        coredata = self.root.find("svapi:coredata", self.namespaces)
        original_text = self.root.find("svapi:originalText", self.namespaces)

        if original_text is None:
            log_text.error("Could not find <originalText> in XML")
            return {}

        article_element = original_text.find(".//ja:article", self.namespaces)
        if article_element is None:
            log_text.warning("No <ja:article> element found in XML.")
            return {}

        head = article_element.find("ja:head", self.namespaces)
        body = article_element.find("ja:body", self.namespaces)
        tail = article_element.find("ja:tail", self.namespaces)

        structured_article = {
            "metadata": {
                "extraction_timestamp": datetime.now().isoformat(),
                "language": article_element.get(
                    "{http://www.w3.org/XML/1998/namespace}lang", "en"
                ),
            }
        }

        if coredata is not None:
            structured_article["publication_info"] = self.extract_publication_info(
                coredata
            )
            title = coredata.findtext("dc:title", namespaces=self.namespaces)
            if title:
                structured_article["title"] = title.strip()

        structured_article["authors"] = self.extract_authors(head, coredata)
        structured_article["affiliations"] = self.extract_affiliations(head)
        structured_article["abstract"] = self.extract_abstract(head)
        structured_article["keywords"] = self.extract_keywords(head, coredata)
        structured_article["content"] = self.extract_body_content(body)
        structured_article["references"] = self.extract_references(tail)

        return structured_article


class APSDownloader:
    def __init__(self):
        """
        Initialize the APSDownloader with a requests session.

        """
        self.session = requests.Session()

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        self.session.headers.update(headers)

    def get_aps_pdf(self, doi: str) -> tuple:
        """
        Download PDF from APS.

        Args:
            doi (str): Document DOI.
        Returns:
            tuple: (pdf_content as bytes or None, error message or None)
        """
        abstract_url = f"https://apsjournals.apsnet.org/doi/{doi}"
        pdf_url = f"https://apsjournals.apsnet.org/doi/pdf/{doi}"

        try:
            # Visit the abstract page first. This makes our session look more legitimate
            self.session.get(abstract_url, timeout=30)

            # Request the PDF with the Referer header. The session already has the main headers (like User-Agent).  We add the specific Referer for this one request.
            pdf_request_headers = {"Referer": abstract_url}

            response = self.session.get(
                pdf_url, headers=pdf_request_headers, timeout=30, allow_redirects=True
            )

            if (
                response.status_code == 200
                and "application/pdf" in response.headers.get("Content-Type", "")
            ):
                return response.content, None
            else:
                return (
                    None,
                    f"Failed to download PDF. Status: {response.status_code}, URL: {pdf_url}",
                )

        except Exception as e:
            return None, f"An error occurred during the request for APS PDF: {e}"


class TXTDownloader:
    _rate_limiter = APIRateLimiter(request_limit=450, sleep_duration=90000)
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
    
        
    @property
    def rate_limiter(self):
        """Access to the rate limiter."""
        return self._rate_limiter
    
    @property
    def download_counts(self):
        """Access to current download counts."""
        return self._rate_limiter.get_counts()

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
        elif "10.1002" in doi_lower or "10.1111" in doi_lower:
            return "wiley", "journal"
        elif "10.1371" in doi_lower:
            return "plos", "journal"
        elif "10.3389" in doi_lower:
            return "frontiers", "journal"
        elif "10.1094" in doi_lower:
            return "aps", "journal"
        elif "10.1109" in doi_lower:
            return "ieee", "journal"
        else:
            return "unknown", "unknown"

    @_rate_limiter.track_request("springer")
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
            return None, log_text.error(
                f"Error fetching Springer OpenAccess for DOI {doi}: {e}"
            )

    @_rate_limiter.track_request("elsevier")
    def get_elsevier_txt(self, doi: str) -> dict | tuple:
        """Download TXT from Elsevier
        Args:
            doi (str): Document DOI.
        Returns:
            dict: An structured dictionary with metadata and full text.
        """
        if "elsevier" not in self.api_keys:
            return None, "No Elsevier API key"

        url = f"https://api.elsevier.com/content/article/doi/{doi}"
        headers = {
            "X-ELS-APIKey": self.api_keys["elsevier"],
            "Accept": "text/xml",
        }

        try:
            response = self.session.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                text = ProcessElsevierXML(response.content).extract_metadata()
                return text, None
            elif response.status_code == 403:
                return None, "Access denied or rate limit exceeded"
            else:
                return None, f"Elsevier API error: {response.status_code}"
        except Exception as e:
            return None, f"Elsevier request failed: {str(e)}"

    @_rate_limiter.track_request("wiley")
    def get_wiley_pdf(self, doi: str) -> tuple:
        """Download PDF from Wiley using TDM API
        Args:
            doi (str): Document DOI.
        Returns:
            tuple: (pdf_content as bytes or None, error message or None)
        """
        if "wiley" not in self.api_keys:
            return None, "No Wiley API key"

        try:
            tdm = TDMClient()
            tdm.download_dir = self.output_dir
            local_path = tdm.download_pdf(doi)
            if not local_path:
                return None, "Wiley TDM download returned no file"
            return "WILEY_DOWNLOADED", None
        except Exception as exc:
            return None, f"Wiley TDM error: {exc}"

    @_rate_limiter.track_request("frontiers")
    def get_frontiers_pdf(self, doi: str) -> tuple:
        """Direct download from Frontiers
        Args:
            doi (str): Document DOI.
        Returns:
            tuple: (pdf_content as bytes or None, error message or None)
        """
        try:
            if "10.3389" in doi:
                # Frontiers PDF URL pattern
                pdf_url = f"https://www.frontiersin.org/articles/{doi}/pdf"

                response = self.session.get(pdf_url, timeout=30)
                if response.status_code == 200:
                    return response.content, None

            return None, "Frontiers PDF not found"
        except Exception as e:
            return None, f"Frontiers request failed: {str(e)}"
        # return f"https://www.frontiersin.org/articles/{doi}/pdf", None

    @_rate_limiter.track_request("unpaywall")
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
            # download_counts["springer"] += 1

        elif publisher == "elsevier":
            pdf_content, error_msg = self.get_elsevier_txt(doi)
            source_used = "Elsevier API"
            # download_counts["elsevier"] += 1

        elif publisher == "wiley":
            pdf_content, error_msg = self.get_wiley_pdf(doi)
            source_used = "Wiley TDM API"
            # download_counts["wiley"] += 1
            if pdf_content == "WILEY_DOWNLOADED":
                log_text.info(f"Wiley PDF already saved | Source: {source_used}")
                return None

        elif publisher == "aps":
            apsdownload = APSDownloader()
            pdf_content, error_msg = apsdownload.get_aps_pdf(doi)
            source_used = "American Phytopathological Society (APS)"
            # download_counts["aps"] += 1

        # elif publisher == 'biorxiv':
        #     pdf_content, error_msg = self.get_biorxiv_pdf(doi)
        #     source_used = "bioRxiv Direct"

        # elif publisher == 'arxiv':
        #     pdf_content, error_msg = self.get_arxiv_pdf(doi)
        #     source_used = "arXiv Direct"

        elif publisher == "frontiers":
            pdf_content, error_msg = self.get_frontiers_pdf(doi)
            source_used = "Frontiers Direct"
            # download_counts["frontiers"] += 1
        # ONLY use Unpaywall as fallback if publisher APIs failed
        if pdf_content is None:
            log_text.info(f"Publisher API failed: {error_msg}")
            log_text.info(f"Trying Unpaywall fallback...")
            pdf_content, fallback_error = self.get_unpaywall_pdf(doi)
            if pdf_content is not None:
                source_used = "Unpaywall"
                # download_counts["unpaywall"] += 1
            else:
                error_msg = f"All methods failed. Publisher: {error_msg}, Unpaywall: {fallback_error}"

        # Save PDF if found - check if is .pdf or structured dict to save as .json file format
        if isinstance(pdf_content, str | None):
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
        elif isinstance(pdf_content, bytes):
            filename = self.output_dir / f"{safe_doi}.pdf"
            try:
                with open(filename, "wb") as f:
                    f.write(pdf_content)
                log_text.info(f"Saved PDF: {filename} | Source: {source_used}")
                return None
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
        
    def print_status(self):
        """Print current request counts and status."""
        counts = self.download_counts
        sleeping = self.rate_limiter._sleeping_apis
        
        log_text.info("\n=== API Request Status ===")
        for api, count in counts.items():
            status = " (SLEEPING)" if api in sleeping else ""
            log_text.info(f"{api.capitalize()}: {count}/450{status}")
        log_text.info("========================\n")


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


def fix_doi(article: dict) -> str:
    """
    Fix DOI in an article entry by extracting it from the link if necessary
    Args:
        article (list): An article entry containing 'doi' and 'link' fields.
    Returns:
        tuple: (bool indicating if a fix was made, message)
    """
    current_doi = article.get("doi")
    link = article.get("link")

    # If DOI is missing or invalid, try to extract from link
    if not is_valid_doi(current_doi):
        extracted_doi = extract_doi_from_url(link)
        if extracted_doi and is_valid_doi(extracted_doi):
            return extracted_doi

    return current_doi


def is_valid_doi(doi: str) -> bool:
    """
    Check if a DOI looks complete and valid
    Args:
        doi (str): The DOI string to validate.
    Returns:
        bool: True if DOI is valid, False otherwise.
    """
    if not doi:
        return False

    # DOI should start with 10. and have at least one more segment
    if not doi.startswith("10."):
        return False

    # Should have at least one slash after 10.
    parts = doi.split("/")
    if len(parts) < 2:
        return False

    # Check for incomplete patterns
    incomplete_patterns = [
        r"^10\.3389/fpls$",  # Frontiers incomplete
        r"^10\.[\d]+$",  # Just the prefix
    ]

    for pattern in incomplete_patterns:
        if re.match(pattern, doi):
            return False

    return True


def extract_doi_from_url(url: str) -> str | None:
    """
    Extract DOI from various academic publisher URLs.
    Returns a DOI like '10.3389/fpls.2024.1372809' or None.
    """
    if not url:
        return None

    url = url.strip()
    unq = unquote(url)  # decode %2F, etc.

    # 1) General DOI anywhere in the URL (stop at next slash, ? or #)
    doi_re = re.compile(r"(10\.\d{4,9}/[^/?#\s]+)", re.IGNORECASE)
    m = doi_re.search(unq)
    if m:
        doi = m.group(1).rstrip(".,;:")  # trim trailing punctuation
        return doi

    # 2) Query param fallback (e.g., ?doi=10.XXX/YYY)
    parsed = urlparse(unq)
    qs = parse_qs(parsed.query)
    for k in ("doi", "DOI"):
        if k in qs and qs[k]:
            candidate = qs[k][0].split("#")[0].split("?")[0]
            if candidate.startswith("10."):
                return candidate

    # 3) Host-specific fallback (e.g., frontiers path heuristics)
    path = parsed.path
    m = re.search(r"/articles/(10\.\d{4,9}/[^/]+)", path, re.IGNORECASE)
    if m:
        return m.group(1).rstrip(".,;:")

    return None


def pmid2doi(pmid: str) -> str | None:
    """
    Convert a PubMed ID (PMID) to a DOI using the NCBI E-utilities API.
    Args:
        pmid (str): The PubMed ID to convert.
    Returns:
        str | None: The corresponding DOI if found, otherwise None.
    """
    if not pmid or not pmid.isdigit():
        return None

    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id={pmid}&retmode=json"

    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            result = data.get("result", {})
            uid_data = result.get(pmid, {})

            # Check multiple possible locations for DOI
            # 1. Check articleids array for DOI entries
            article_ids = uid_data.get("articleids", [])
            for article_id in article_ids:
                if article_id.get("idtype") == "doi":
                    doi = article_id.get("value")
                    if doi and doi.startswith("10."):
                        return doi

            # 2. Check elocationid as fallback
            doi = uid_data.get("elocationid")
            if doi and doi.startswith("10."):
                return doi

    except Exception as e:
        log_text.error(f"Error converting PMID {pmid} to DOI: {e}")

    return None


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
            if len(articles) > 0:
                for article in articles:
                    doi = fix_doi(article)
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
            pmids = data.get("PubMedIDs")
            log_text.info(f"Processing {len(pmids)} PubMed IDs")
            for pmid in pmids:
                doi = pmid2doi(pmid)
                bioproject_name = (
                    str(json_file).split("/")[-1].split("_articles.json")[0]
                )
                if doi:
                    downloader = TXTDownloader(
                        api_keys_file, email, input_dir, bioproject_name
                    )
                    result = downloader.download_txt(doi)

                    if result is None:
                        log_text.info(
                            f"Successfully processed PMID: {pmid} -> DOI: {doi}"
                        )
                    else:
                        log_text.error(
                            f"Failed to process PMID: {pmid} -> DOI: {doi} | Error: {result}"
                        )
                        logger_dict[bioproject_name] = logger_dict.get(
                            bioproject_name, []
                        ) + [doi]
                else:
                    log_text.error(f"Could not convert PMID to DOI: {pmid}")
                    logger_dict[bioproject_name] = logger_dict.get(
                        bioproject_name, []
                    ) + [f"PMID:{pmid}"]
            # To avoid hitting rate limits
            time.sleep(randint(1, 3))
            downloader.print_status()
        except Exception as e:
            log_text.error(f"Error processing file {json_file}: {str(e)}")
            

        # Savinf the log of failed DOIs in the folder
        bioproject_name = str(json_file).split("/")[-1].split("_articles.json")[0]
        error_file = bioproject_name + "/failed_dois.json"
        output_dir = input_dir / "files" / error_file
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        with open(output_dir, "w", encoding="utf-8") as f:
            json.dump(logger_dict, f, indent=2, ensure_ascii=False)
        log_text.info(f"Saved error log to: {output_dir}")
        log_text.info("")
        all_errors.update(logger_dict)
    # Save a combined log of all failed DOIs
    combined_error_file = input_dir / "files" / "all_failed_dois.json"
    with open(combined_error_file, "w", encoding="utf-8") as f:
        json.dump(all_errors, f, indent=2, ensure_ascii=False)
    log_text.info(f"Saved combined error log to: {combined_error_file}")
    downloader.print_status()

if __name__ == "__main__":
    main()
