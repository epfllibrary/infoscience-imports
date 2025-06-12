"""Unpaywall client for Infoscience imports"""

import os
from typing import List, Tuple, Optional
from urllib.parse import urljoin
from pathlib import Path
import numpy as np
import requests
import tenacity

from apiclient import (
    APIClient,
    endpoint,
    retry_request,
    JsonResponseHandler,
)
from apiclient.retrying import retry_if_api_request_error
from dotenv import load_dotenv
from config import logs_dir
from config import LICENSE_CONDITIONS
from utils import manage_logger

load_dotenv(os.path.join(os.getcwd(), ".env"))
email = os.environ.get("CONTACT_API_EMAIL")

log_file_path = os.path.join(logs_dir, "logging.log")
logger = manage_logger(log_file_path)

unpaywall_base_url = "https://api.unpaywall.org/v2"

els_api_key = os.environ.get("ELS_API_KEY")

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)

script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
output_dir = project_root / "data" / "pdfs"
output_dir = Path(output_dir).resolve()

output_dir.mkdir(parents=True, exist_ok=True)

PDF_FOLDER = output_dir


def ensure_pdf_folder():
    try:
        os.makedirs(PDF_FOLDER, exist_ok=True)
        if not os.access(PDF_FOLDER, os.W_OK):
            logger.warning(
                f"Permission issue detected for {PDF_FOLDER}, try to fix it."
            )
            os.chmod(
                PDF_FOLDER, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH
            )
    except Exception as e:
        logger.error(f"Error during PDF directory check : {str(e)}")


@endpoint(base_url=unpaywall_base_url)
class Endpoint:
    base = ""
    doi = "/{doi}"

class Client(APIClient):
    @retry_request
    def fetch_by_doi(self, doi, format="best-oa-location", **param_kwargs):
        logger.info("Starting Unpaywall DOI retrieval process.")

        param_kwargs.setdefault("email", email)
        self.params = {**param_kwargs}

        try:
            result = self.get(Endpoint.doi.format(doi=doi), params=self.params)
            logger.debug(f"Unpaywall response for DOI '{doi}': {result}")
            # Check if the result indicates an error
            if result.get("HTTP_status_code") == 404 and result.get("error"):
                message = result.get("message", "No specific error message provided.")
                logger.error(f"Error fetching DOI '{doi}': {message}")
                return None  # Or handle as needed

            if result:
                return self._process_fetch_record(result, format)

        except Exception as e:
            logger.error(
                f"An exception occurred while fetching DOI '{doi}': {str(e)}"
            )
            return None  # Handle any other exceptions as needed

        return None

    def _process_fetch_record(self, x, format):
        if format == "oa":
            return self._extract_oa_infos(x)
        elif format == "best-oa-location":
            return self._extract_best_oa_location_infos(x)
        elif format == "upw":
            return x

    def _extract_oa_infos(self, x):
        rec = {}
        rec["is_oa"] = x["is_oa"]
        rec["oa_status"] = x["oa_status"]
        return rec

    def _extract_best_oa_location_infos(self, record):
        """
        Extracts open access information from the best OA location section of the Unpaywall record.

        Parameters:
            record (dict): A single Unpaywall metadata response for a publication.

        Returns:
            dict: A dictionary containing open access metadata (oa status, license, version, URLs, etc.).
        """
        rec = {}

        # Basic OA info
        rec["is_oa"] = record.get("is_oa")
        rec["oa_status"] = record.get("oa_status")

        best_oa_location = record.get("best_oa_location")
        if not best_oa_location:
            logger.warning("No 'best_oa_location' found for DOI: %s", record.get("doi"))
            return rec

        logger.debug("Extracting OA metadata from best_oa_location.")

        # Extract license and version even if URL is missing
        rec["license"] = best_oa_location.get("license")
        rec["version"] = best_oa_location.get("version")

        # Build a list of available URLs (even if no direct PDF)
        urls = [
            best_oa_location.get("url_for_pdf"),
            best_oa_location.get("url_for_landing_page"),
            best_oa_location.get("url"),
        ]
        urls = list(filter(None, urls))  # remove None values
        rec["pdf_urls"] = "|".join(urls) if urls else None

        # Only try to download if url_for_pdf is explicitly provided and license is valid
        license_type = rec["license"]
        if (
            record.get("is_oa")
            and license_type
            and any(l in license_type for l in LICENSE_CONDITIONS["allowed_licenses"])
        ):
            try:
                doi = record.get("doi") or record.get("doi_url", "").replace(
                    "https://doi.org/", ""
                )

                valid_pdf_url, local_filename = self._validate_and_download_pdf(
                    urls, doi
                )
                if valid_pdf_url:
                    rec["pdf_url"] = valid_pdf_url
                    rec["valid_pdf"] = local_filename
                    logger.info(
                        f"Valid PDF found at: {valid_pdf_url}, saved as {local_filename}"
                    )
                else:
                    logger.warning("No valid PDF downloaded for DOI: %s", record.get("doi"))
            except Exception as e:
                logger.error(
                    "Error downloading PDF for DOI %s: %s", record.get("doi"), str(e)
                )
        else:
            logger.info(
                "PDF download skipped for DOI %s (missing url_for_pdf or disallowed license).",
                record.get("doi"),
            )

        return rec

    def _validate_and_download_pdf(
        self, urls: List[str], doi: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Iterates through provided and Crossref-derived URLs to find a valid downloadable PDF.

        Parameters:
            urls (List[str]): List of possible PDF URLs from Unpaywall.
            doi (str): DOI used to name the downloaded file.

        Returns:
            Tuple[str, str] if successful: (PDF URL, local filename),
            otherwise (None, None).
        """
        ensure_pdf_folder()

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,application/x-pdf,application/octet-stream,*/*",
            "Referer": "https://www.google.com/",
            "DNT": "1",  
            "Upgrade-Insecure-Requests": "1",
        }

        # Combine with additional Crossref PDF links (fallbacks)
        all_urls = list(dict.fromkeys(urls + self._get_crossref_pdf_links(doi)))

        for url in all_urls:
            if not url:
                continue
            try:
                logger.debug(f"Trying candidate PDF URL: {url}")
                pdf_url, filename = self._check_and_download_pdf(
                    url, doi, str(PDF_FOLDER), headers
                )

                if pdf_url and filename:
                    full_path = os.path.join(PDF_FOLDER, filename)
                    if filename.lower().endswith(".pdf") and os.path.isfile(full_path):
                        logger.info(
                            f"Successfully validated and downloaded PDF from: {pdf_url}"
                        )
                        return pdf_url, filename
                    else:
                        logger.warning(
                            f"File was downloaded but does not appear to be a valid PDF: {filename}"
                        )
            except Exception as e:
                logger.error(f"Unexpected error while downloading from {url}: {e}")

        logger.warning(f"All attempts failed: no valid PDF found for DOI {doi}")
        return None, None

    def _check_and_download_pdf(
        self, url: str, doi: str, pdf_folder: str, headers: dict
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Attempts to download a PDF from a given URL after checking content type and redirects.

        Returns:
            Tuple[str, str] if successful (URL, filename), or (None, None) otherwise.
        """

        def try_download(attempt_url):
            # Elsevier-specific case (API route)
            if "api.elsevier.com" in attempt_url:
                logger.info(f"Using Elsevier API download for URL: {attempt_url}")
                return self._get_elsevier_pdf(doi, els_api_key, pdf_folder)

            try:
                response = requests.get(
                    attempt_url,
                    headers=headers,
                    stream=True,
                    timeout=30,
                    allow_redirects=True,  # Follow redirects to actual PDF
                )
                response.raise_for_status()

                final_url = response.url
                content_type = response.headers.get("Content-Type", "").lower()

                logger.debug(f"Resolved URL after redirection: {final_url}")
                logger.debug(f"Content-Type returned: {content_type}")

                # Acceptable content or file extension
                is_pdf = "application/pdf" in content_type or final_url.lower().endswith(
                    ".pdf"
                )

                if is_pdf:
                    return self._download_pdf(response, final_url, doi, pdf_folder)
                else:
                    logger.warning(
                        f"Rejected non-PDF content from {final_url} (Content-Type: {content_type})"
                    )
                    return None, None
            except requests.RequestException as e:
                logger.error(f"Error downloading from {attempt_url}: {e}")
                return None, None

        # First direct try
        result = try_download(url)
        if result[0] is not None:
            return result

        # Fallback: attempt with '.pdf' appended if applicable
        if not url.lower().endswith(".pdf") and "doi.org" not in url.lower():
            pdf_url = urljoin(url, url.split("/")[-1] + ".pdf")
            logger.debug(f"Retrying with '.pdf' appended URL: {pdf_url}")
            result = try_download(pdf_url)
            if result[0] is not None:
                return result

        return None, None

    def _download_pdf(
        self, response: requests.Response, url: str, doi: str, pdf_folder: str
    ) -> Tuple[str, str]:
        """
        Saves a PDF response to disk.

        Parameters:
            response (requests.Response): The response object from requests.get().
            url (str): The final resolved URL of the PDF.
            doi (str): The DOI to use as filename.
            pdf_folder (str): Destination directory.

        Returns:
            Tuple[str, str]: (original URL, saved filename)
        """
        filename = f"{doi.replace('/', '_')}.pdf"
        file_path = os.path.join(pdf_folder, filename)

        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        logger.info(f"PDF saved to: {file_path}")
        return url, filename

    def _get_elsevier_pdf(self, doi, api_key, pdf_folder):
        """
        Downloads a PDF article from Elsevier's API using the DOI (Digital Object Identifier) of the article.

        Parameters:
        doi (str): The DOI of the article to download.
        api_key (str): Your Elsevier API key for authentication.

        Notes:
        ------
        - Make sure to replace 'your_api_key_here' with a valid Elsevier API key.
        - The DOI should be a valid DOI for an article available in the Elsevier database.
        """

        url = f"https://api.elsevier.com/content/article/doi/{doi}"

        headers = {"Accept": "application/pdf", "X-ELS-APIKey": api_key}

        ensure_pdf_folder()

        response = requests.get(url, headers=headers, stream=True, timeout=30)

        if response.status_code == 200:
            return self._download_pdf(response, url, doi, pdf_folder)
        else:
            logger.error(f"Error Elsevier downloading PDF : {response.status_code}")
            logger.error(response.text)
            return None

    @staticmethod
    def _get_crossref_pdf_links(doi: str) -> List[str]:
        """
            Retrieve VOR PDF links for a given DOI using the Crossref API.

            Args:
                doi (str): The Digital Object Identifier of the publication.

            Returns:
                List[str]: A list of URLs that match the criteria.
            """
        if not isinstance(doi, str) or not doi.strip():
            raise ValueError("DOI must be a non-empty string")

        base_url = "https://api.crossref.org/works/"
        url = f"{base_url}{doi}?mailto={email}"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            links = data.get("message", {}).get("link", [])
            filtered_links = [
                link["URL"]
                for link in links
                if link.get("content-version") in ["vor", "am"]
            ]

            if filtered_links:
                logger.info(f"Fulltext links found for DOI {doi}: {filtered_links}")
            else:
                logger.info(f"No Fulltext links found for DOI: {doi}")

            return filtered_links

        except requests.RequestException as e:
            logger.error(f"Error fetching Crossref data for DOI {doi}: {str(e)}")
            return []
        except (KeyError, IndexError, ValueError) as e:
            logger.error(f"Error parsing Crossref response for DOI {doi}: {str(e)}")
            return []

UnpaywallClient = Client(
    response_handler=JsonResponseHandler,
)
