"""Unpaywall client for Infoscience imports"""

import os
from typing import List, Tuple, Optional
from urllib.parse import urljoin
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
email = os.environ.get("UPW_EMAIL")

log_file_path = os.path.join(logs_dir, "unpaywall_client.log")
logger = manage_logger(log_file_path)

unpaywall_base_url = "https://api.unpaywall.org/v2"

els_api_key = os.environ.get("ELS_API_KEY")

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)

@endpoint(base_url=unpaywall_base_url)
class Endpoint:
    base = ""
    doi = "/{doi}"

class Client(APIClient):
    @retry_request
    def fetch_by_doi(self, doi, format="best-oa-location", **param_kwargs):
        logger.info("Starting upw DOI retrieval process.")

        param_kwargs.setdefault("email", email)
        self.params = {**param_kwargs}

        try:
            result = self.get(Endpoint.doi.format(doi=doi), params=self.params)

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

    def _extract_best_oa_location_infos(self, x):
        rec = self._extract_oa_infos(x)
        logger.info("Extracting OA location infos.")

        if (
            rec.get("is_oa")
            and rec.get("oa_status") in LICENSE_CONDITIONS["allowed_oa_statuses"]
        ):
            best_oa_location = x.get("best_oa_location")
            license_type = best_oa_location["license"]
            rec["license"] = license_type
            rec["version"] = best_oa_location["version"]

            result = None
            logger.debug(f"License type: {license_type}")

            if (
                license_type is not None
                and license_type is not np.nan
                and any(
                    allowed in license_type
                    for allowed in LICENSE_CONDITIONS["allowed_licenses"]
                )
            ):
                urls = [
                    best_oa_location["url_for_pdf"],
                    best_oa_location["url_for_landing_page"],
                    best_oa_location["url"],
                ]
                urls = list(filter(None, urls))
                result = "|".join(urls)
                logger.info("URLs concatenated successfully.")

                valid_pdf_url, local_filename = self._validate_and_download_pdf(
                    urls, x["doi"]
                )
                if valid_pdf_url:
                    rec["pdf_url"] = valid_pdf_url
                    rec["valid_pdf"] = local_filename
                    logger.info(
                        f"Valid PDF found at: {valid_pdf_url} and saved as {local_filename}"
                    )
                else:
                    logger.warning("No valid PDF found among the provided URLs.")
            else:
                logger.warning("License type is invalid or not allowed.")

            rec["pdf_urls"] = result
        return rec

    def _validate_and_download_pdf(
        self, urls: List[str], doi: str
    ) -> Tuple[Optional[str], Optional[str]]:
        pdf_folder = "../data/pdfs"
        os.makedirs(pdf_folder, exist_ok=True)

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/pdf,application/x-pdf,application/octet-stream,*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.google.com/",
        }

        # Regroupe toutes les URLs
        all_urls = list(dict.fromkeys(urls + self._get_crossref_pdf_links(doi)))

        for url in all_urls:
            try:
                # Vérification de l'URL et téléchargement du PDF si valide
                pdf_url, filename = self._check_and_download_pdf(
                    url, doi, pdf_folder, headers
                )

                if pdf_url and filename:
                    # Vérification du fichier téléchargé (est-ce bien un PDF ?)
                    if filename.lower().endswith(".pdf") and os.path.isfile(
                        os.path.join(pdf_folder, filename)
                    ):
                        logger.info(f"PDF file successfully downloaded from {url}")
                        return pdf_url, filename
                    else:
                        logger.warning(f"Not valid file downloaded from {url}")
            except Exception as e:
                # Gestion des exceptions pour ne pas interrompre le processus
                logger.error(f"Error downloading {url}: {e}")

        # Si aucun PDF n'est trouvé
        logger.warning(f"No PDF file found for DOI: {doi}")
        return None, None

    def _check_and_download_pdf(
        self, url: str, doi: str, pdf_folder: str, headers: dict
    ) -> Tuple[Optional[str], Optional[str]]:
        def try_download(attempt_url):
            # Vérification si l'URL contient "api.elsevier.com"
            if "api.elsevier.com" in attempt_url:
                logger.info(
                    f"Elsevier PDF detected : {attempt_url}. Download attempt via dedicated API."
                )
                return self._get_elsevier_pdf(doi, els_api_key, pdf_folder)

            # Processus normal pour les autres URLs
            try:
                response = requests.get(
                    attempt_url, headers=headers, stream=True, timeout=30
                )
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "").lower()

                if "application/pdf" in content_type:
                    return self._download_pdf(response, attempt_url, doi, pdf_folder)
                else:
                    logger.info(
                        f"The URL {attempt_url} does not point to a PDF file. Content-Type: {content_type}"
                    )
                    return None, None
            except requests.RequestException as e:
                logger.error(
                    f"Error checking/downloading PDF from {attempt_url}: {str(e)}"
                )
                return None, None

        # First attempt with the original URL
        result = try_download(url)
        if result[0] is not None:
            return result

        # If the first attempt failed and the URL doesn't end with .pdf, try appending .pdf
        if not url.lower().endswith(".pdf") and "doi.org" not in url.lower():
            pdf_url = urljoin(url, url.split("/")[-1] + ".pdf")
            logger.info(f"Attempting to download PDF from modified URL: {pdf_url}")
            result = try_download(pdf_url)
            if result[0] is not None:
                return result

        # If both attempts failed, return None, None
        return None, None

    def _download_pdf(
        self, response: requests.Response, url: str, doi: str, pdf_folder: str
    ) -> Tuple[str, str]:
        filename = f"{doi.replace('/', '_')}.pdf"
        file_path = os.path.join(pdf_folder, filename)
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logger.info(f"PDF successfully downloaded and saved to {file_path}")
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

        # Créer le dossier s'il n'existe pas
        if not os.path.exists(pdf_folder):
            os.makedirs(pdf_folder)

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
        Retrieve PDF links for a given DOI using the Crossref API.

        Args:
            doi (str): The Digital Object Identifier of the publication.

        Returns:
            List[str]: A list of URLs that might contain PDFs.
        """
        if not isinstance(doi, str) or not doi.strip():
            raise ValueError("DOI must be a non-empty string")

        base_url = "https://api.crossref.org/works/"
        url = f"{base_url}{doi}?mailto={email}"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "message" in data and "link" in data["message"]:
                links = [link["URL"] for link in data["message"]["link"]]
                logger.info(
                    f"Crossref links found for DOI {doi}: {links}"
                )  # Log les liens trouvés
                return links
            else:
                logger.info(
                    f"No links found in Crossref for DOI: {doi}"
                )  # Log si aucun lien n'est trouvé
                return []

        except requests.RequestException as e:
            logger.error(
                f"Error fetching Crossref data for DOI {doi}: {str(e)}"
            )  # Log en cas d'erreur de requête
            return []
        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"Error parsing Crossref response for DOI {doi}: {str(e)}"
            )  # Log des erreurs de parsing
            return []

UnpaywallClient = Client(
    response_handler=JsonResponseHandler,
)
