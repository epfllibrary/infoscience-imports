"""
EPO OPS (Espacenet) client for Infoscience imports
Wrapper around python-epo-ops-client (epo_ops).
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Set, Union, Callable

import tenacity
from dotenv import load_dotenv

import epo_ops
from epo_ops.models import Epodoc, Docdb

from lxml import etree

from config import logs_dir
from utils import manage_logger


# ----------------------------
# Env
# ----------------------------
load_dotenv(os.path.join(os.getcwd(), ".env"))

EPO_OPS_KEY = os.environ.get("EPO_OPS_KEY")
EPO_OPS_SECRET = os.environ.get("EPO_OPS_SECRET")

DEFAULT_ACRONYMS: Set[str] = {
    "epfl",
    "ai",
    "dna",
    "eu",
    "cnrs",
    "mit",
}

_WORD_RE = re.compile(r"\b\w+\b", re.UNICODE)
_WS_RE = re.compile(r"\s+")

IdDict = Dict[str, str]
IdLike = Union[str, IdDict]  # accept epodoc string OR dict
IdOut = IdDict  # fetch_ids returns dicts only

def normalize_pub_number(value: str) -> str:
    if not isinstance(value, str):
        return ""
    return _WS_RE.sub("", value.strip()).upper()

def _is_retryable_exception(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int) and (status == 429 or 500 <= status <= 599):
        return True
    msg = str(exc).lower()
    transient = (
        "timeout",
        "timed out",
        "connection reset",
        "temporary failure",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
        "too many requests",
    )
    return any(t in msg for t in transient)


retry_decorator = tenacity.retry(
    retry=tenacity.retry_if_exception(_is_retryable_exception),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30)
    + tenacity.wait_random(0, 1),
    stop=tenacity.stop_after_attempt(6),
    reraise=True,
)

def smart_title_capitalize(text: str, acronyms: Optional[Set[str]] = None) -> str:
    """
    Title-capitalize each word while preserving whitelisted acronyms,
    including when surrounded by punctuation.
    """
    if not isinstance(text, str):
        return ""
    s = text.strip()
    if not s:
        return ""
    acronyms = acronyms if acronyms is not None else DEFAULT_ACRONYMS

    def transform(match: re.Match) -> str:
        w = match.group(0)
        lw = w.lower()
        if lw in acronyms:
            return w.upper()
        return lw.capitalize()

    return _WORD_RE.sub(transform, s)


def smart_lower_capitalize(text: str, acronyms: Optional[Set[str]] = None) -> str:
    """
    Lowercase full text, capitalize first alphabetic character,
    preserve whitelisted acronyms (even with punctuation).
    """
    if not isinstance(text, str):
        return ""
    s = text.strip()
    if not s:
        return ""
    acronyms = acronyms if acronyms is not None else DEFAULT_ACRONYMS

    s = s.lower()

    def transform(match: re.Match) -> str:
        w = match.group(0)
        if w.lower() in acronyms:
            return w.upper()
        return w

    s = _WORD_RE.sub(transform, s)

    for i, ch in enumerate(s):
        if ch.isalpha():
            s = s[:i] + ch.upper() + s[i + 1 :]
            break
    return s


class EPOClient:
    """Client for EPO OPS API."""
    log_file_path = os.path.join(logs_dir, "logging.log")
    logger = manage_logger(log_file_path)

    # IMPORTANT: default namespace is the EPO exchange namespace
    NS = {
        "ops": "http://ops.epo.org",
        "ex": "http://www.epo.org/exchange",
        "xlink": "http://www.w3.org/1999/xlink",
    }

    def __init__(self):
        if not EPO_OPS_KEY or not EPO_OPS_SECRET:
            raise RuntimeError(
                "Missing EPO OPS credentials: set EPO_OPS_KEY and EPO_OPS_SECRET"
            )

        self.ops = epo_ops.Client(key=EPO_OPS_KEY, secret=EPO_OPS_SECRET)
        self.last_response: Any = None

    # ----------------------------
    # Search / Count / IDs
    # ----------------------------
    @retry_decorator
    def search_query(
        self,
        cql: str,
        range_begin: int = 1,
        range_end: int = 25,
        constituents: Optional[List[str]] = None,
    ) -> etree._Element:
        """
        Executes a CQL search and returns the XML root element of the search response.
        """
        resp = self._ops_search(cql=cql, range_begin=range_begin, range_end=range_end, constituents=constituents)
        self.last_response = resp
        return self._ensure_xml_root(resp)

    @retry_decorator
    def count_results(self, cql: str) -> int:
        root = self.search_query(cql=cql, range_begin=1, range_end=1)

        node = self._first(root, ".//ops:biblio-search")
        if node is not None:
            val = node.get("total-result-count")
            try:
                return int(val) if val else 0
            except Exception:
                return 0
        return 0

    @retry_decorator
    def fetch_ids(
        self,
        cql: str,
        per_page: int = 25,
        max_records: Optional[int] = None,
        range_begin: int = 1,
        constituents: Optional[List[str]] = None,
    ) -> List[IdOut]:
        out: List[IdOut] = []
        seen: Set[str] = set()
        rb = max(1, int(range_begin))

        while True:
            re_ = rb + int(per_page) - 1
            root = self.search_query(
                cql=cql,
                range_begin=rb,
                range_end=re_,
                constituents=constituents,
            )

            page_ids: List[IdOut] = self._extract_pub_ids_from_search(root)
            if not page_ids:
                break

            added = 0

            for d in page_ids:
                c = d.get("docdb_country", "")
                n = d.get("docdb_number", "")
                k = d.get("docdb_kind", "")
                e = d.get("epodoc", "")

                key = f"{c}{n}{k}" if (c and n and k) else (f"{c}{n}" if (c and n) else e)
                key = normalize_pub_number(key)

                if not key or key in seen:
                    continue

                seen.add(key)
                out.append(d)
                added += 1

                if max_records and len(out) >= max_records:
                    return out[:max_records]

            if added == 0:
                break

            rb += per_page

        return out

    @retry_decorator
    def fetch_records(
        self,
        cql: str,
        format: str = "digest",
        per_page: int = 25,
        max_records: Optional[int] = None,
        range_begin: int = 1,
        constituents: Optional[List[str]] = None,
        group_by_family: bool = False,
    ) -> List[Dict[str, Any]]:
        ids: List[IdOut] = self.fetch_ids(
            cql=cql,
            per_page=per_page,
            max_records=max_records,
            range_begin=range_begin,
            constituents=constituents,
        )
        recs = self.fetch_records_by_ids(ids, format=format)
        return self._aggregate_by_family(recs) if group_by_family else recs

    def fetch_records_by_ids(
        self, pub_ids: List[IdLike], format: str = "digest"
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        for pid in pub_ids:
            if isinstance(pid, dict):
                label = (
                    pid.get("epodoc")
                    or (
                        pid.get("docdb_country", "")
                        + pid.get("docdb_number", "")
                        + pid.get("docdb_kind", "")
                    )
                    or (pid.get("docdb_country", "") + pid.get("docdb_number", ""))
                )
            else:
                label = str(pid)

            self.logger.info(f"[OPS] Fetching record for ID: {label}")

            rec = self.fetch_record_by_unique_id(pid, format=format)
            if rec:
                out.append(rec)

        return out

    # ----------------------------
    # Fetch single record
    # ----------------------------
    @retry_decorator
    def fetch_record_by_unique_id(self, pub_id: IdLike, format: str = "digest") -> Optional[Dict[str, Any]]:
        if pub_id is None:
            return None

        epodoc_num = ""
        docdb_country = ""
        docdb_number = ""
        docdb_kind = ""

        if isinstance(pub_id, str):
            epodoc_num = normalize_pub_number(pub_id)
        elif isinstance(pub_id, dict):
            epodoc_num = normalize_pub_number(pub_id.get("epodoc", "") or "")
            docdb_country = normalize_pub_number(pub_id.get("docdb_country", "") or "")
            docdb_number = normalize_pub_number(pub_id.get("docdb_number", "") or "")
            docdb_kind = normalize_pub_number(pub_id.get("docdb_kind", "") or "")
        else:
            epodoc_num = normalize_pub_number(str(pub_id))

        # Defensive: epodoc must NOT include kind suffix like A1
        # If someone passed US20260026733A1, strip trailing kind (1-2 letters + 1 digit) cautiously.
        epodoc_num = re.sub(r"([A-Z]{1,2}\d)$", "", epodoc_num) if epodoc_num else ""

        if not epodoc_num and not (docdb_country and docdb_number):
            return None

        attempts: List[tuple[str, Any, str]] = []

        # 1) Prefer epodoc next (constructed from docdb or returned)
        if epodoc_num:
            attempts.append(("epodoc", Epodoc(epodoc_num), epodoc_num))

        # 2) Then try full docdb (country+number+kind) if we have all components
        if docdb_country and docdb_number and docdb_kind:
            attempts.append(
                (
                    "docdb_full",
                    Docdb(docdb_country, docdb_number, docdb_kind),
                    f"{docdb_country}{docdb_number}{docdb_kind}",
                )
            )

        last_err: Optional[Exception] = None

        for label, ops_input, shown_id in attempts:
            try:
                resp = self.ops.published_data(
                    reference_type="publication",
                    input=ops_input,
                    endpoint="biblio",
                )
                self.last_response = resp

                root = self._ensure_xml_root(resp)

                if not root.xpath(".//ex:exchange-document", namespaces=self.NS):
                    self.logger.info(f"[OPS] {label} returned no exchange-document id={shown_id}")
                    continue

                internal_id = epodoc_num or shown_id
                return self._process_record(root, internal_id, format=format)

            except Exception as e:
                last_err = e
                self.logger.info(f"[OPS] {label} exception id={shown_id}: {e}")
                continue

        if last_err:
            self.logger.warning(f"[OPS] No published record found for pub_id={pub_id} (last error: {last_err})")
        return None

    # ----------------------------
    # Processing
    # ----------------------------
    def _process_record(
        self, root: etree._Element, pub_id: str, format: str
    ) -> Dict[str, Any]:
        if format == "ifs3":
            return self._extract_ifs3_record_info(root, pub_id)
        return self._extract_digest_record_info(root, pub_id)

    # ----------------------------
    # Extractors (DIGEST)
    # ----------------------------
    def _extract_digest_record_info(
        self, root: etree._Element, pub_id: str
    ) -> Dict[str, Any]:
        exdoc = self._first(root, ".//ex:exchange-document")
        family_id = exdoc.get("family-id", "") if exdoc is not None else ""

        # Publication reference (DocDB + Epodoc)
        docdb_country = self._text(
            root,
            ".//ex:publication-reference/ex:document-id[@document-id-type='docdb']/ex:country",
        )
        docdb_number = self._text(
            root,
            ".//ex:publication-reference/ex:document-id[@document-id-type='docdb']/ex:doc-number",
        )
        docdb_kind = self._text(
            root,
            ".//ex:publication-reference/ex:document-id[@document-id-type='docdb']/ex:kind",
        )
        pub_date = self._text(
            root,
            ".//ex:publication-reference/ex:document-id[@document-id-type='docdb']/ex:date",
        )

        epodoc_number = self._text(
            root,
            ".//ex:publication-reference/ex:document-id[@document-id-type='epodoc']/ex:doc-number",
        )

        # Titles (multi-lang) -> Title Capitalize
        titles = self._collect_lang_map(
            root,
            ".//ex:invention-title",
            join_paragraphs=False,
            normalizer=smart_lower_capitalize,
        )

        # Abstracts (multi-lang) -> Lower + Capitalize
        abstracts = self._collect_lang_map(
            root,
            ".//ex:abstract",
            join_paragraphs=True,
            normalizer=smart_lower_capitalize,
        )

        preferred_langs = ["en", "fr", "de", "it"]
        title = self._pick_preferred_lang(titles, preferred_langs)          # déjà normalisé title-case
        abstract = self._pick_preferred_lang(abstracts, preferred_langs)    # déjà normalisé lower-cap

        # Applicants / Inventors (keep as-is except a mild lower-capitalize on applicants label if you want)
        applicants_original = self._texts_join(
            root,
            ".//ex:parties/ex:applicants/ex:applicant[@data-format='original']//ex:name",
        )
        applicants_epodoc = self._texts_join(
            root,
            ".//ex:parties/ex:applicants/ex:applicant[@data-format='epodoc']//ex:name",
        )
        applicants = applicants_original or applicants_epodoc

        inventors_original = self._texts_join(
            root,
            ".//ex:parties/ex:inventors/ex:inventor[@data-format='original']//ex:name",
        )
        inventors_epodoc = self._texts_join(
            root,
            ".//ex:parties/ex:inventors/ex:inventor[@data-format='epodoc']//ex:name",
        )
        inventors = inventors_original or inventors_epodoc

        # IPC (IPCR free text)
        # ipcr = self._texts_join(
        #     root,
        #     ".//ex:classifications-ipcr/ex:classification-ipcr/ex:text",
        #     cleaner=self._clean_classif,
        # )

        # CPC (CPCI)
        # cpc = self._collect_cpc_codes(root)

        # Application reference
        app_docdb_number = self._text(
            root,
            ".//ex:application-reference/ex:document-id[@document-id-type='docdb']/ex:doc-number",
        )
        app_docdb_country = self._text(
            root,
            ".//ex:application-reference/ex:document-id[@document-id-type='docdb']/ex:country",
        )
        app_docdb_kind = self._text(
            root,
            ".//ex:application-reference/ex:document-id[@document-id-type='docdb']/ex:kind",
        )
        app_epodoc_number = self._text(
            root,
            ".//ex:application-reference/ex:document-id[@document-id-type='epodoc']/ex:doc-number",
        )
        app_date = self._text(
            root,
            ".//ex:application-reference/ex:document-id[@document-id-type='epodoc']/ex:date",
        )
        app_original = self._text(
            root,
            ".//ex:application-reference/ex:document-id[@document-id-type='original']/ex:doc-number",
        )

        priority_pairs = self._extract_priority_pairs(root)

        # Citations
        cited_count = self._count(root, ".//ex:references-cited/ex:citation")
        cited_epodoc = self._texts_join(
            root,
            ".//ex:references-cited//ex:document-id[@document-id-type='epodoc']/ex:doc-number",
        )

        return {
            "source": "epo",
            "internal_id": pub_id,
            "docdb_country": docdb_country,
            "docdb_number": docdb_number,
            "docdb_kind": docdb_kind,
            "epodoc_number": epodoc_number or pub_id,
            "family_id": family_id,
            "issueDate": pub_date,
            "pubyear": pub_date[:4] if pub_date else "",
            "title": title,
            "title_en": titles.get("en", ""),
            "title_fr": titles.get("fr", ""),
            "title_de": titles.get("de", ""),
            "title_it": titles.get("it", ""),
            "title_und": titles.get("und", ""),
            "abstract": abstract,
            "abstract_en": abstracts.get("en", ""),
            "abstract_fr": abstracts.get("fr", ""),
            "abstract_de": abstracts.get("de", ""),
            "abstract_it": abstracts.get("it", ""),
            "abstract_und": abstracts.get("und", ""),
            "doctype": "Patent",
            "applicants": smart_title_capitalize(applicants),
            "inventors": inventors,
            # "ipc_ipcr": ipcr,
            # "cpc_codes": cpc,
            "application_docdb": " ".join(
                [x for x in [app_docdb_country, app_docdb_number, app_docdb_kind] if x]
            ).strip(),
            "application_epodoc": app_epodoc_number,
            "application_date": app_date,
            "application_original": app_original,
            "priority": priority_pairs,
            "references_cited_count": str(cited_count),
            "references_cited_epodoc": cited_epodoc,
        }

    # ----------------------------
    # Extractors (IFS3) – richer output
    # ----------------------------
    def _extract_ifs3_record_info(
        self, root: etree._Element, pub_id: str
    ) -> Dict[str, Any]:
        d = self._extract_digest_record_info(root, pub_id)
        # d["cpc_structured"] = self._collect_cpc_structured(root)
        return d

    # ----------------------------
    # OPS search adapter (version-tolerant)
    # ----------------------------
    def _ops_search(
        self,
        cql: str,
        range_begin: int,
        range_end: int,
        constituents: Optional[List[str]] = None,
    ) -> Any:
        """
        Version compatible avec python-epo-ops-client:
        published_data_search(cql, range_begin, range_end, constituents)
        """
        if hasattr(self.ops, "published_data_search"):
            return self.ops.published_data_search(
                cql=cql,
                range_begin=range_begin,
                range_end=range_end,
                constituents=constituents,
            )

        # fallback éventuel si une autre version expose "search"
        if hasattr(self.ops, "search"):
            return self.ops.search(cql=cql, range_begin=range_begin, range_end=range_end)

        raise AttributeError(
            "No OPS search method found on epo_ops.Client. "
            "Expected 'published_data_search' (with range_begin/range_end)."
        )

    def _extract_pub_ids_from_search(self, root: etree._Element) -> List[IdOut]:
        """
        Extract identifier dicts from OPS search responses.

        In 'light' search results, epodoc doc-number is often absent.
        We reconstruct an epodoc-like identifier as COUNTRY+DOCNUMBER (without kind),
        which is generally accepted by OPS as an Epodoc publication reference.

        Returns dicts:
        {
            "family_id": "...",
            "docdb_country": "US",
            "docdb_number": "20260026733",
            "docdb_kind": "A1",
            "epodoc": "US20260026733"
        }
        """
        out: List[IdOut] = []
        seen: Set[str] = set()

        pubs = root.xpath(".//ops:publication-reference", namespaces=self.NS)

        for pr in pubs:
            family_id = (pr.get("family-id") or "").strip()

            country = pr.xpath(
                "string(./document-id[@document-id-type='docdb']/country | "
                "./ex:document-id[@document-id-type='docdb']/ex:country)",
                namespaces=self.NS,
            ).strip()

            number = pr.xpath(
                "string(./document-id[@document-id-type='docdb']/doc-number | "
                "./ex:document-id[@document-id-type='docdb']/ex:doc-number)",
                namespaces=self.NS,
            ).strip()

            kind = pr.xpath(
                "string(./document-id[@document-id-type='docdb']/kind | "
                "./ex:document-id[@document-id-type='docdb']/ex:kind)",
                namespaces=self.NS,
            ).strip()

            # epodoc may be absent; compute fallback from docdb
            epodoc = pr.xpath(
                "string(./document-id[@document-id-type='epodoc']/doc-number | "
                "./ex:document-id[@document-id-type='epodoc']/ex:doc-number)",
                namespaces=self.NS,
            ).strip()

            c = normalize_pub_number(country)
            n = normalize_pub_number(number)
            k = normalize_pub_number(kind)
            e = normalize_pub_number(epodoc) or (f"{c}{n}" if c and n else "")

            d: IdOut = {
                "family_id": family_id,
                "docdb_country": c,
                "docdb_number": n,
                "docdb_kind": k,
                "epodoc": e,
            }

            # dedup key: docdb_full > docdb_no_kind > epodoc
            key = ""
            if c and n and k:
                key = f"{c}{n}{k}"
            elif c and n:
                key = f"{c}{n}"
            elif e:
                key = e

            if not key or key in seen:
                continue

            seen.add(key)
            out.append(d)

        return out

    def _extract_priority_pairs(self, root: etree._Element, sep: str = "||") -> str:
        """
        Extract priority claims as 'PRIORITY_NUMBER:PRIORITY_DATE' pairs, joined by '||'.
        Uses epodoc doc-number + date within the same priority-claim node.
        Example: EP20230194124:20230829||...
        """
        pairs: List[str] = []
        seen: Set[str] = set()

        claims = root.xpath(".//ex:priority-claims/ex:priority-claim", namespaces=self.NS)
        for cl in claims:
            if not isinstance(cl, etree._Element):
                continue

            num = cl.xpath(
                "string(./ex:document-id[@document-id-type='epodoc']/ex:doc-number)",
                namespaces=self.NS,
            ).strip()
            dt = cl.xpath(
                "string(./ex:document-id[@document-id-type='epodoc']/ex:date)",
                namespaces=self.NS,
            ).strip()

            if not num:
                continue
            pair = f"{num}::{dt}" if dt else f"{num}::"
            if pair not in seen:
                seen.add(pair)
                pairs.append(pair)

        return sep.join(pairs)

    def _aggregate_by_family(self, recs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Group records by family_id.

        Multi-fields aggregated (sorted by date desc):
        - application_number::application_date
        - priority_number::priority_date  (your current field is 'priority' with '::')
        - epodoc_number::docdb_country::docdb_kind::issueDate

        Dates are formatted YYYY-MM-DD.
        For other metadata: keep value from the most recent member (by issueDate).
        """
        if not recs:
            return []

        # --- group by family_id (fallback if missing)
        families: Dict[str, List[Dict[str, Any]]] = {}
        for r in recs:
            fid = (
                r.get("family_id") or ""
            ).strip() or f"NOFAMILY::{r.get('internal_id','')}"
            families.setdefault(fid, []).append(r)

        out: List[Dict[str, Any]] = []

        for fid, members in families.items():
            # sort members by issueDate desc (YYYYMMDD) — empty dates go last
            members_sorted = sorted(
                members,
                key=lambda x: (x.get("issueDate") or ""),
                reverse=True,
            )
            latest = members_sorted[0]

            # --- build publication variants: epodoc::country::kind::date
            pub_variants: List[tuple[str, str]] = []  # (date_yyyymmdd, string)
            seen_pub: Set[str] = set()
            for m in members_sorted:
                ep = (m.get("epodoc_number") or "").strip()
                cc = (m.get("docdb_country") or "").strip()
                kd = (m.get("docdb_kind") or "").strip()
                dt = (m.get("issueDate") or "").strip()  # YYYYMMDD
                if not (ep or cc or kd or dt):
                    continue
                dt_fmt = self._fmt_yyyymmdd(dt)
                s = f"{ep}::{cc}::{kd}::{dt_fmt}"
                if s not in seen_pub:
                    seen_pub.add(s)
                    pub_variants.append((dt, s))

            pub_variants.sort(key=lambda t: t[0] or "", reverse=True)
            publications = "||".join([s for _, s in pub_variants])

            # --- application_number::application_date (prefer epodoc application number + application_date)
            app_pairs: List[tuple[str, str]] = []
            seen_app: Set[str] = set()
            for m in members_sorted:
                app_num = (m.get("application_epodoc") or "").strip()
                app_dt = self._fmt_yyyymmdd((m.get("application_date") or "").strip())
                if not app_num:
                    continue
                s = f"{app_num}::{app_dt}"
                if s not in seen_app:
                    seen_app.add(s)
                    # sort key uses unformatted YYYY-MM-DD -> comparable lexicographically
                    app_pairs.append((app_dt or "0000-00-00", s))

            app_pairs.sort(key=lambda t: t[0], reverse=True)
            applications = "||".join([s for _, s in app_pairs])

            # --- priority pairs (your field name is 'priority', with '::' and dates in YYYYMMDD)
            # normalize and sort by date desc
            pr_pairs_raw: List[tuple[str, str]] = []
            seen_pr: Set[str] = set()

            for m in members_sorted:
                for num, dt in self._split_pairs(
                    m.get("priority", ""), pair_sep="||", kv_sep="::"
                ):
                    num = num.strip()
                    dt_fmt = self._fmt_yyyymmdd(dt.strip())
                    if not num:
                        continue
                    s = f"{num}::{dt_fmt}"
                    if s in seen_pr:
                        continue
                    seen_pr.add(s)
                    pr_pairs_raw.append((dt_fmt or "0000-00-00", s))

            pr_pairs_raw.sort(key=lambda t: t[0], reverse=True)
            priorities = "||".join([s for _, s in pr_pairs_raw])

            # --- Build aggregated record: keep latest's metadata + overwrite aggregated fields
            agg = dict(latest)  # copy

            # normalize issueDate to YYYY-MM-DD on the aggregated record
            agg["issueDate"] = self._fmt_yyyymmdd((latest.get("issueDate") or "").strip())

            # also normalize application_date on the latest copy (optional but consistent)
            if "application_date" in agg:
                agg["application_date"] = self._fmt_yyyymmdd(
                    (agg.get("application_date") or "").strip()
                )

            # replace internal_id: the family record becomes the family itself (or keep latest internal_id)
            # choose one (I keep latest internal_id + add family_internal_id)
            agg["family_internal_id"] = fid

            # aggregated multi fields
            agg["publications"] = publications
            agg["applications"] = applications
            agg["priority"] = priorities  # keep same key name for pipeline compatibility

            # optionally: keep list of member ids
            agg["members_internal_ids"] = "||".join(
                [m.get("internal_id", "") for m in members_sorted if m.get("internal_id")]
            )

            out.append(agg)

        # sort families by most recent issueDate (already YYYY-MM-DD)
        out.sort(key=lambda r: r.get("issueDate") or "", reverse=True)
        return out

    # ----------------------------
    # XML utilities
    # ----------------------------
    def _ensure_xml_root(self, resp: Any) -> etree._Element:
        if isinstance(resp, etree._Element):
            return resp
        if hasattr(resp, "content"):
            return etree.fromstring(resp.content)
        if isinstance(resp, (bytes, bytearray)):
            return etree.fromstring(bytes(resp))
        if isinstance(resp, str):
            return etree.fromstring(resp.encode("utf-8", errors="replace"))
        return etree.fromstring(str(resp).encode("utf-8", errors="replace"))

    def _first(self, root: etree._Element, xpath: str) -> Optional[etree._Element]:
        nodes = root.xpath(xpath, namespaces=self.NS)
        return nodes[0] if nodes else None

    def _text(self, root: etree._Element, xpath: str) -> str:
        nodes = root.xpath(xpath, namespaces=self.NS)
        if not nodes:
            return ""
        n = nodes[0]
        if isinstance(n, etree._Element):
            return (n.text or "").strip()
        return str(n).strip()

    def _texts_join(
        self, root: etree._Element, xpath: str, sep: str = "||", cleaner=None
    ) -> str:
        nodes = root.xpath(xpath, namespaces=self.NS)
        vals: List[str] = []
        for n in nodes:
            v = (
                (n.text or "").strip()
                if isinstance(n, etree._Element)
                else str(n).strip()
            )
            if not v:
                continue
            if cleaner:
                v = cleaner(v)
                if not v:
                    continue
            vals.append(v)

        seen = set()
        out = []
        for v in vals:
            if v in seen:
                continue
            seen.add(v)
            out.append(v)
        return sep.join(out)

    def _count(self, root: etree._Element, xpath: str) -> int:
        return len(root.xpath(xpath, namespaces=self.NS))

    @staticmethod
    def _clean_classif(s: str) -> str:
        s = s.replace("\u2002", " ").replace("\u2003", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # def _collect_cpc_codes(self, root: etree._Element) -> str:
    #     pcs = root.xpath(
    #         ".//ex:patent-classifications/ex:patent-classification[ex:classification-scheme/@scheme='CPCI']",
    #         namespaces=self.NS,
    #     )
    #     codes: List[str] = []
    #     for pc in pcs:
    #         sec = self._child_text(pc, "ex:section")
    #         cls = self._child_text(pc, "ex:class")
    #         sub = self._child_text(pc, "ex:subclass")
    #         mg = self._child_text(pc, "ex:main-group")
    #         sg = self._child_text(pc, "ex:subgroup")
    #         if not (sec and cls and sub and mg and sg):
    #             continue
    #         codes.append(f"{sec}{cls}{sub}{mg}/{sg}")

    #     seen = set()
    #     out = []
    #     for c in codes:
    #         if c in seen:
    #             continue
    #         seen.add(c)
    #         out.append(c)
    #     return "||".join(out)

    # def _collect_cpc_structured(self, root: etree._Element) -> str:
    #     pcs = root.xpath(
    #         ".//ex:patent-classifications/ex:patent-classification[ex:classification-scheme/@scheme='CPCI']",
    #         namespaces=self.NS,
    #     )
    #     out: List[str] = []
    #     for pc in pcs:
    #         sec = self._child_text(pc, "ex:section")
    #         cls = self._child_text(pc, "ex:class")
    #         sub = self._child_text(pc, "ex:subclass")
    #         mg = self._child_text(pc, "ex:main-group")
    #         sg = self._child_text(pc, "ex:subgroup")
    #         val = self._child_text(pc, "ex:classification-value")
    #         if not (sec and cls and sub and mg and sg):
    #             continue
    #         code = f"{sec}{cls}{sub}{mg}/{sg}"
    #         out.append(f"{code}:{val}" if val else code)

    #     seen = set()
    #     uniq = []
    #     for v in out:
    #         if v in seen:
    #             continue
    #         seen.add(v)
    #         uniq.append(v)
    #     return "||".join(uniq)

    def _child_text(self, node: etree._Element, child_xpath: str) -> str:
        res = node.xpath(child_xpath, namespaces=self.NS)
        if not res:
            return ""
        el = res[0]
        return (
            (el.text or "").strip()
            if isinstance(el, etree._Element)
            else str(el).strip()
        )

    def _collect_lang_map(
        self,
        root: etree._Element,
        xpath: str,
        join_paragraphs: bool = True,
        normalizer: Optional[Callable[[str], str]] = None,
    ) -> Dict[str, str]:
        """
        Collect values per @lang for nodes matching xpath.
        Returns: {"en": "...", "fr": "...", ...}
        - If multiple nodes for same lang, concatenates (with space).
        - If join_paragraphs=True, joins <p> segments (for abstracts).
        - normalizer applied per lang value (e.g. title/abstract normalization).
        """
        nodes = root.xpath(xpath, namespaces=self.NS)
        tmp: Dict[str, List[str]] = {}

        for n in nodes:
            if not isinstance(n, etree._Element):
                continue
            lang = (n.get("lang") or "").strip().lower() or "und"

            if join_paragraphs and n.tag.endswith("abstract"):
                parts = []
                for p in n.xpath("./ex:p", namespaces=self.NS):
                    if isinstance(p, etree._Element) and p.text and p.text.strip():
                        parts.append(p.text.strip())
                text = " ".join(parts).strip()
            else:
                text = (n.text or "").strip()

            if not text:
                continue
            tmp.setdefault(lang, []).append(text)

        merged: Dict[str, str] = {}
        for lang, vals in tmp.items():
            seen = set()
            uniq = []
            for v in vals:
                if v in seen:
                    continue
                seen.add(v)
                uniq.append(v)
            merged_val = " ".join(uniq).strip()
            merged[lang] = normalizer(merged_val) if normalizer else merged_val

        return merged

    def _pick_preferred_lang(
        self, lang_map: Dict[str, str], preferred: List[str]
    ) -> str:
        for lang in preferred:
            v = lang_map.get(lang)
            if v:
                return v
        for v in lang_map.values():
            if v:
                return v
        return ""

    @staticmethod
    def _fmt_yyyymmdd(s: str) -> str:
        s = (s or "").strip()
        if len(s) != 8 or not s.isdigit():
            return ""
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

    @staticmethod
    def _split_pairs(
        value: str, pair_sep: str = "||", kv_sep: str = "::"
    ) -> List[tuple[str, str]]:
        value = (value or "").strip()
        if not value:
            return []
        out: List[tuple[str, str]] = []
        for part in value.split(pair_sep):
            part = part.strip()
            if not part:
                continue
            if kv_sep in part:
                k, v = part.split(kv_sep, 1)
                out.append((k.strip(), v.strip()))
            else:
                out.append((part.strip(), ""))
        return out
