"""Fetch EU case-law metadata via the official CELLAR SPARQL endpoint and ingest
it into ``eu_hierarchical.db``.

Only public information is inserted – no synthetic fields are generated.
If a particular datum (e.g. summary) is not present in the API response, we
store an empty string so that downstream code can still rely on the column.

Usage (example):

    python -m eu_link_db.ingest_cases_from_api ECLI:EU:C:2021:504
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests
from sqlmodel import Session

from .models_hierarchical import Caselaw, get_session

SPARQL_ENDPOINT = "https://op.europa.eu/sparql"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
import xml.etree.ElementTree as ET

NS = {
    "cdm": "http://publications.europa.eu/ontology/cdm#",
    "dct": "http://purl.org/dc/terms/",
}


def extract_celex_from_ecli(ecli: str) -> Optional[str]:
    """Convert an ECLI identifier to a CELEX ID (best-effort)."""
    import re

    match = re.match(r"ECLI:EU:C:(\d{4}):(\d+)", ecli)
    if not match:
        return None
    year = match.group(1)
    number = match.group(2).zfill(3)
    return f"6{year}CJ{number}"


def parse_case_rdf(ecli: str, xml_bytes: bytes) -> Optional[Dict]:
    """Extract metadata from RDF/XML response."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        print(f"    XML parse error for {ecli}: {exc}")
        return None

    # Title
    title = ""
    title_elem = root.find('.//cdm:resource_legal_title', NS)
    if title_elem is not None and title_elem.text:
        title = title_elem.text

    # Decision date
    decision_date = None
    date_elem = root.find('.//cdm:work_date_document', NS)
    if date_elem is not None and date_elem.text:
        try:
            decision_date = datetime.fromisoformat(date_elem.text)
        except ValueError:
            pass

    # Court label (english)
    court = ""
    for t in root.findall('.//dct:title', NS):
        lang = t.attrib.get('{http://www.w3.org/XML/1998/namespace}lang', '')
        if lang.lower() == 'en':
            court = t.text or ""
            break

    return {
        "ecli": ecli,
        "court": court,
        "decision_date": decision_date or datetime(1900, 1, 1),
        "title": title,
        "summary_text": "",  # not provided
        "source_url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{extract_celex_from_ecli(ecli) or ''}",
    }

def fetch_case_metadata(ecli: str) -> Optional[Dict]:
    """Fetch metadata via REST RDF/XML first, fallback to SPARQL JSON."""

    # 1. REST RDF/XML
    url = f"https://publications.europa.eu/resource/ecli/{ecli}?format=application/rdf+xml"
    headers = {"Accept": "application/rdf+xml", "User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            return parse_case_rdf(ecli, r.content)
        print(f"    REST returned {r.status_code} for {ecli}")
    except Exception as exc:
        print(f"    REST error for {ecli}: {exc}")

    # 2. SPARQL JSON
    query = f"""
    PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
    PREFIX dct: <http://purl.org/dc/terms/>
    SELECT ?title ?date ?courtLabel WHERE {{
        ?work cdm:case-law_ecli "{ecli}" .
        OPTIONAL {{ ?work cdm:resource_legal_title ?title . }}
        OPTIONAL {{ ?work cdm:work_date_document ?date . }}
        OPTIONAL {{ ?work cdm:case-law_court ?court . ?court dct:title ?courtLabel . FILTER(lang(?courtLabel)='en') }}
    }} LIMIT 1"""

    sparql_headers = {"Accept": "application/sparql-results+json"}
    try:
        from urllib.parse import quote
        endpoint = "https://publications.europa.eu/webapi/rdf/sparql"
        encoded_query = query
        resp = requests.get(endpoint, params={"query": encoded_query, "format": "application/sparql-results+json"}, headers=sparql_headers, timeout=30)
        if resp.status_code == 200:
            data = json.loads(resp.text)
            return parse_sparql_json(ecli, data)
        print(f"    SPARQL returned {resp.status_code} for {ecli}")
    except Exception as exc:
        print(f"    SPARQL error for {ecli}: {exc}")
    return None


def parse_sparql_json(ecli: str, data: Dict) -> Optional[Dict]:
    """Convert SPARQL JSON results to case_data dict."""
    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return None
    b = bindings[0]
    title = b.get("title", {}).get("value", "")
    date_raw = b.get("date", {}).get("value")
    court = b.get("courtLabel", {}).get("value", "")
    decision_date = None
    if date_raw:
        try:
            decision_date = datetime.fromisoformat(date_raw)
        except ValueError:
            pass
    return {
        "ecli": ecli,
        "court": court,
        "decision_date": decision_date or datetime(1900, 1, 1),
        "title": title,
        "summary_text": "",
        "source_url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{extract_celex_from_ecli(ecli) or ''}",
    }
    """Run a SPARQL query to fetch metadata for *one* case.

    CELLAR provides a public SPARQL endpoint. We query minimal fields: title
    (work title) and decision date. The query is narrow to keep it cheap.
    """

    # Try CELLAR REST API first (RDF/XML)
    url = f"https://publications.europa.eu/resource/ecli/{ecli}?format=application/rdf+xml"
    headers = {"Accept": "application/rdf+xml", "User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.content
        print(f"    REST fetch returned {resp.status_code}, falling back to SPARQL…")
    except Exception as exc:
        print(f"    REST fetch error: {exc}; falling back to SPARQL…")

    # Fallback: SPARQL endpoint (may be blocked in some nets)
    query = f"""
    PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?title ?date ?courtLabel
    WHERE {{
        ?work cdm:case-law_ecli "{ecli}" .
        OPTIONAL {{ ?work cdm:resource_legal_title ?title . }}
        OPTIONAL {{ ?work cdm:work_date_document ?date . }}
        OPTIONAL {{ ?work cdm:case-law_court ?court .
                   ?court dct:title ?courtLabel . FILTER(lang(?courtLabel) = 'en') }}
    }}
    LIMIT 1
    """

    headers = {"Accept": "application/sparql-results+json"}
    response = requests.post(
        SPARQL_ENDPOINT,
        data={"query": query},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    if response.status_code == 200:
        return response.content
    return None


def parse_sparql_result(ecli: str, data: Dict) -> Optional[Dict]:
    """Convert SPARQL JSON into the dict expected by ``Caselaw``."""

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return None  # Not found

    b = bindings[0]
    title = b.get("title", {}).get("value", "")
    date_raw = b.get("date", {}).get("value")
    court = b.get("courtLabel", {}).get("value", "")

    decision_date = None
    if date_raw:
        try:
            decision_date = datetime.fromisoformat(date_raw)
        except ValueError:
            pass

    return {
        "ecli": ecli,
        "court": court or "",  # empty string if missing
        "decision_date": decision_date or datetime(1900, 1, 1),  # fallback
        "title": title or "",  # keep empty if not available
        "summary_text": "",  # summary not provided by API
        "source_url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{extract_celex_from_ecli(ecli) or ''}",
    }

# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def ingest_cases(ecli_list: List[str], db_url: str = "sqlite:///eu_hierarchical.db") -> int:
    """Fetch each ECLI via API and insert into DB, skipping duplicates."""
    session: Session = get_session(db_url)
    inserted = 0

    try:
        for ecli in ecli_list:
            print(f"Fetching {ecli} …")
            case_data = fetch_case_metadata(ecli)
            if not case_data:
                print(f"  ⚠️  No data for {ecli}, skipping.")
                continue

            # Duplicate check
            if session.get(Caselaw, case_data["ecli"]):
                print(f"  ℹ️  {ecli} already in DB, skipping.")
                continue

            session.add(Caselaw(**case_data))
            inserted += 1

            # Commit right away to make sure data persists even if later entries fail
            session.commit()
            time.sleep(0.2)  # polite pause

        print(f"✅ Inserted {inserted} new cases.")
        return inserted
    finally:
        session.close()

# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m eu_link_db.ingest_cases_from_api <ECLI1> [ECLI2 ...]")
        sys.exit(1)

    eclis = sys.argv[1:]
    ingest_cases(eclis)
