"""
SEC EDGAR scraper for 13F-HR filings.

Respects SEC's published guidelines:
  - Custom User-Agent: "13F-Aggregator contact@example.com"
  - Max 10 requests/second (we stay well under with 0.15s delay)

Data flow:
  fetch_fund_filings()  →  fetch_filing_index()  →  fetch_holdings_xml()
  →  parse_info_table()  →  resolve_tickers()  →  upsert to DB
"""

import re
import time
import logging
import requests
import xml.etree.ElementTree as ET
from typing import Optional

from sqlalchemy.orm import Session

from database import Fund, Filing, Holding, SessionLocal, init_db

logger = logging.getLogger(__name__)

# ── SEC-required User-Agent ────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "13F-Aggregator contact@example.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov",
}
EDGAR_HEADERS = {**HEADERS, "Host": "www.sec.gov"}

SEC_DATA_BASE   = "https://data.sec.gov"
SEC_WWW_BASE    = "https://www.sec.gov"
REQUEST_DELAY   = 0.15   # seconds between calls → ~6 req/s, safely under the 10 req/s cap

# ── Tracked funds: (name, CIK) ────────────────────────────────────────────────
TRACKED_FUNDS = {
    "0001067983": "Berkshire Hathaway",
    "0001037389": "Renaissance Technologies",
    "0001336528": "Pershing Square Capital",
    "0001649339": "Scion Asset Management",
    "0001103804": "Viking Global Investors",
    "0001167483": "Tiger Global Management",
    "0000831641": "Appaloosa Management",
    "0001040273": "Third Point LLC",
    "0001350694": "Bridgewater Associates",
    "0001061219": "Baupost Group",
}

# ── Sector keyword fallback (used when ticker is unknown) ─────────────────────
SECTOR_KEYWORDS: list[tuple[str, list[str]]] = [
    ("AI & Data Infrastructure",  ["artificial intelligence", " ai ", "machine learning", "neural"]),
    ("Cybersecurity",              ["cyber", "security", "firewall", "endpoint"]),
    ("Cloud & SaaS",               ["cloud", "saas", "software as a service"]),
    ("Semiconductors",             ["semiconductor", "chip", "micro", "wafer", "fabless"]),
    ("Technology",                 ["tech", "software", "digital", "internet", "computing", "data", "system"]),
    ("Healthcare & Biotech",       ["pharma", "biotech", "therapeutics", "biosciences", "medical", "health", "hospital", "drug"]),
    ("Financials",                 ["bank", "financial", "capital", "insurance", "mortgage", "asset management", "investment"]),
    ("Energy",                     ["oil", "gas", "energy", "petroleum", "refin", "pipeline", "lng"]),
    ("Consumer Discretionary",     ["retail", "automotive", "auto", "hotel", "restaurant", "travel", "luxury", "e-commerce"]),
    ("Consumer Staples",           ["food", "beverage", "tobacco", "household", "consumer staple"]),
    ("Industrials",                ["aerospace", "defense", "manufactur", "transport", "logistics", "machinery"]),
    ("Materials",                  ["mining", "chemical", "steel", "aluminum", "material", "mineral"]),
    ("Real Estate",                ["reit", "property", "real estate"]),
    ("Utilities",                  ["electric", "utility", "utilities", "power", "water"]),
    ("Telecom",                    ["telecom", "communication", "wireless", "broadband", "5g"]),
]

# Hand-curated ticker → sector for the most commonly held names
TICKER_SECTOR: dict[str, str] = {
    # Semiconductors / AI Infrastructure
    "NVDA": "Semiconductors", "AMD": "Semiconductors", "INTC": "Semiconductors",
    "AVGO": "Semiconductors", "QCOM": "Semiconductors", "TXN": "Semiconductors",
    "AMAT": "Semiconductors", "ASML": "Semiconductors", "LRCX": "Semiconductors",
    "KLAC": "Semiconductors", "MU": "Semiconductors", "MRVL": "Semiconductors",
    "TSM": "Semiconductors",  "SMCI": "Semiconductors", "NXPI": "Semiconductors",
    # AI & Data
    "PLTR": "AI & Data Infrastructure", "AI": "AI & Data Infrastructure",
    "PATH": "AI & Data Infrastructure", "SNOW": "Cloud & SaaS",
    # Cloud / SaaS
    "CRM": "Cloud & SaaS", "NOW": "Cloud & SaaS", "WDAY": "Cloud & SaaS",
    "DDOG": "Cloud & SaaS", "MDB": "Cloud & SaaS", "TEAM": "Cloud & SaaS",
    "ZM":  "Cloud & SaaS", "TWLO": "Cloud & SaaS",
    # Big Tech
    "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Technology",
    "GOOG": "Technology", "META": "Technology", "AMZN": "Technology",
    "ORCL": "Technology", "IBM": "Technology",  "ADBE": "Technology",
    # Cybersecurity
    "CRWD": "Cybersecurity", "PANW": "Cybersecurity", "ZS": "Cybersecurity",
    "FTNT": "Cybersecurity", "OKTA": "Cybersecurity", "S": "Cybersecurity",
    "CYBR": "Cybersecurity",
    # Financials
    "JPM": "Financials", "BAC": "Financials", "GS": "Financials",
    "MS":  "Financials", "WFC": "Financials", "C": "Financials",
    "BLK": "Financials", "SCHW": "Financials", "AXP": "Financials",
    "V": "Financials",   "MA": "Financials",   "PYPL": "Financials",
    "COF": "Financials", "USB": "Financials",
    # Healthcare / Biotech
    "JNJ": "Healthcare & Biotech", "UNH": "Healthcare & Biotech",
    "PFE": "Healthcare & Biotech", "ABBV": "Healthcare & Biotech",
    "MRK": "Healthcare & Biotech", "LLY": "Healthcare & Biotech",
    "TMO": "Healthcare & Biotech", "DHR": "Healthcare & Biotech",
    "AMGN": "Healthcare & Biotech", "GILD": "Healthcare & Biotech",
    "BIIB": "Healthcare & Biotech", "REGN": "Healthcare & Biotech",
    "MRNA": "Healthcare & Biotech", "BMY": "Healthcare & Biotech",
    "ABT": "Healthcare & Biotech", "ISRG": "Healthcare & Biotech",
    # Energy
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy", "SLB": "Energy",
    "EOG": "Energy", "MPC": "Energy", "PSX": "Energy", "OXY": "Energy",
    "VLO": "Energy", "HES": "Energy", "DVN": "Energy",
    # Consumer Discretionary
    "TSLA": "Consumer Discretionary", "HD": "Consumer Discretionary",
    "LOW": "Consumer Discretionary", "MCD": "Consumer Discretionary",
    "SBUX": "Consumer Discretionary", "NKE": "Consumer Discretionary",
    "TGT": "Consumer Discretionary", "TJX": "Consumer Discretionary",
    "BKNG": "Consumer Discretionary", "F": "Consumer Discretionary",
    "GM": "Consumer Discretionary",
    # Consumer Staples
    "WMT": "Consumer Staples", "PG": "Consumer Staples", "KO": "Consumer Staples",
    "PEP": "Consumer Staples", "COST": "Consumer Staples", "PM": "Consumer Staples",
    "MO": "Consumer Staples",
    # Telecom
    "VZ": "Telecom", "T": "Telecom", "TMUS": "Telecom",
    # Industrials
    "BA": "Industrials", "CAT": "Industrials", "GE": "Industrials",
    "HON": "Industrials", "UPS": "Industrials", "RTX": "Industrials",
    "LMT": "Industrials", "NOC": "Industrials", "GD": "Industrials",
    "FDX": "Industrials", "DE": "Industrials",
    # Real Estate
    "AMT": "Real Estate", "PLD": "Real Estate", "CCI": "Real Estate",
    "EQIX": "Real Estate", "PSA": "Real Estate",
    # Materials
    "NEM": "Materials", "FCX": "Materials", "LIN": "Materials",
    "APD": "Materials", "SHW": "Materials",
    # Utilities
    "NEE": "Utilities", "DUK": "Utilities", "SO": "Utilities",
    "D": "Utilities", "AEP": "Utilities",
}


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _get(url: str, headers: Optional[dict] = None, retries: int = 3) -> Optional[requests.Response]:
    h = headers or HEADERS
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=h, timeout=30)
            time.sleep(REQUEST_DELAY)
            if resp.status_code == 200:
                return resp
            logger.warning("HTTP %s for %s", resp.status_code, url)
        except requests.RequestException as exc:
            logger.error("Request error (%s): %s", url, exc)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


# ── Ticker / sector resolution ─────────────────────────────────────────────────

# Cached name→ticker lookup built from SEC's company_tickers.json
_name_to_ticker: dict[str, str] = {}


def _load_sec_ticker_map() -> None:
    """Download SEC's public company_tickers.json and build a normalised name lookup."""
    global _name_to_ticker
    if _name_to_ticker:
        return
    resp = _get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=EDGAR_HEADERS,
    )
    if not resp:
        logger.warning("Could not load SEC ticker map; tickers will be left blank.")
        return
    data = resp.json()
    for entry in data.values():
        name   = _normalise_name(str(entry.get("title", "")))
        ticker = str(entry.get("ticker", "")).upper().strip()
        if name and ticker:
            _name_to_ticker[name] = ticker
    logger.info("Loaded %d tickers from SEC company_tickers.json", len(_name_to_ticker))


def _normalise_name(name: str) -> str:
    """Lowercase, strip common corporate suffixes and punctuation for fuzzy matching."""
    name = name.lower().strip()
    for suffix in [" inc", " corp", " co", " ltd", " llc", " lp", " plc",
                   " group", " holdings", " international", " the",
                   ".", ",", "'", "/"]:
        name = name.replace(suffix, " ")
    return re.sub(r"\s+", " ", name).strip()


def resolve_ticker(issuer_name: str) -> str:
    """Best-effort CUSIP issuer name → ticker using the SEC name lookup."""
    if not _name_to_ticker:
        _load_sec_ticker_map()
    key = _normalise_name(issuer_name)
    # Exact match
    if key in _name_to_ticker:
        return _name_to_ticker[key]
    # Try stripping up to 2 trailing words (handles "Apple Inc Com" → "Apple")
    parts = key.split()
    for n in (len(parts) - 1, len(parts) - 2):
        if n >= 1:
            short = " ".join(parts[:n])
            if short in _name_to_ticker:
                return _name_to_ticker[short]
    return ""


def resolve_sector(ticker: str, issuer_name: str) -> str:
    """Return sector string using ticker map first, then keyword scan on issuer name."""
    if ticker and ticker.upper() in TICKER_SECTOR:
        return TICKER_SECTOR[ticker.upper()]
    name_lower = issuer_name.lower()
    for sector, keywords in SECTOR_KEYWORDS:
        if any(kw in name_lower for kw in keywords):
            return sector
    return "Other"


# ── XML parsing helpers ────────────────────────────────────────────────────────

def _strip_ns(tag: str) -> str:
    """'{http://...}localName' → 'localName'."""
    return tag.split("}")[-1] if "}" in tag else tag


def _find_text(element: ET.Element, *local_names: str) -> str:
    """Search for the first matching child (any namespace) and return its text."""
    for child in element.iter():
        if _strip_ns(child.tag) in local_names:
            return (child.text or "").strip()
    return ""


def parse_info_table(xml_bytes: bytes) -> list[dict]:
    """
    Parse a 13F-HR information table XML file.

    Returns a list of dicts:
      { cusip, issuer_name, title_of_class, value (thousands), shares }
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        logger.error("XML parse error: %s", exc)
        return []

    holdings = []
    for row in root.iter():
        if _strip_ns(row.tag) != "infoTable":
            continue

        cusip      = _find_text(row, "cusip")
        name       = _find_text(row, "nameOfIssuer")
        title      = _find_text(row, "titleOfClass")
        value_str  = _find_text(row, "value")
        shares_str = _find_text(row, "sshPrnamt")

        if not cusip:
            continue

        try:
            value  = float(value_str.replace(",", "")) if value_str else 0.0
            shares = float(shares_str.replace(",", "")) if shares_str else 0.0
        except ValueError:
            value, shares = 0.0, 0.0

        holdings.append({
            "cusip":          cusip.strip(),
            "issuer_name":    name,
            "title_of_class": title,
            "value":          value,
            "shares":         shares,
        })

    return holdings


# ── Filing discovery ───────────────────────────────────────────────────────────

def fetch_fund_filings(cik: str, max_filings: int = 4) -> list[dict]:
    """
    Return the most recent `max_filings` 13F-HR filings for a fund.
    Result: [{ accession_number, date_filed, period_of_report }, ...]
    """
    url  = f"{SEC_DATA_BASE}/submissions/CIK{int(cik):010d}.json"
    resp = _get(url)
    if not resp:
        return []

    data     = resp.json()
    filings  = data.get("filings", {}).get("recent", {})
    forms    = filings.get("form", [])
    acc_nums = filings.get("accessionNumber", [])
    dates    = filings.get("filingDate", [])
    periods  = filings.get("reportDate", [])

    results = []
    for form, acc, filed, period in zip(forms, acc_nums, dates, periods):
        if form.upper() == "13F-HR":
            results.append({
                "accession_number": acc,
                "date_filed":       filed,
                "period_of_report": period,
            })
            if len(results) >= max_filings:
                break

    return results


def _find_infotable_url(cik: str, accession_number: str) -> Optional[str]:
    """
    Fetch the filing index and return the URL of the information table XML file.
    Looks for a document with type '13F-HR' or description containing 'INFORMATION TABLE'.
    """
    acc_dashes    = accession_number.replace("-", "")
    cik_int       = str(int(cik))
    index_url     = f"{SEC_WWW_BASE}/Archives/edgar/data/{cik_int}/{acc_dashes}/{accession_number}-index.htm"
    filing_json   = f"{SEC_DATA_BASE}/submissions/CIK{int(cik):010d}/filings/{accession_number}.json"

    # Use the raw filing index JSON if available
    resp = _get(
        f"{SEC_WWW_BASE}/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13F-HR&dateb=&owner=include&count=40&search_text=",
        headers=EDGAR_HEADERS,
    )

    # Primary approach: fetch the index page and scan for the info table link
    index_page_url = (
        f"{SEC_WWW_BASE}/Archives/edgar/data/{cik_int}/{acc_dashes}/"
    )
    index_resp = _get(index_page_url, headers=EDGAR_HEADERS)
    if not index_resp:
        return None

    # Regex: look for links ending in .xml that are likely the info table
    links = re.findall(r'href="([^"]+\.xml)"', index_resp.text, re.IGNORECASE)
    for link in links:
        lower = link.lower()
        if any(kw in lower for kw in ("infotable", "information", "13fhr", "form13f")):
            base = f"{SEC_WWW_BASE}{link}" if link.startswith("/") else link
            return base

    # Fallback: return the first XML link found (likely the primary document)
    if links:
        link = links[0]
        return f"{SEC_WWW_BASE}{link}" if link.startswith("/") else link

    return None


def fetch_holdings_for_filing(cik: str, accession_number: str) -> list[dict]:
    """Download and parse the 13F information table for one accession number."""
    url = _find_infotable_url(cik, accession_number)
    if not url:
        logger.warning("No info table found for CIK %s / %s", cik, accession_number)
        return []

    resp = _get(url, headers=EDGAR_HEADERS)
    if not resp:
        return []

    return parse_info_table(resp.content)


# ── Database sync ──────────────────────────────────────────────────────────────

def _upsert_fund(db: Session, cik: str, name: str) -> Fund:
    fund = db.query(Fund).filter(Fund.cik == cik).first()
    if not fund:
        fund = Fund(cik=cik, name=name)
        db.add(fund)
        db.flush()
    return fund


def _upsert_filing(db: Session, fund: Fund, meta: dict) -> Optional[Filing]:
    period = meta["period_of_report"]
    if not period:
        return None
    existing = (
        db.query(Filing)
        .filter(Filing.fund_id == fund.id, Filing.period_of_report == period)
        .first()
    )
    if existing:
        return None   # already loaded; skip
    filing = Filing(
        fund_id          = fund.id,
        period_of_report = period,
        date_filed       = meta.get("date_filed"),
        accession_number = meta.get("accession_number"),
    )
    db.add(filing)
    db.flush()
    return filing


def sync_fund(cik: str, name: str, db: Session, max_filings: int = 4) -> int:
    """
    Fetch and store up to `max_filings` quarters of 13F data for one fund.
    Returns count of new Holding rows inserted.
    """
    _load_sec_ticker_map()

    fund        = _upsert_fund(db, cik, name)
    filing_metas = fetch_fund_filings(cik, max_filings)
    inserted    = 0

    for meta in filing_metas:
        filing = _upsert_filing(db, fund, meta)
        if filing is None:
            logger.info("  [%s] period %s already in DB — skipping", name, meta["period_of_report"])
            continue

        logger.info("  [%s] fetching %s (%s)", name, meta["accession_number"], meta["period_of_report"])
        raw_holdings = fetch_holdings_for_filing(cik, meta["accession_number"])

        total_value = 0.0
        for h in raw_holdings:
            ticker = resolve_ticker(h["issuer_name"])
            sector = resolve_sector(ticker, h["issuer_name"])
            holding = Holding(
                filing_id      = filing.id,
                cusip          = h["cusip"],
                ticker         = ticker,
                issuer_name    = h["issuer_name"],
                title_of_class = h["title_of_class"],
                value          = h["value"],
                shares         = h["shares"],
                sector         = sector,
            )
            db.add(holding)
            total_value += h["value"]
            inserted    += 1

        filing.total_value = total_value
        db.commit()
        logger.info("  [%s] stored %d positions, period=%s", name, len(raw_holdings), meta["period_of_report"])

    return inserted


def sync_all_funds(db: Optional[Session] = None, max_filings: int = 4) -> dict:
    """Sync all tracked funds. Returns a summary dict."""
    close_after = db is None
    if db is None:
        db = SessionLocal()

    init_db()
    summary = {}
    for cik, name in TRACKED_FUNDS.items():
        logger.info("Syncing %s (CIK %s) …", name, cik)
        try:
            n = sync_fund(cik, name, db, max_filings)
            summary[name] = {"status": "ok", "new_holdings": n}
        except Exception as exc:
            logger.exception("Failed to sync %s: %s", name, exc)
            summary[name] = {"status": "error", "error": str(exc)}

    if close_after:
        db.close()
    return summary


# ── CLI entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    result = sync_all_funds()
    for fund, info in result.items():
        print(f"{fund}: {info}")
