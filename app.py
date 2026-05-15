"""
FastAPI backend for the 13F Thematic Aggregator.

Endpoints:
  GET  /api/funds                     – list all tracked funds
  GET  /api/funds/{fund_id}           – fund details + latest holdings
  GET  /api/funds/{fund_id}/changes   – QoQ position changes for one fund
  GET  /api/themes                    – sector-level thematic trends (latest quarter)
  GET  /api/themes/{sector}           – stock-level detail within a sector
  GET  /api/dashboard                 – combined snapshot for the main dashboard
  POST /api/sync                      – trigger a background SEC data refresh
  GET  /api/sync/status               – check whether a sync is in progress
"""

import logging
import threading
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from database import get_db, init_db, Fund, Filing, Holding, ThemeSnapshot
from analyzer import (
    compute_all_qoq,
    compute_theme_snapshots,
    get_dashboard_data,
    get_themes_for_period,
    get_top_buys,
    get_fund_changes,
    get_sector_detail,
    get_latest_period,
)
from sec_scraper import sync_all_funds

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="13F Thematic Aggregator",
    description="Track institutional 13F filings and surface macro investment themes.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the frontend
app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/", include_in_schema=False)
def serve_frontend():
    return FileResponse("frontend/index.html")


# ── Startup ────────────────────────────────────────────────────────────────────

@app.on_event("startup")
def on_startup():
    init_db()
    logger.info("Database initialised.")


# ── Sync state ─────────────────────────────────────────────────────────────────

_sync_lock   = threading.Lock()
_sync_status = {"running": False, "last_result": None, "error": None}


def _run_sync():
    global _sync_status
    try:
        logger.info("Background sync started …")
        result = sync_all_funds(max_filings=4)
        compute_all_qoq()
        compute_theme_snapshots()
        _sync_status["last_result"] = result
        logger.info("Background sync complete.")
    except Exception as exc:
        logger.exception("Sync failed: %s", exc)
        _sync_status["error"] = str(exc)
    finally:
        _sync_status["running"] = False


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/api/dashboard")
def dashboard(db: Session = Depends(get_db)):
    """Combined data for the main dashboard: themes, top buys, fund list."""
    return get_dashboard_data(db)


@app.get("/api/funds")
def list_funds(db: Session = Depends(get_db)):
    funds = db.query(Fund).all()
    result = []
    for f in funds:
        latest_filing = (
            db.query(Filing)
            .filter(Filing.fund_id == f.id)
            .order_by(Filing.period_of_report.desc())
            .first()
        )
        result.append({
            "id":            f.id,
            "cik":           f.cik,
            "name":          f.name,
            "latest_period": latest_filing.period_of_report if latest_filing else None,
            "total_value":   latest_filing.total_value if latest_filing else None,
        })
    return result


@app.get("/api/funds/{fund_id}")
def fund_detail(fund_id: int, db: Session = Depends(get_db)):
    fund = db.query(Fund).filter(Fund.id == fund_id).first()
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")

    filings = (
        db.query(Filing)
        .filter(Filing.fund_id == fund_id)
        .order_by(Filing.period_of_report.desc())
        .all()
    )

    filing_data = []
    for fil in filings:
        # Sector breakdown for this filing
        holdings_df_raw = [
            {"sector": h.sector or "Other", "value": h.value or 0}
            for h in fil.holdings
        ]
        sector_breakdown: dict[str, float] = {}
        for row in holdings_df_raw:
            sector_breakdown[row["sector"]] = sector_breakdown.get(row["sector"], 0) + row["value"]

        filing_data.append({
            "period_of_report": fil.period_of_report,
            "date_filed":       fil.date_filed,
            "total_value":      fil.total_value,
            "holding_count":    len(fil.holdings),
            "sector_breakdown": sorted(
                [{"sector": k, "value": v} for k, v in sector_breakdown.items()],
                key=lambda x: x["value"], reverse=True
            ),
        })

    return {
        "id":      fund.id,
        "cik":     fund.cik,
        "name":    fund.name,
        "filings": filing_data,
    }


@app.get("/api/funds/{fund_id}/changes")
def fund_changes(
    fund_id: int,
    period: Optional[str] = Query(None, description="YYYY-MM-DD quarter end"),
    db: Session = Depends(get_db),
):
    fund = db.query(Fund).filter(Fund.id == fund_id).first()
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")
    return get_fund_changes(db, fund_id, period)


@app.get("/api/funds/{fund_id}/holdings")
def fund_holdings(
    fund_id: int,
    period: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Raw holdings for a specific fund + quarter."""
    fund = db.query(Fund).filter(Fund.id == fund_id).first()
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")

    q = db.query(Filing).filter(Filing.fund_id == fund_id)
    if period:
        q = q.filter(Filing.period_of_report == period)
    else:
        q = q.order_by(Filing.period_of_report.desc())
    filing = q.first()

    if not filing:
        return []

    return [
        {
            "cusip":          h.cusip,
            "ticker":         h.ticker,
            "issuer_name":    h.issuer_name,
            "title_of_class": h.title_of_class,
            "value":          h.value,
            "shares":         h.shares,
            "sector":         h.sector,
        }
        for h in sorted(filing.holdings, key=lambda x: x.value or 0, reverse=True)
    ]


@app.get("/api/themes")
def themes(
    period: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Return thematic sector aggregates for the specified (or latest) period."""
    if not period:
        period = get_latest_period(db)
    if not period:
        return {"period": None, "themes": []}
    return {"period": period, "themes": get_themes_for_period(db, period)}


@app.get("/api/themes/{sector}")
def sector_detail(
    sector: str,
    period: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Return individual stock backing within a sector."""
    return {
        "sector":  sector,
        "period":  period or get_latest_period(db),
        "stocks":  get_sector_detail(db, sector, period),
    }


@app.get("/api/top-buys")
def top_buys(
    period: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    if not period:
        period = get_latest_period(db)
    if not period:
        return []
    return get_top_buys(db, period, limit)


@app.post("/api/sync")
def trigger_sync(background_tasks: BackgroundTasks):
    """Kick off a background SEC data fetch + analysis pass."""
    with _sync_lock:
        if _sync_status["running"]:
            return {"status": "already_running"}
        _sync_status["running"] = True
        _sync_status["error"]   = None

    background_tasks.add_task(_run_sync)
    return {"status": "started"}


@app.get("/api/sync/status")
def sync_status():
    return _sync_status


# ── Dev run ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
