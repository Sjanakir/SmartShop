"""
Pandas-powered QoQ analysis and thematic aggregation engine.

Steps:
  1. For each fund, compare consecutive 13F quarters → PositionChange rows
  2. Aggregate all funds' changes by sector/period → ThemeSnapshot rows
  3. Provide query helpers consumed by app.py
"""

import logging
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from database import (
    SessionLocal, Fund, Filing, Holding,
    PositionChange, ThemeSnapshot,
)

logger = logging.getLogger(__name__)

# ── QoQ change classification ─────────────────────────────────────────────────

def _classify_action(prev_shares: float, curr_shares: float) -> str:
    if prev_shares == 0 and curr_shares > 0:
        return "NEW"
    if curr_shares == 0 and prev_shares > 0:
        return "SOLD_OUT"
    if curr_shares > prev_shares:
        return "INCREASED"
    if curr_shares < prev_shares:
        return "DECREASED"
    return "UNCHANGED"


def _pct_change(prev: float, curr: float) -> Optional[float]:
    """Safe percentage change; None if previous is zero."""
    if prev == 0:
        return None
    return round((curr - prev) / prev * 100, 2)


# ── Per-fund QoQ computation ──────────────────────────────────────────────────

def compute_qoq_for_fund(fund: Fund, db: Session) -> int:
    """
    Compare consecutive filings for one fund and upsert PositionChange rows.
    Returns the number of change rows written.
    """
    filings = (
        db.query(Filing)
        .filter(Filing.fund_id == fund.id)
        .order_by(Filing.period_of_report)
        .all()
    )

    if len(filings) < 2:
        return 0

    written = 0
    for prev_filing, curr_filing in zip(filings, filings[1:]):
        # Build DataFrames from the two quarters
        def to_df(filing: Filing) -> pd.DataFrame:
            rows = [
                {
                    "cusip":       h.cusip,
                    "ticker":      h.ticker or "",
                    "issuer_name": h.issuer_name or "",
                    "sector":      h.sector or "Other",
                    "shares":      h.shares or 0.0,
                    "value":       h.value or 0.0,
                }
                for h in filing.holdings
            ]
            if not rows:
                return pd.DataFrame(columns=["cusip", "ticker", "issuer_name", "sector", "shares", "value"])
            return pd.DataFrame(rows).groupby("cusip", as_index=False).agg(
                ticker    =("ticker", "first"),
                issuer_name=("issuer_name", "first"),
                sector    =("sector", "first"),
                shares    =("shares", "sum"),
                value     =("value", "sum"),
            )

        prev_df = to_df(prev_filing)
        curr_df = to_df(curr_filing)

        # Outer-join on CUSIP so both opens and closes are captured
        merged = pd.merge(
            prev_df.rename(columns={"shares": "prev_shares", "value": "prev_value"}),
            curr_df.rename(columns={"shares": "curr_shares", "value": "curr_value"}),
            on="cusip", how="outer", suffixes=("_prev", "_curr"),
        )

        # Coalesce metadata columns that came from only one side
        for col in ("ticker", "issuer_name", "sector"):
            merged[col] = merged[f"{col}_prev"].fillna("").where(
                merged[f"{col}_prev"].fillna("") != "", merged[f"{col}_curr"].fillna("")
            )

        merged["prev_shares"] = merged["prev_shares"].fillna(0)
        merged["curr_shares"] = merged["curr_shares"].fillna(0)
        merged["prev_value"]  = merged["prev_value"].fillna(0)
        merged["curr_value"]  = merged["curr_value"].fillna(0)

        period = curr_filing.period_of_report

        for _, row in merged.iterrows():
            action = _classify_action(row["prev_shares"], row["curr_shares"])
            if action == "UNCHANGED":
                continue

            # Delete stale row for this (fund, cusip, period) if it exists
            db.query(PositionChange).filter(
                PositionChange.fund_id        == fund.id,
                PositionChange.cusip          == row["cusip"],
                PositionChange.current_period == period,
            ).delete(synchronize_session=False)

            pc = PositionChange(
                fund_id        = fund.id,
                cusip          = row["cusip"],
                ticker         = row["ticker"],
                issuer_name    = row["issuer_name"],
                sector         = row["sector"],
                current_period = period,
                action         = action,
                prev_shares    = row["prev_shares"],
                curr_shares    = row["curr_shares"],
                prev_value     = row["prev_value"],
                curr_value     = row["curr_value"],
                shares_chg_pct = _pct_change(row["prev_shares"], row["curr_shares"]),
                value_chg      = row["curr_value"] - row["prev_value"],
            )
            db.add(pc)
            written += 1

        db.commit()

    return written


def compute_all_qoq(db: Optional[Session] = None) -> dict:
    """Run QoQ analysis for every fund. Returns a summary."""
    close_after = db is None
    if db is None:
        db = SessionLocal()

    summary = {}
    for fund in db.query(Fund).all():
        n = compute_qoq_for_fund(fund, db)
        summary[fund.name] = n
        logger.info("QoQ: %s → %d change rows", fund.name, n)

    if close_after:
        db.close()
    return summary


# ── Thematic aggregation ──────────────────────────────────────────────────────

def compute_theme_snapshots(db: Optional[Session] = None) -> None:
    """
    For each (sector, period) pair, count how many funds were buying vs selling,
    sum the dollar-value change, and compute a conviction score in [−1, +1].
    """
    close_after = db is None
    if db is None:
        db = SessionLocal()

    changes = db.query(PositionChange).all()
    if not changes:
        logger.warning("No position changes found; run QoQ analysis first.")
        if close_after:
            db.close()
        return

    rows = [
        {
            "fund_id":   c.fund_id,
            "sector":    c.sector or "Other",
            "period":    c.current_period,
            "action":    c.action,
            "value_chg": c.value_chg or 0.0,
        }
        for c in changes
    ]
    df = pd.DataFrame(rows)

    # Aggregate per (sector, period)
    for (sector, period), grp in df.groupby(["sector", "period"]):
        funds_buying  = grp[grp["action"].isin(["NEW", "INCREASED"])]["fund_id"].nunique()
        funds_selling = grp[grp["action"] == "DECREASED"]["fund_id"].nunique()
        funds_new     = grp[grp["action"] == "NEW"]["fund_id"].nunique()
        total_chg     = grp["value_chg"].sum()
        total_active  = grp["fund_id"].nunique()
        conviction    = round((funds_buying - funds_selling) / max(total_active, 1), 4)

        # Upsert
        snap = (
            db.query(ThemeSnapshot)
            .filter(ThemeSnapshot.sector == sector, ThemeSnapshot.period == period)
            .first()
        )
        if not snap:
            snap = ThemeSnapshot(sector=sector, period=period)
            db.add(snap)

        snap.funds_buying     = int(funds_buying)
        snap.funds_selling    = int(funds_selling)
        snap.funds_new        = int(funds_new)
        snap.total_value_chg  = float(total_chg)
        snap.conviction_score = float(conviction)

    db.commit()
    logger.info("Theme snapshots refreshed.")

    if close_after:
        db.close()


# ── Read-side query helpers (used by app.py) ──────────────────────────────────

def get_latest_period(db: Session) -> Optional[str]:
    row = db.query(PositionChange.current_period).order_by(
        PositionChange.current_period.desc()
    ).first()
    return row[0] if row else None


def get_themes_for_period(db: Session, period: str) -> list[dict]:
    """Return all ThemeSnapshot rows for a period, sorted by conviction score desc."""
    snaps = (
        db.query(ThemeSnapshot)
        .filter(ThemeSnapshot.period == period)
        .order_by(ThemeSnapshot.conviction_score.desc())
        .all()
    )
    return [
        {
            "sector":           s.sector,
            "period":           s.period,
            "funds_buying":     s.funds_buying,
            "funds_selling":    s.funds_selling,
            "funds_new":        s.funds_new,
            "total_value_chg":  s.total_value_chg,
            "conviction_score": s.conviction_score,
        }
        for s in snaps
    ]


def get_top_buys(db: Session, period: str, limit: int = 20) -> list[dict]:
    """
    Highest-conviction individual stock buys across all funds:
    number of funds buying, aggregate value increase.
    """
    rows = db.query(PositionChange).filter(
        PositionChange.current_period == period,
        PositionChange.action.in_(["NEW", "INCREASED"]),
    ).all()
    if not rows:
        return []

    df = pd.DataFrame([
        {
            "cusip":       r.cusip,
            "ticker":      r.ticker or r.issuer_name or r.cusip,
            "issuer_name": r.issuer_name,
            "sector":      r.sector,
            "value_chg":   r.value_chg or 0,
            "action":      r.action,
        }
        for r in rows
    ])

    agg = (
        df.groupby(["cusip", "ticker", "issuer_name", "sector"])
        .agg(
            fund_count =("cusip", "count"),
            total_value_chg=("value_chg", "sum"),
        )
        .reset_index()
        .sort_values("fund_count", ascending=False)
        .head(limit)
    )
    return agg.to_dict(orient="records")


def get_fund_changes(db: Session, fund_id: int, period: Optional[str] = None) -> list[dict]:
    """Return position changes for one fund, optionally filtered to a period."""
    q = db.query(PositionChange).filter(PositionChange.fund_id == fund_id)
    if period:
        q = q.filter(PositionChange.current_period == period)
    else:
        latest = get_latest_period(db)
        if latest:
            q = q.filter(PositionChange.current_period == latest)

    rows = q.order_by(PositionChange.value_chg.desc()).all()
    return [
        {
            "cusip":         r.cusip,
            "ticker":        r.ticker or r.issuer_name or r.cusip,
            "issuer_name":   r.issuer_name,
            "sector":        r.sector,
            "action":        r.action,
            "prev_shares":   r.prev_shares,
            "curr_shares":   r.curr_shares,
            "prev_value":    r.prev_value,
            "curr_value":    r.curr_value,
            "shares_chg_pct": r.shares_chg_pct,
            "value_chg":     r.value_chg,
            "period":        r.current_period,
        }
        for r in rows
    ]


def get_sector_detail(db: Session, sector: str, period: Optional[str] = None) -> list[dict]:
    """Return all stocks in a sector for a period, with fund-level backing counts."""
    if not period:
        period = get_latest_period(db)
    if not period:
        return []

    rows = (
        db.query(PositionChange)
        .filter(
            PositionChange.current_period == period,
            PositionChange.sector == sector,
        )
        .all()
    )
    if not rows:
        return []

    df = pd.DataFrame([
        {
            "cusip":       r.cusip,
            "ticker":      r.ticker or r.issuer_name or r.cusip,
            "issuer_name": r.issuer_name,
            "action":      r.action,
            "fund_id":     r.fund_id,
            "value_chg":   r.value_chg or 0,
        }
        for r in rows
    ])

    result = []
    for (cusip, ticker, name), grp in df.groupby(["cusip", "ticker", "issuer_name"]):
        buying  = grp[grp["action"].isin(["NEW", "INCREASED"])]["fund_id"].nunique()
        selling = grp[grp["action"] == "DECREASED"]["fund_id"].nunique()
        result.append({
            "cusip":           cusip,
            "ticker":          ticker,
            "issuer_name":     name,
            "funds_buying":    int(buying),
            "funds_selling":   int(selling),
            "total_value_chg": float(grp["value_chg"].sum()),
        })

    result.sort(key=lambda x: x["funds_buying"], reverse=True)
    return result


def get_dashboard_data(db: Session) -> dict:
    """Aggregate everything needed for the dashboard in one call."""
    period = get_latest_period(db)
    if not period:
        return {"period": None, "themes": [], "top_buys": [], "funds": []}

    funds = db.query(Fund).all()
    return {
        "period":    period,
        "themes":    get_themes_for_period(db, period),
        "top_buys":  get_top_buys(db, period),
        "funds": [{"id": f.id, "name": f.name, "cik": f.cik} for f in funds],
    }


# ── CLI entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    summary = compute_all_qoq()
    compute_theme_snapshots()
    print("QoQ rows per fund:", summary)
