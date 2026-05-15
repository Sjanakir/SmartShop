"""
SQLAlchemy models for the 13F Thematic Aggregator.
Tables: Fund → Filing → Holding (raw positions per quarter)
        PositionChange (QoQ diffs), ThemeSnapshot (sector aggregates)
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float,
    ForeignKey, Text, UniqueConstraint, Index,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

DATABASE_URL = "sqlite:///./sec_13f.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ── Dependency for FastAPI ────────────────────────────────────────────────────
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Core models ───────────────────────────────────────────────────────────────

class Fund(Base):
    """One row per tracked institutional investor."""
    __tablename__ = "funds"

    id          = Column(Integer, primary_key=True, index=True)
    cik         = Column(String(12), unique=True, nullable=False)   # SEC CIK (zero-padded to 10 digits)
    name        = Column(String(200), nullable=False)
    description = Column(Text)

    filings         = relationship("Filing", back_populates="fund", cascade="all, delete-orphan")
    position_changes = relationship("PositionChange", back_populates="fund", cascade="all, delete-orphan")


class Filing(Base):
    """One row per 13F-HR submission (one per fund per quarter)."""
    __tablename__ = "filings"

    id               = Column(Integer, primary_key=True, index=True)
    fund_id          = Column(Integer, ForeignKey("funds.id"), nullable=False)
    period_of_report = Column(String(10), nullable=False)   # YYYY-MM-DD  (end of quarter)
    date_filed       = Column(String(10))
    accession_number = Column(String(25))                   # e.g. 0001234567-23-000001
    total_value      = Column(Float)                        # sum of position values (thousands USD)

    __table_args__ = (
        UniqueConstraint("fund_id", "period_of_report", name="uq_fund_period"),
    )

    fund     = relationship("Fund", back_populates="filings")
    holdings = relationship("Holding", back_populates="filing", cascade="all, delete-orphan")


class Holding(Base):
    """Individual stock position reported in one 13F-HR filing."""
    __tablename__ = "holdings"

    id             = Column(Integer, primary_key=True, index=True)
    filing_id      = Column(Integer, ForeignKey("filings.id"), nullable=False)
    cusip          = Column(String(9), nullable=False)   # 9-char CUSIP
    ticker         = Column(String(15))                  # best-effort resolved ticker
    issuer_name    = Column(String(250))
    title_of_class = Column(String(80))
    value          = Column(Float)   # USD thousands (as reported)
    shares         = Column(Float)   # share/prn amount
    sector         = Column(String(100), default="Other")

    __table_args__ = (
        Index("ix_holding_filing_cusip", "filing_id", "cusip"),
    )

    filing = relationship("Filing", back_populates="holdings")


class PositionChange(Base):
    """
    Quarter-over-quarter diff for one (fund, security) pair.
    Populated by analyzer.py after each sync.
    """
    __tablename__ = "position_changes"

    id            = Column(Integer, primary_key=True, index=True)
    fund_id       = Column(Integer, ForeignKey("funds.id"), nullable=False)
    cusip         = Column(String(9))
    ticker        = Column(String(15))
    issuer_name   = Column(String(250))
    sector        = Column(String(100), default="Other")
    current_period = Column(String(10))          # the later (newer) quarter
    action        = Column(String(20))            # NEW | INCREASED | DECREASED | SOLD_OUT
    prev_shares   = Column(Float, default=0)
    curr_shares   = Column(Float, default=0)
    prev_value    = Column(Float, default=0)     # thousands USD
    curr_value    = Column(Float, default=0)
    shares_chg_pct = Column(Float)               # % change in share count
    value_chg     = Column(Float)                # absolute USD-thousand change

    __table_args__ = (
        UniqueConstraint("fund_id", "cusip", "current_period", name="uq_change_key"),
        Index("ix_change_period_sector", "current_period", "sector"),
    )

    fund = relationship("Fund", back_populates="position_changes")


class ThemeSnapshot(Base):
    """
    Sector-level aggregate for one quarter.
    Conviction score = (funds_buying - funds_selling) / total_active_funds
    """
    __tablename__ = "theme_snapshots"

    id               = Column(Integer, primary_key=True, index=True)
    sector           = Column(String(100), nullable=False)
    period           = Column(String(10), nullable=False)   # YYYY-MM-DD
    funds_buying     = Column(Integer, default=0)            # funds that net-increased exposure
    funds_selling    = Column(Integer, default=0)
    funds_new        = Column(Integer, default=0)            # funds opening fresh positions
    total_value_chg  = Column(Float, default=0)              # aggregate USD-thousand change
    conviction_score = Column(Float, default=0)              # −1 … +1

    __table_args__ = (
        UniqueConstraint("sector", "period", name="uq_theme_period"),
    )


# ── Bootstrap ─────────────────────────────────────────────────────────────────

def init_db():
    Base.metadata.create_all(bind=engine)
