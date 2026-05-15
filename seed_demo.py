"""
Seed the database with realistic demo data so the UI works without
waiting for a full SEC EDGAR sync (which can take several minutes).

Run:  python seed_demo.py
Then: uvicorn app:app --reload
"""

import random
from database import init_db, SessionLocal, Fund, Filing, Holding, PositionChange, ThemeSnapshot
from analyzer import compute_all_qoq, compute_theme_snapshots

DEMO_FUNDS = [
    ("0001067983", "Berkshire Hathaway"),
    ("0001037389", "Renaissance Technologies"),
    ("0001336528", "Pershing Square Capital"),
    ("0001649339", "Scion Asset Management"),
    ("0001103804", "Viking Global Investors"),
    ("0001167483", "Tiger Global Management"),
    ("0000831641", "Appaloosa Management"),
    ("0001040273", "Third Point LLC"),
    ("0001350694", "Bridgewater Associates"),
    ("0001061219", "Baupost Group"),
]

# (cusip, ticker, issuer, sector)
DEMO_STOCKS = [
    ("037833100", "AAPL",  "Apple Inc",              "Technology"),
    ("594918104", "MSFT",  "Microsoft Corp",          "Technology"),
    ("02079K305", "GOOGL", "Alphabet Inc",            "Technology"),
    ("67066G104", "NVDA",  "NVIDIA Corp",             "Semiconductors"),
    ("74762E102", "PLTR",  "Palantir Technologies",   "AI & Data Infrastructure"),
    ("891027102", "CRWD",  "CrowdStrike Holdings",    "Cybersecurity"),
    ("30303M102", "META",  "Meta Platforms",          "Technology"),
    ("023135106", "AMZN",  "Amazon.com Inc",          "Technology"),
    ("46090E103", "NOW",   "ServiceNow Inc",          "Cloud & SaaS"),
    ("81762P102", "SNOW",  "Snowflake Inc",           "Cloud & SaaS"),
    ("191216100", "KO",    "Coca-Cola Co",            "Consumer Staples"),
    ("461202103", "JPM",   "JPMorgan Chase",          "Financials"),
    ("084670702", "BRK.B", "Berkshire Hathaway B",   "Financials"),
    ("92826C839", "V",     "Visa Inc",                "Financials"),
    ("57636Q104", "MA",    "Mastercard Inc",          "Financials"),
    ("30231G102", "XOM",   "Exxon Mobil Corp",        "Energy"),
    ("166764100", "CVX",   "Chevron Corp",            "Energy"),
    ("00724F101", "ADBE",  "Adobe Inc",               "Technology"),
    ("594918104", "PANW",  "Palo Alto Networks",      "Cybersecurity"),
    ("98980G102", "ZS",    "Zscaler Inc",             "Cybersecurity"),
    ("716943103", "PFE",   "Pfizer Inc",              "Healthcare & Biotech"),
    ("58933Y105", "MRK",   "Merck & Co",              "Healthcare & Biotech"),
    ("002824100", "ABBV",  "AbbVie Inc",              "Healthcare & Biotech"),
    ("91324P102", "UNH",   "UnitedHealth Group",      "Healthcare & Biotech"),
    ("855244109", "TSLA",  "Tesla Inc",               "Consumer Discretionary"),
    ("828806109", "SBUX",  "Starbucks Corp",          "Consumer Discretionary"),
    ("369604103", "GE",    "GE Aerospace",            "Industrials"),
    ("097023105", "BA",    "Boeing Co",               "Industrials"),
    ("69608A108", "PLTR",  "Palantir Technologies A", "AI & Data Infrastructure"),
    ("14040H105", "AMD",   "Advanced Micro Devices",  "Semiconductors"),
    ("879585208", "TSM",   "Taiwan Semiconductor",    "Semiconductors"),
    ("126650100", "CRM",   "Salesforce Inc",          "Cloud & SaaS"),
    ("45866F104", "INTC",  "Intel Corp",              "Semiconductors"),
]

PERIODS = ["2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31"]

rng = random.Random(42)

def rand_shares(base=1_000_000):
    return round(rng.uniform(0.5, 5) * base)

def rand_value(shares, price_range=(50, 800)):
    price = rng.uniform(*price_range)
    return round(shares * price / 1000)   # in thousands

def main():
    init_db()
    db = SessionLocal()

    print("Seeding funds …")
    fund_objs = {}
    for cik, name in DEMO_FUNDS:
        f = db.query(Fund).filter(Fund.cik == cik).first()
        if not f:
            f = Fund(cik=cik, name=name)
            db.add(f)
            db.flush()
        fund_objs[cik] = f

    print("Seeding filings & holdings …")
    for cik, _ in DEMO_FUNDS:
        fund = fund_objs[cik]
        # Each fund holds a random subset of stocks, consistent across quarters with drift
        held_stocks = rng.sample(DEMO_STOCKS, k=rng.randint(8, 18))

        prev_shares: dict[str, float] = {}
        for period in PERIODS:
            # Skip if already exists
            exists = db.query(Filing).filter(
                Filing.fund_id == fund.id,
                Filing.period_of_report == period
            ).first()
            if exists:
                continue

            filing = Filing(
                fund_id=fund.id,
                period_of_report=period,
                date_filed=period[:7] + "-15",
                accession_number=f"0001{cik[-6:]}-{period[:4]}-{period[5:7]}0001",
            )
            db.add(filing)
            db.flush()

            total_value = 0.0
            for stock in held_stocks:
                cusip, ticker, name, sector = stock
                # Introduce drift: some stocks grow, some shrink, occasionally drop to 0
                base_shares = prev_shares.get(cusip, rand_shares())
                drift = rng.choice([0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.2, 1.35, 0.0, 1.6])
                shares = round(base_shares * drift)
                if shares <= 0:
                    shares = 0
                    prev_shares[cusip] = 0
                    continue

                value = rand_value(shares)
                h = Holding(
                    filing_id=filing.id,
                    cusip=cusip,
                    ticker=ticker,
                    issuer_name=name,
                    title_of_class="COM",
                    value=value,
                    shares=shares,
                    sector=sector,
                )
                db.add(h)
                total_value += value
                prev_shares[cusip] = shares

            filing.total_value = total_value

        db.commit()

    print("Computing QoQ changes …")
    compute_all_qoq(db)

    print("Computing theme snapshots …")
    compute_theme_snapshots(db)

    db.close()
    print("Done! Demo data ready. Start the server with: uvicorn app:app --reload")

if __name__ == "__main__":
    main()
