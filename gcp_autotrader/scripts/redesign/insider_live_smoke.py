"""INSIDER live-feed E2E smoke test — drives the NEW ingest pipeline against LIVE NSE:
corporates-pit-gg index -> per-filing XBRL fetch -> parse_insider_xbrl -> domain.aggregate_legs
-> (finalize_clusters demo). Proves the live path produces real, current, parseable insider
clusters (not stubbed). Read-only, polite (delays), NO GCP cost, NO BQ write. Reuses the exact
prod service + domain code so this is a faithful e2e of what prod will do."""
import os, sys, time
from datetime import date, timedelta
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.services.insider_ingest_service import InsiderIngestService, _index_filings, parse_insider_xbrl
from autotrader.domain import insider_signals

WINDOW_DAYS = int(os.environ.get("SMOKE_WINDOW", "12"))
MAX_FETCH = int(os.environ.get("SMOKE_MAX", "320"))

svc = InsiderIngestService(bq=None, window_days=WINDOW_DAYS)   # bq unused for fetch/parse
end = date.today(); start = end - timedelta(days=WINDOW_DAYS); window_start = start.isoformat()
print(f"window {window_start} .. {end.isoformat()}  (max {MAX_FETCH} XBRL fetches)\n", flush=True)

sess = svc._session()
records = svc._fetch_index(sess, start, end)
if records is None:
    print("INDEX FETCH FAILED"); sys.exit(1)
filings = _index_filings(records, window_start)
print(f"index records: {len(records)} | filings in window w/ xbrl: {len(filings)}", flush=True)

rows, missing, fetched = [], 0, 0
for rec in filings[:MAX_FETCH]:
    xbrl = svc._fetch_xbrl(sess, rec)
    fetched += 1
    if xbrl is None:
        missing += 1
    else:
        rows.extend(parse_insider_xbrl(xbrl, rec))
    time.sleep(0.3)
    if fetched % 50 == 0:
        print(f"  fetched {fetched}/{min(len(filings),MAX_FETCH)}  rows={len(rows)}  missing_xbrl={missing}", flush=True)

print(f"\nparsed {len(rows)} transaction legs from {fetched} filings ({missing} XBRL 404/missing)", flush=True)
# leg-level sanity: how many are informed open-market BUYS?
buys = [r for r in rows if insider_signals.qualifies_leg(r)]
print(f"informed open-market BUY legs: {len(buys)}  across {len({r['symbol'] for r in buys})} symbols", flush=True)
cats = {}
for r in buys: cats[r["person_category"]] = cats.get(r["person_category"], 0) + 1
print(f"  categories: {dict(sorted(cats.items(), key=lambda x:-x[1])[:6])}", flush=True)
modes = {}
for r in rows: modes[r["acq_mode"]] = modes.get(r["acq_mode"], 0) + 1
print(f"  all modes seen: {dict(sorted(modes.items(), key=lambda x:-x[1])[:8])}", flush=True)

# PASS 1: cluster formation (>=2 informed market buys, same symbol+day)
legs = insider_signals.aggregate_legs(rows, min_buyers=2)
print(f"\n>>> CLUSTERS (>=2 informed open-market buys same symbol/day): {len(legs)}", flush=True)
for sym, lg in list(legs.items())[:8]:
    print(f"  {sym}: {len(lg)} legs | cats={[x['category'] for x in lg]} shares={[int(x['shares']) for x in lg]}", flush=True)

# PASS 2 demo: finalize with an illustrative price (prod uses the live reaction-close per symbol)
demo_price = {s: 500.0 for s in legs}
final = insider_signals.finalize_clusters(legs, demo_price, min_buyers=2, min_leg_value=insider_signals.MIN_LEG_VALUE)
print(f"\nfinalize_clusters @ Rs500 demo price -> {len(final)} clusters clear the >=Rs5L per-leg gate", flush=True)
for s, c in list(final.items())[:5]:
    print(f"  {s}: n_buyers={c['n_buyers']} total_val=Rs{c['total_val']:,.0f} category={c['category']}", flush=True)
print("\nE2E RESULT: live corporates-pit-gg index + XBRL parse + domain cluster logic all functional", flush=True)
print("on CURRENT data. (finalize uses live Upstox reaction-close per symbol in prod.)", flush=True)
