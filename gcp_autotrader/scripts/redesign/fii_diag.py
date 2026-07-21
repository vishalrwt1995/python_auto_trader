"""FII/DII flow diagnostic (channel candidate #3) — is FII net index-futures positioning a market-
timing signal, and does it COMPLEMENT/BEAT the Nifty>100DMA regime gate (the high-value prize — a
better gate lifts every channel)? Data: nse_participant_oi FII/DII daily index-fut long/short
(2015-2026, pulled to scratchpad). Nifty from cached market_inputs_2015.json. No-look-ahead: OI[d]
is published EOD d → forward Nifty return measured from d+1 (act next session). Reports forward Nifty
by FII-net sign, FII-net 20d-trend, DII-net sign, and the 2x2 (Nifty>100DMA x FII-net-long) — the
key: does FII positioning separate forward returns WITHIN each price-regime? IS(<=2020)/OOS(>=2021).
READ-ONLY, cached + one scratchpad pull (zero further GCP cost)."""
import os, sys, json, statistics
from bisect import bisect_left
C=os.path.expanduser("~/.autotrader_backtest_cache")
SCRATCH=sys.argv[1] if len(sys.argv)>1 else "."
IS_END="2020-12-31"

oi=json.load(open(f"{SCRATCH}/fii_dii_oi.json"))
fii_net, dii_net = {}, {}
for r in oi:
    net=float(r["fut_idx_long"])-float(r["fut_idx_short"])
    (fii_net if r["client_type"]=="FII" else dii_net)[r["date"][:10]]=net

mkt=json.load(open(f"{C}/market_inputs_2015.json"))
md=sorted(x for x in mkt if mkt[x].get("nifty_close"))
nc=[float(mkt[x]["nifty_close"]) for x in md]
idx={d:i for i,d in enumerate(md)}
ma100=[None]*len(nc); run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100: run-=nc[i-100]
    if i>=99: ma100[i]=run/100.0

# FII net 20d trend (rising = today's net > its trailing 20d mean)
fdates=sorted(fii_net)
fpos={d:i for i,d in enumerate(fdates)}
def fii_rising(d):
    i=fpos.get(d)
    if i is None or i<20: return None
    win=[fii_net[fdates[j]] for j in range(i-20,i)]
    return fii_net[d] > sum(win)/len(win)

rows=[]
for d in fdates:
    if d not in idx: continue
    i=idx[d]
    if i+1>=len(nc) or ma100[i] is None: continue
    e=i+1                                   # act next session (no look-ahead)
    def fwd(k): return (nc[e+k]/nc[e]-1.0) if e+k<len(nc) and nc[e]>0 else None
    rows.append({"d":d,"fii":fii_net[d],"dii":dii_net.get(d),"rising":fii_rising(d),
                 "nifty_up":nc[i]>ma100[i],"f5":fwd(5),"f10":fwd(10),"f20":fwd(20)})
print(f"{len(rows)} aligned days (FII OI + Nifty), 2015-2026\n", flush=True)

def m(pool,k):
    v=[r[k] for r in pool if r[k] is not None]
    if not v: return "  n/a  "
    return f"avg={statistics.mean(v)*100:+5.2f}% hit={100*sum(1 for x in v if x>0)/len(v):4.1f}% n={len(v)}"
def report(name,pred):
    for lbl,lo,hi in [("IS ","0000",IS_END),("OOS",IS_END,"9999")]:
        sub=[r for r in rows if pred(r) and lo<r["d"]<=hi]
        print(f"  {name:32} {lbl}: f5 {m(sub,'f5')}  f20 {m(sub,'f20')}", flush=True)

print("=== BASELINE (all days) — unconditional forward Nifty ===", flush=True)
report("all days", lambda r: True)
print("\n=== by FII net index-fut sign ===", flush=True)
report("FII net LONG (>0)", lambda r: r["fii"]>0)
report("FII net SHORT (<0)", lambda r: r["fii"]<0)
print("\n=== by FII net 20d-trend (positioning momentum) ===", flush=True)
report("FII net RISING (>20dMA)", lambda r: r["rising"] is True)
report("FII net FALLING (<20dMA)", lambda r: r["rising"] is False)
print("\n=== by DII net sign (FII counterparty) ===", flush=True)
report("DII net LONG (>0)", lambda r: r["dii"] is not None and r["dii"]>0)
report("DII net SHORT (<0)", lambda r: r["dii"] is not None and r["dii"]<0)
print("\n=== 2x2: Nifty>100DMA x FII-net-long (does FII ADD to the price gate?) ===", flush=True)
report("Nifty UP  + FII long", lambda r: r["nifty_up"] and r["fii"]>0)
report("Nifty UP  + FII short", lambda r: r["nifty_up"] and r["fii"]<0)
report("Nifty DOWN + FII long", lambda r: not r["nifty_up"] and r["fii"]>0)
report("Nifty DOWN + FII short", lambda r: not r["nifty_up"] and r["fii"]<0)
print("\nREAD: signal is real ONLY if a bucket beats baseline consistently IN BOTH halves. For the", flush=True)
print("regime-overlay prize: within 'Nifty UP', does FII-long beat FII-short (adds info beyond price)?", flush=True)
