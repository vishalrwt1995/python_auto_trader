"""VALIDATE the insider-overlay lead: does adding a STOCK px>Ntrend filter improve the LIVE insider
CLUSTER channel's PORTFOLIO metrics (not just event-level f60)? Replicates the validated insider config
(cluster >=2 informed open-market buys same day, value=shares×entry-close>=Rs5L, DOUBLE MACRO GATE
b200>50 & Nifty>100DMA, turnover>=10cr, price>=30, hold 90d, ATR14×2.5 stop, 10 slots, 1.5% risk,
notional-cap 10%, rank by n_buyers). Runs the walk with NO stock-trend filter (= live channel) vs
+px>50/100/200DMA. Reports CAGR/maxDD/Calmar/trades-per-yr, IS(<=2020)/OOS(>=2021). Win = the filter
improves Calmar in BOTH halves WITHOUT over-thinning frequency (a real additive channel improvement),
across a trend-length plateau. Survivorship-safe (pead_full_bars_2014). READ-ONLY, cached, single-process."""
import os, sys, json, glob, pickle
from bisect import bisect_right, bisect_left
from datetime import datetime
from collections import defaultdict
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig
C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
UPSTOX=CostConfig.upstox(); CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
TURN_MIN,PRICE_MIN,ATR_MULT,RISK_PCT,CAPPCT,B200_MIN=10e7,30.0,2.5,0.015,0.10,50.0
HOLD,SLOTS,MIN_BUYERS,MIN_LEG_VAL=90,10,2,5e5
INF=("promoter","director","key managerial","managerial","immediate relative","relative")

def sma(c,n):
    out=[None]*len(c);s=0.0
    for i in range(len(c)):
        s+=c[i]
        if i>=n:s-=c[i-n]
        if i>=n-1:out[i]=s/n
    return out
def atr14(h,l,c):
    tr=[h[0]-l[0]]
    for i in range(1,len(c)):tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    out=[None]*len(c);s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14:s-=tr[i-14]
        if i>=13:out[i]=s/14.0
    return out
def fnum(x):
    try:return float(str(x).replace(",",""))
    except:return None
def dd_of(r):
    s=str(r.get("date","")).split()[0] if r.get("date") else ""
    try:return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except:return None
print("loading ...",flush=True)
bars=pickle.load(open(f"{C}/pead_full_bars_2014.pkl","rb"))
SYM={}
for s,b in bars.items():
    if len(b)<210:continue
    d=[x[0] for x in b];o=[x[1] for x in b];h=[x[2] for x in b];l=[x[3] for x in b];c=[x[4] for x in b];v=[x[5] for x in b]
    turn=[None]*len(c);run=0.0
    for i in range(len(c)):
        if i>=1:run+=c[i-1]*v[i-1]
        if i>=21:run-=c[i-21]*v[i-21]
        if i>=21:turn[i]=run/20.0
    SYM[s]={"d":d,"o":o,"c":c,"atr":atr14(h,l,c),"turn":turn,"s50":sma(c,50),"s100":sma(c,100),"s200":sma(c,200)}
b200h=pickle.load(open(f"{C}/swing_b200_history.pkl","rb"));bdd=sorted(b200h.keys())
mkt=json.load(open(f"{C}/market_inputs_2015.json"));md=sorted(x for x in mkt if mkt[x].get("nifty_close"));nc=[float(mkt[x]["nifty_close"]) for x in md]
ma=[None]*len(nc);run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100:run-=nc[i-100]
    if i>=99:ma[i]=run/100.0
def nifty_ok(dt):
    i=bisect_left(md,dt)-1;return i<0 or ma[i] is None or nc[i]>ma[i]
def b200_at(dt):
    i=bisect_right(bdd,dt)-1;return b200h[bdd[i]] if i>=0 else 0.0
recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try:recs.extend(json.load(open(fn)))
    except:pass
# group informed open-market buy legs by (symbol, disclosure-day)
legs=defaultdict(list)
for r in recs:
    t=str(r.get("tdpTransactionType","")).lower();cat=str(r.get("personCategory","")).lower();m=str(r.get("acqMode","")).lower()
    if "buy" in t and any(k in cat for k in INF) and "market" in m and "off" not in m:
        sh=fnum(r.get("secAcq")) or 0.0; dd=dd_of(r); sym=str(r.get("symbol") or "").strip().upper()
        if sh>0 and dd and sym: legs[(sym,dd)].append(sh)
cands=[]
for (sym,dd),ls in legs.items():
    if len(ls)<MIN_BUYERS:continue
    S=SYM.get(sym)
    if not S:continue
    ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0:continue
    if S["turn"][ref] is None or S["turn"][ref]<TURN_MIN or S["o"][ref]<PRICE_MIN:continue
    epx=S["o"][ref]; ec=S["c"][ref]
    kept=[x for x in ls if x*ec>=MIN_LEG_VAL]
    if len(kept)<MIN_BUYERS:continue
    ed=S["d"][ref]
    cands.append({"ed":ed,"sym":sym,"ref":ref,"sl":ATR_MULT*S["atr"][ref-1],"n":len(kept),
                  "b200":b200_at(ed),"nok":nifty_ok(ed),
                  "a50":S["s50"][ref] is not None and ec>S["s50"][ref],
                  "a100":S["s100"][ref] is not None and ec>S["s100"][ref],
                  "a200":S["s200"][ref] is not None and ec>S["s200"][ref]})
cands.sort(key=lambda x:(x["ed"],-x["n"]))
print(f"  {len(cands)} insider cluster candidates (>=2 informed buys, value-gated)\n",flush=True)

def walk(trend):   # trend in (None,'a50','a100','a200')
    equity=CAP0;openp=[];closed=[]
    for c in cands:
        if not(c["b200"]>B200_MIN and c["nok"]):continue
        if trend and not c[trend]:continue
        ed=c["ed"];still=[]
        for xd,pnl,notl in openp:
            if xd<=ed:equity+=pnl;closed.append((xd,pnl))
            else:still.append((xd,pnl,notl))
        openp=still
        committed=sum(n for _,_,n in openp)
        if len(openp)>=SLOTS:continue
        S=SYM[c["sym"]];ref=c["ref"];epx=S["o"][ref]
        if epx<=0:continue
        room=equity-committed
        if room<=epx:continue
        qty=int(min(RISK_PCT*equity/c["sl"]*epx, CAPPCT*equity, room)//epx)
        if qty<1:continue
        xi=min(ref+HOLD,len(S["c"])-1);xpx=S["c"][xi];stop=epx-c["sl"]
        for k in range(ref+1,xi+1):
            if k<len(S["c"]) and S["c"][k]<=stop:xpx=stop;xi=k;break
        xd=S["d"][xi];ef=epx*(1+SLIP);xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)+compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        openp.append((xd,pnl,qty*epx))
    for xd,pnl,notl in openp:equity+=pnl;closed.append((xd,pnl))
    if len(closed)<10:return None
    def met(cl):
        if len(cl)<5:return None
        eq=CAP0;peak=CAP0;mdd=0.0
        for xd,pnl in cl:eq+=pnl;peak=max(peak,eq);mdd=min(mdd,eq/peak-1)
        span=max(1,int(cl[-1][0][:4])-int(cl[0][0][:4])+1)
        return dict(cagr=(eq/CAP0)**(1/span)-1,mdd=mdd,cal=((eq/CAP0)**(1/span)-1)/abs(mdd) if mdd<0 else 0,n=len(cl),span=span)
    closed.sort()
    return met(closed),met([x for x in closed if x[0]<=IS_END]),met([x for x in closed if x[0]>IS_END])
print(f"  {'filter':14}{'FULL cagr/DD/Cal':>26}{'IS cagr/Cal':>16}{'OOS cagr/Cal':>17}{'tr/yr':>7}",flush=True)
for trend,lbl in [(None,"NONE (live)"),("a50","+px>50DMA"),("a100","+px>100DMA"),("a200","+px>200DMA")]:
    r=walk(trend)
    if not r or not r[0]:print(f"  {lbl:14} thin");continue
    f,a,z=r
    fs=f"{f['cagr']*100:+.0f}%/{f['mdd']*100:.0f}%/{f['cal']:.2f}"
    is_=f"{a['cagr']*100:+.0f}%/{a['cal']:.2f}" if a else "n/a"
    os_=f"{z['cagr']*100:+.0f}%/{z['cal']:.2f}" if z else "n/a"
    print(f"  {lbl:14}{fs:>26}{is_:>16}{os_:>17}{f['n']/f['span']:>6.0f}",flush=True)
print("\nREAD: baseline (NONE) should ~reproduce the validated insider walk. A trend filter WINS only if it", flush=True)
print("lifts Calmar in BOTH IS and OOS across the 50/100/200 plateau without cutting tr/yr too hard.", flush=True)
