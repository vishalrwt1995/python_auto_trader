"""COMPLETENESS sweep — before claiming the ceiling, test every dimension held fixed so far, one at a
time vs the reference config, each judged on BOTH-half robustness (IS Cal & OOS Cal), not full-sample.
Dims: (1) macro-GATE variants, (2) STOP multiple, (3) EXIT style (fixed vs trailing vs 50DMA-break),
(4) universe turnover floor, (5) person category. Reference = gated-double + px>200DMA + hold60 +
cap10% + risk1.5% + stop2.5 + fixed-exit. A challenger only 'wins' if it beats ref on min(IS,OOS)
Calmar AND keeps >=~10 tr/yr. Survivorship-safe, real Upstox cost. READ-ONLY, single-process, cached."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import sys, json, glob, pickle
from bisect import bisect_right, bisect_left
from datetime import datetime
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig
C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
UPSTOX=CostConfig.upstox(); CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
PRICE_MIN,MAXSLOTS=30.0,20
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
def dd_of(r):
    s=str(r.get("date","")).split()[0] if r.get("date") else ""
    try:return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception:return None
print("loading + featurizing ...",flush=True)
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
    SYM[s]={"d":d,"o":o,"c":c,"atr":atr14(h,l,c),"turn":turn,"s50":sma(c,50),"s200":sma(c,200)}
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
    except Exception:pass
# rich pool: all promoter pledge-revokes, price>=30, turn>=5cr, px>200DMA, with attributes
pool=[]
for r in recs:
    if "revoke" not in str(r.get("tdpTransactionType","")).lower():continue
    cat=str(r.get("personCategory","")).lower()
    if "promoter" not in cat:continue
    sym=str(r.get("symbol") or "").strip().upper();dd=dd_of(r);S=SYM.get(sym)
    if not S or not dd:continue
    ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0:continue
    if S["turn"][ref] is None or S["turn"][ref]<5e7 or S["o"][ref]<PRICE_MIN:continue
    if not (S["s200"][ref] is not None and S["c"][ref]>S["s200"][ref]):continue
    ed=S["d"][ref]
    pool.append({"ed":ed,"sym":sym,"ref":ref,"atrp":S["atr"][ref-1],"turn":S["turn"][ref],
                 "b200":b200_at(ed),"nok":nifty_ok(ed),"grp":("group" in cat)})
pool.sort(key=lambda x:x["ed"])
print(f"  {len(pool)} pooled promoter pledge-revoke (px>200DMA, turn>=5cr)\n",flush=True)

def walk(cands, exit_mode="fixed", hold=60, stop_mult=2.5, trail_pct=0.15, turn_min=10e7, cap_pct=0.10, risk_pct=0.015, gate="double", cat="any"):
    sub=[]
    for c in cands:
        if c["turn"]<turn_min:continue
        if cat=="pg" and not c["grp"]:continue
        if cat=="p" and c["grp"]:continue
        if gate=="double" and not(c["b200"]>50 and c["nok"]):continue
        if gate=="nifty" and not c["nok"]:continue
        if gate=="b200" and not c["b200"]>50:continue
        if gate=="b40" and not(c["b200"]>40 and c["nok"]):continue
        if gate=="b60" and not(c["b200"]>60 and c["nok"]):continue
        if gate=="none":pass
        sub.append(c)
    equity=CAP0;openp=[];closed=[];CAPBAR=250
    for c in sub:
        ed=c["ed"];still=[]
        for xd,pnl,notl in openp:
            if xd<=ed:equity+=pnl;closed.append((xd,pnl))
            else:still.append((xd,pnl,notl))
        openp=still
        committed=sum(n for _,_,n in openp)
        if len(openp)>=MAXSLOTS:continue
        S=SYM[c["sym"]];ref=c["ref"];epx=S["o"][ref]
        if epx<=0:continue
        room=equity-committed
        if room<=epx:continue
        sl=stop_mult*c["atrp"];stop_px=epx-sl
        cap_amt=min(risk_pct*equity/sl*epx,cap_pct*equity,room);qty=int(cap_amt//epx)
        if qty<1:continue
        end=min(ref+(hold if exit_mode=="fixed" else CAPBAR),len(S["c"])-1)
        xpx=S["c"][end];xi=end;peak=epx
        for k in range(ref+1,end+1):
            cl=S["c"][k]
            if cl<=stop_px:xpx=stop_px;xi=k;break
            if exit_mode=="fixed" and k==ref+hold:xpx=cl;xi=k;break
            if exit_mode=="trail":
                peak=max(peak,cl)
                if cl<=peak*(1-trail_pct):xpx=cl;xi=k;break
            if exit_mode=="ma50" and S["s50"][k] is not None and cl<S["s50"][k]:xpx=cl;xi=k;break
        xd=S["d"][xi];ef=epx*(1+SLIP);xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)+compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        openp.append((xd,pnl,qty*epx))
    for xd,pnl,notl in openp:equity+=pnl;closed.append((xd,pnl))
    if len(closed)<20:return None
    def met(cl):
        if len(cl)<10:return None
        eq=CAP0;peak=CAP0;mdd=0.0
        for xd,pnl in cl:eq+=pnl;peak=max(peak,eq);mdd=min(mdd,eq/peak-1)
        span=max(1,int(cl[-1][0][:4])-int(cl[0][0][:4])+1)
        return dict(cagr=(eq/CAP0)**(1/span)-1,mdd=mdd,cal=((eq/CAP0)**(1/span)-1)/abs(mdd) if mdd<0 else 0,eq=eq,n=len(cl),span=span)
    closed.sort()
    f=met(closed);a=met([x for x in closed if x[0]<=IS_END]);z=met([x for x in closed if x[0]>IS_END])
    return f,a,z
REF=dict(exit_mode="fixed",hold=60,stop_mult=2.5,turn_min=10e7,gate="double",cat="any")
def row(name,**kw):
    p=dict(REF);p.update(kw);res=walk(pool,**p)
    if not res:print(f"  {name:30} (too few)");return None
    f,a,z=res
    if not(f and a and z):print(f"  {name:30} (half too thin)");return None
    mn=min(a['cal'],z['cal'])
    win="  <-- beats ref" if name!="REFERENCE" and mn>REF_MIN+0.03 else ""
    print(f"  {name:30} CAGR{f['cagr']*100:>+6.1f}% DD{f['mdd']*100:>6.1f}% Cal{f['cal']:>5.2f} | IS Cal{a['cal']:>5.2f} OOS Cal{z['cal']:>5.2f} minHalf{mn:>5.2f} | {f['n']/f['span']:>4.0f}/y{win}",flush=True)
    return mn
_,ra,rz=walk(pool,**REF); REF_MIN=min(ra['cal'],rz['cal'])
print(f"=== REFERENCE min-half Calmar = {REF_MIN:.2f} (challengers must beat this by >0.03 robustly) ===\n",flush=True)
print("REFERENCE",);row("REFERENCE")
print("\n--- (1) MACRO-GATE variants ---",flush=True)
for g,lbl in [("nifty","nifty>100DMA only"),("b200","b200>50 only"),("b40","b200>40 & nifty"),("b60","b200>60 & nifty"),("none","NO gate")]:
    row(f"gate={lbl}",gate=g)
print("\n--- (2) STOP multiple ---",flush=True)
for sm in (1.5,2.0,3.0,4.0,99.0):
    row(f"stop={('none' if sm>50 else sm)}xATR",stop_mult=sm)
print("\n--- (3) EXIT style ---",flush=True)
row("exit=trail 12%",exit_mode="trail",trail_pct=0.12)
row("exit=trail 18%",exit_mode="trail",trail_pct=0.18)
row("exit=trail 25%",exit_mode="trail",trail_pct=0.25)
row("exit=50DMA-break",exit_mode="ma50")
print("\n--- (4) universe turnover floor ---",flush=True)
for t,lbl in [(5e7,"5cr"),(25e7,"25cr"),(50e7,"50cr")]:
    row(f"turn>={lbl}",turn_min=t)
print("\n--- (5) person category ---",flush=True)
row("promoters-only (not group)",cat="p")
row("promoter-group-only",cat="pg")
print("\nREAD: if nothing robustly beats REFERENCE min-half Calmar, the locked config IS the ceiling.",flush=True)
