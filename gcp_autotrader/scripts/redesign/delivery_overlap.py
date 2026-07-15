"""DELIVERY diversification check — name + concurrent-holding overlap vs existing channels.

Is delivery a genuine diversifier (earns its own capital) or does it double-book bets already
made by momentum / core / pead? Regenerates each channel's positions from its OWN prod-faithful
logic (reuses domain/momentum_signals, core_signals, pead_signals — no replica) and measures:
  1) turnover tier      — delivery (25-50cr mid-cap) vs the others (should be structurally lower)
  2) name overlap       — |delivery names ∩ channel names| / |delivery names|
  3) concurrent overlap — % of delivery positions where the SAME name is held by a channel during
                          an overlapping date window (the true double-booking metric)
Swing handled by argument (heavy engine; different signal + regime-gated) + live-confirm — see notes.
Delivery/momentum use pead_full_bars_2014; core/pead use swing_adj_bars_2015 (symbols comparable).
READ-ONLY, single-process, thread-capped. No prod/existing-backtest file touched."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, json, pickle
from math import sqrt
from bisect import bisect_right, bisect_left
from statistics import mean, median
from collections import defaultdict
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain import momentum_signals as ms
from autotrader.domain.core_signals import momentum_score as c_mom, realized_vol as c_vol, passes_universe_gates as c_gates, UNIVERSE_TOP
from autotrader.domain.pead_signals import earnings_surprise, pre_event_runup, passes_pead_gates, ANTI_PUMP_LOOKBACK, MAX_HOLD_DAYS, ATR_SL_MULT as PEAD_ATR
from autotrader.domain.swing_exit import simulate_exit

GC = os.path.expanduser("~/.autotrader_grind_cache"); C = os.path.expanduser("~/.autotrader_backtest_cache")
def atr14(b):
    h=[x[2] for x in b]; l=[x[3] for x in b]; c=[x[4] for x in b]; tr=[h[0]-l[0]]
    for i in range(1,len(c)): tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    o=[None]*len(c); s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14: s-=tr[i-14]
        if i>=13: o[i]=s/14.0
    return o

# ══ Phase A: pead_full_bars_2014 → delivery positions + momentum holdings ══
print("Phase A: delivery + momentum (pead_full_bars_2014) ...", flush=True)
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
MD = sorted(d for d in mkt if mkt[d].get("nifty_close")); NIFTY=[float(mkt[d]["nifty_close"]) for d in MD]
deliv = pickle.load(open(f"{GC}/delivery.pkl", "rb"))

SYM={}
for s,b in bars.items():
    if not b or len(b)<300: continue
    c_=[float(x[4]) for x in b]; v_=[float(x[5]) for x in b]
    r_=[0.0]*len(c_)
    for k in range(1,len(c_)): r_[k]=(c_[k]/c_[k-1]-1.0) if c_[k-1]>0 else 0.0
    SYM[s]={"d":[x[0] for x in b],"o":[float(x[1]) for x in b],"c":c_,"v":v_,"r":r_,
            "atr":atr14(b),"bd":{x[0]:i for i,x in enumerate(b)}}

# delivery positions (confirmed config: deliv>=75 & ret5<=0, 25-50cr, hold10, 5 slots)
POOL=[]
for sym,dl in deliv.items():
    S=SYM.get(sym)
    if S is None: continue
    c,v,bd=S["c"],S["v"],S["bd"]
    for (d,pct,qty,ttl) in dl:
        if d not in bd: continue
        i=bd[d]
        if i<20 or i+1>=len(c) or c[i]<30.0: continue
        turn=mean(c[j]*v[j] for j in range(i-20,i))
        if turn<2.5e8 or turn>=5e8: continue
        if pct<75: continue                      # CORRECTED config: deliv>=75 only (no dip filter)
        a=S["atr"][i]
        if not a or a<=0: continue
        POOL.append((d,sym,i,a,turn))
POOL.sort()
SLOTS=5; free=[""]*SLOTS; deliv_pos=[]; deliv_turn=[]
for (d,sym,i,a,turn) in POOL:
    slot=next((k for k in range(SLOTS) if free[k]<=d),None)
    if slot is None: continue
    S=SYM[sym]; ei=i+1
    off,_,_=simulate_exit(S["b"] if "b" in S else bars[sym],ei,True,2.5*a,20,trail_R=1.0,activate_R=1.75)
    xi=min(ei+off,len(S["d"])-1); free[slot]=S["d"][xi]
    deliv_pos.append((sym,S["d"][ei],S["d"][xi])); deliv_turn.append(turn/1e7)
print(f"  delivery positions: {len(deliv_pos)} | unique syms {len({p[0] for p in deliv_pos})}", flush=True)

# momentum monthly holdings (union of baskets w/ overlay) → windows per sym
for s,S in SYM.items(): S["b"]=bars[s]   # simulate_exit needs raw bars; attach ref
MONTH_END=[MD[i-1] for i in range(1,len(MD)) if MD[i][:7]!=MD[i-1][:7]]+[MD[-1]]
REBAL=[d for d in MONTH_END if d>="2020-01-01"]
def m_idx_le(S,dt):
    i=bisect_right(S["d"],dt)-1; return i if i>=0 else None
_panel={}
def m_cands(dt):
    if dt in _panel: return _panel[dt]
    out=[]
    for s,S in SYM.items():
        i=m_idx_le(S,dt)
        if i is None: continue
        mom=ms.momentum_score(S["c"],i); vol=ms.realized_vol(S["r"],i); tmed=ms.median_turnover(S["c"],S["v"],i)
        if not ms.passes_universe_gates(S["c"][i],tmed,mom is not None and vol is not None): continue
        out.append({"symbol":s,"momentum":mom,"vol":vol,"turnover":tmed})
    _panel[dt]=out; return out
def m_nifty_ok(dt):
    i=bisect_right(MD,dt)-1
    return ms.nifty_regime_ok(NIFTY[:i+1],ma_window=100) if i>=0 else False
mom_win=defaultdict(list); mom_turn=[]; prev=[]
for k,rd in enumerate(REBAL):
    nrd=REBAL[k+1] if k+1<len(REBAL) else MD[-1]
    if not m_nifty_ok(rd):
        prev=[]; continue
    basket=ms.rank_blend_select(m_cands(rd),prev_holds=prev,topn=20,buffer_mult=1.5,regime_ok=True)
    tmap={c["symbol"]:c["turnover"] for c in m_cands(rd)}
    for s in basket:
        mom_win[s].append((rd,nrd)); mom_turn.append(tmap.get(s,0)/1e7)
    prev=list(basket)
print(f"  momentum: unique syms {len(mom_win)} | median turnover {median(mom_turn):.0f}cr", flush=True)
del bars, SYM, _panel

# ══ Phase B: swing_adj_bars_2015 → core + pead ══
print("Phase B: core + pead (swing_adj_bars_2015) ...", flush=True)
raw=pickle.load(open(f"{C}/swing_adj_bars_2015.pkl","rb"))
SY={}
for s,b in raw.items():
    if not b or len(b)<270: continue
    d=[x[0] for x in b]; o=[float(x[1]) for x in b]; hi=[float(x[2]) for x in b]
    lo=[float(x[3]) for x in b]; cl=[float(x[4]) for x in b]; vo=[float(x[5]) for x in b]
    rets=[0.0]+[(cl[i]/cl[i-1]-1.0) if cl[i-1]>0 else 0.0 for i in range(1,len(cl))]
    tov=[]; rs=0.0; w=[]
    for i in range(len(cl)):
        x=cl[i]*vo[i]; w.append(x); rs+=x
        if len(w)>20: rs-=w.pop(0)
        tov.append(rs/len(w))
    SY[s]={"d":d,"o":o,"hi":hi,"lo":lo,"c":cl,"r":rets,"tov":tov,"atr":atr14(b),"bars":b,"bd":{dt:i for i,dt in enumerate(d)}}

# CORE quarterly top-30 blend (m12xlowvol, large-cap) → windows
alld=sorted({d for v in SY.values() for d in v["d"]})
def cfirst(iso):
    k=bisect_left(alld,iso); return alld[k] if k<len(alld) else None
core_reb=[d for d in sorted({cfirst(f"{y}-{m:02d}-01") for y in range(2020,2027) for m in (1,4,7,10)}-{None}) if d]
def c_i_le(s,d):
    j=bisect_right(SY[s]["d"],d)-1; return j if j>=0 else None
_ccache={}
def c_cand(d):
    if d in _ccache: return _ccache[d]
    out=[]
    for s,v in SY.items():
        i=c_i_le(s,d)
        if i is None or i-252<0: continue
        if not c_gates(v["c"][i],v["tov"][i],has_history=True): continue
        m12=c_mom(v["c"],i,252,21); vol=c_vol(v["r"],i,60)
        if m12 is None or vol is None or vol<=0: continue
        out.append({"symbol":s,"m12":m12,"vol":vol,"turnover":v["tov"][i]})
    _ccache[d]=out; return out
def c_select(cands,topn=30):
    large=sorted(cands,key=lambda c:-c["turnover"])[:UNIVERSE_TOP]
    if len(large)<topn: return []
    mr={c["symbol"]:r for r,c in enumerate(sorted(large,key=lambda c:-c["m12"]))}
    vr={c["symbol"]:r for r,c in enumerate(sorted(large,key=lambda c:c["vol"]))}
    return sorted(large,key=lambda c:mr[c["symbol"]]+vr[c["symbol"]])[:topn]
core_win=defaultdict(list); core_turn=[]
for k,rd in enumerate(core_reb):
    nrd=core_reb[k+1] if k+1<len(core_reb) else alld[-1]
    basket=c_select(c_cand(rd))
    for c in basket:
        core_win[c["symbol"]].append((rd,nrd)); core_turn.append(c["turnover"]/1e7)
print(f"  core: unique syms {len(core_win)} | median turnover {median(core_turn) if core_turn else 0:.0f}cr", flush=True)

# PEAD gate-passing trades (superset — overstates overlap, safe direction) → windows
ev=json.load(open(f"{C}/pead_nse_result_dates_2012_2026.json"))["events"]
mdates=sorted(d for d in mkt if mkt[d].get("nifty_close")); nif=[float(mkt[d]["nifty_close"]) for d in mdates]
pk=-1e18; ddbd={}
for d,v in zip(mdates,nif): pk=max(pk,v); ddbd[d]=v/pk-1.0 if pk>0 else 0.0
def mkt_dd(d):
    i=bisect_left(mdates,d)-1; return ddbd[mdates[i]] if i>=0 else None
pead_win=defaultdict(list)
for e in ev:
    sy=SY.get(e["symbol"])
    if sy is None or e["date"]<"2020-01-01": continue
    dl=sy["d"]; ri=bisect_left(dl,e["date"])
    if ri>=len(dl) or ri<ANTI_PUMP_LOOKBACK+1 or ri+1>=len(sy["c"]): continue
    sp=earnings_surprise(sy["c"],ri); ru=pre_event_runup(sy["c"],ri); md=mkt_dd(dl[ri])
    if not passes_pead_gates(sp,ru,md): continue
    a=sy["atr"][ri]
    if not a or a<=0: continue
    ei=ri+1; off,_,_=simulate_exit(sy["bars"],ei,True,PEAD_ATR*a,MAX_HOLD_DAYS,trail_R=1.0,activate_R=1.75)
    xi=min(ei+off,len(dl)-1); pead_win[e["symbol"]].append((dl[ei],dl[xi]))
print(f"  pead: unique syms {len(pead_win)} (gate-passing superset)", flush=True)
del raw, SY

# ══ Phase C: overlap metrics ══
def overlap(name, win):
    dsyms={p[0] for p in deliv_pos}
    name_ov=len(dsyms & set(win.keys()))
    conc=0
    for (sym,de,dx) in deliv_pos:
        for (ce,cx) in win.get(sym,[]):
            if ce<=dx and de<=cx: conc+=1; break
    print(f"  {name:10} name-overlap {name_ov:>3}/{len(dsyms)} ({100*name_ov/len(dsyms):4.1f}%)  "
          f"concurrent {conc:>3}/{len(deliv_pos)} ({100*conc/len(deliv_pos):4.1f}%)", flush=True)

print("\n=== TURNOVER TIER (median, cr) ===", flush=True)
print(f"  delivery {median(deliv_turn):.0f}cr | momentum {median(mom_turn):.0f}cr | core {median(core_turn) if core_turn else 0:.0f}cr", flush=True)
print("=== OVERLAP vs delivery (name = ever-traded; concurrent = same name held same window) ===", flush=True)
overlap("momentum", mom_win); overlap("core", core_win); overlap("pead", pead_win)
print("\nLow name + concurrent overlap = genuine diversifier (own capital justified). Swing: heavy engine,"
      "\ndifferent signal (MOM/PB/MR, regime-gated, TREND_UP), skew larger-cap → confirm concurrency live.", flush=True)
