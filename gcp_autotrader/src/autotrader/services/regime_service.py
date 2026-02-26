from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date as date_cls, datetime, timedelta
from typing import Any

import httpx

from autotrader.adapters.upstox_client import UpstoxClient
from autotrader.domain.indicators import compute_indicators, normalize_candles
from autotrader.domain.models import (
    Bias,
    FiiDiiSnapshot,
    FreshnessSnapshot,
    NiftySnapshot,
    NiftyStructureSnapshot,
    PcrSnapshot,
    RegimeSnapshot,
)
from autotrader.settings import StrategySettings
from autotrader.time_utils import IST, now_ist, parse_any_ts

logger = logging.getLogger(__name__)


@dataclass
class MarketRegimeService:
    upstox: UpstoxClient
    cfg: StrategySettings
    _last_fii_dii: FiiDiiSnapshot | None = field(default=None, init=False, repr=False)
    _last_fii_dii_fetch_ts: datetime | None = field(default=None, init=False, repr=False)
    _last_nifty_structure: NiftyStructureSnapshot | None = field(default=None, init=False, repr=False)
    _last_nifty_structure_fetch_ts: datetime | None = field(default=None, init=False, repr=False)
    _last_regime_key: str = field(default="", init=False, repr=False)
    _last_regime_ts: datetime | None = field(default=None, init=False, repr=False)

    @staticmethod
    def _to_float(v: object) -> float:
        try:
            if isinstance(v, str):
                s = v.strip().replace(",", "")
                if not s:
                    return 0.0
                return float(s)
            return float(v)
        except Exception:
            return 0.0

    @staticmethod
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, float(v)))

    @staticmethod
    def _parse_iso_date(v: str) -> date_cls | None:
        s = str(v or "").strip()
        if not s:
            return None
        # NSE and broker payloads use mixed date formats.
        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d-%b-%Y",
            "%d-%b-%y",
            "%d %b %Y",
            "%d/%m/%Y",
            "%d-%m-%Y",
        ):
            try:
                # For datetime formats we trim to the expected length; for date formats full string is fine.
                probe = s[:19] if "H" in fmt or "T" in fmt else s
                return datetime.strptime(probe, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _session_phase(now_i: datetime) -> str:
        if now_i.weekday() >= 5:
            return "WEEKEND"
        mins = now_i.hour * 60 + now_i.minute
        if mins < 540:
            return "PRE_OPEN_PREP"
        if mins < 555:
            return "PRE_OPEN"
        if mins < 615:
            return "OPENING"
        if mins <= 930:
            return "REGULAR"
        if mins <= 1080:
            return "POST_CLOSE"
        return "OFF_HOURS"

    @staticmethod
    def _safe_age_sec(now_i: datetime, ts: datetime | None) -> float:
        if ts is None:
            return 999999.0
        try:
            return max(0.0, (now_i - ts.astimezone(IST)).total_seconds())
        except Exception:
            return 999999.0

    @staticmethod
    def _sort_candles_asc(candles: list[list[Any]]) -> list[list[Any]]:
        def _key(c: list[Any]) -> tuple[int, str]:
            ts = parse_any_ts(c[0] if c else None)
            if ts is None:
                return (0, str(c[0] if c else ""))
            return (1, ts.astimezone(IST).isoformat())

        return sorted([c for c in candles if isinstance(c, list) and len(c) >= 6], key=_key)

    @staticmethod
    def _is_monthly_expiry(expiry: date_cls, all_expiries: list[date_cls]) -> bool:
        return not any((d.year == expiry.year and d.month == expiry.month and d > expiry) for d in all_expiries)

    def _fetch_vix_upstox(self) -> tuple[float, datetime | None]:
        key = (self.upstox.settings.india_vix_instrument_key or "").strip()
        if not key:
            return 0.0, None
        q = self.upstox.get_quote(key)
        px = self._to_float(q.ltp) or self._to_float(q.close)
        ts = parse_any_ts(getattr(q, "ts", ""))
        return (float(px) if px > 0 else 0.0), ts

    def fetch_vix_with_source(self) -> tuple[float, str, datetime]:
        now_i = now_ist()
        try:
            px, ts = self._fetch_vix_upstox()
            if px > 0:
                return px, "upstox", (ts.astimezone(IST) if ts is not None else now_i)
        except Exception:
            logger.debug("Upstox VIX fetch failed", exc_info=True)
        return self._fetch_vix_yahoo()

    def _fetch_vix_yahoo(self) -> tuple[float, str, datetime]:
        fetched_at = now_ist()
        try:
            r = httpx.get(
                "https://query1.finance.yahoo.com/v8/finance/chart/%5EINDIAVIX?interval=1d&range=1d",
                timeout=10.0,
            )
            if r.status_code == 200:
                data = r.json()
                px = (((data.get("chart") or {}).get("result") or [{}])[0].get("meta") or {}).get("regularMarketPrice")
                if px is not None:
                    out = float(px)
                    if out > 0:
                        return out, "yahoo", fetched_at
        except Exception:
            logger.debug("Yahoo VIX fetch failed", exc_info=True)
        return 0.0, "fallback", fetched_at

    def fetch_vix(self) -> float:
        px, _, _ = self.fetch_vix_with_source()
        return px

    def _list_pcr_expiries(self, instrument_key: str) -> list[str]:
        expiries: list[str] = []
        try:
            expiries = self.upstox.get_expiries(instrument_key)
        except Exception:
            logger.debug("Upstox get_expiries failed for PCR; trying option contracts", exc_info=True)
        if not expiries:
            try:
                contracts = self.upstox.get_option_contracts(instrument_key)
                for row in contracts:
                    if not isinstance(row, dict):
                        continue
                    ex = str(
                        row.get("expiry")
                        or row.get("expiry_date")
                        or row.get("expiryDate")
                        or row.get("date")
                        or ""
                    ).strip()
                    if ex:
                        expiries.append(ex)
            except Exception:
                logger.debug("Upstox option contracts fallback failed for PCR expiry", exc_info=True)
        if not expiries:
            return []
        seen: set[str] = set()
        out = [x for x in expiries if x and not (x in seen or seen.add(x))]
        parsed = [(self._parse_iso_date(x), x) for x in out]
        valid = [(d, x) for d, x in parsed if d is not None]
        if not valid:
            return out
        valid.sort(key=lambda t: t[0])
        return [x for _, x in valid]

    def _pick_pcr_expiry(self, instrument_key: str) -> str:
        override = (self.upstox.settings.pcr_expiry_date or "").strip()
        if override:
            return override
        expiries = self._list_pcr_expiries(instrument_key)
        if not expiries:
            return ""
        today = now_ist().date()
        parsed: list[tuple[date_cls, str]] = []
        for ex in expiries:
            d = self._parse_iso_date(ex)
            if d is not None:
                parsed.append((d, ex))
        if not parsed:
            return expiries[0]
        future = [item for item in parsed if item[0] >= today]
        if future:
            future.sort(key=lambda x: x[0])
            return future[0][1]
        parsed.sort(key=lambda x: x[0], reverse=True)
        return parsed[0][1]

    def _aggregate_pcr_from_option_chain(self, rows: list[dict], *, spot: float = 0.0, expiry_date: str = "") -> PcrSnapshot:
        put_oi_sum = 0.0
        call_oi_sum = 0.0
        put_oi_change_sum = 0.0
        call_oi_change_sum = 0.0
        fallback_pcrs: list[float] = []
        oi_by_strike: dict[float, float] = {}
        call_oi_by_strike: dict[float, float] = {}
        put_oi_by_strike: dict[float, float] = {}

        for row in rows:
            if not isinstance(row, dict):
                continue
            call_md = (row.get("call_options") or {}) if isinstance(row.get("call_options"), dict) else {}
            call_mkt = (call_md.get("market_data") or {}) if isinstance(call_md.get("market_data"), dict) else {}
            put_md = (row.get("put_options") or {}) if isinstance(row.get("put_options"), dict) else {}
            put_mkt = (put_md.get("market_data") or {}) if isinstance(put_md.get("market_data"), dict) else {}

            call_oi = self._to_float(call_mkt.get("oi"))
            put_oi = self._to_float(put_mkt.get("oi"))
            call_prev_oi = self._to_float(call_mkt.get("prev_oi") or call_mkt.get("prevOi") or call_mkt.get("previous_oi"))
            put_prev_oi = self._to_float(put_mkt.get("prev_oi") or put_mkt.get("prevOi") or put_mkt.get("previous_oi"))
            call_doi = call_oi - call_prev_oi if call_oi > 0 and call_prev_oi > 0 else 0.0
            put_doi = put_oi - put_prev_oi if put_oi > 0 and put_prev_oi > 0 else 0.0

            if call_oi > 0:
                call_oi_sum += call_oi
            if put_oi > 0:
                put_oi_sum += put_oi
            call_oi_change_sum += call_doi
            put_oi_change_sum += put_doi

            strike = self._to_float(row.get("strike_price") if "strike_price" in row else row.get("strikePrice"))
            if strike > 0 and (call_oi > 0 or put_oi > 0):
                oi_by_strike[strike] = oi_by_strike.get(strike, 0.0) + call_oi + put_oi
                if call_oi > 0:
                    call_oi_by_strike[strike] = call_oi_by_strike.get(strike, 0.0) + call_oi
                if put_oi > 0:
                    put_oi_by_strike[strike] = put_oi_by_strike.get(strike, 0.0) + put_oi

            row_pcr = self._to_float(row.get("pcr"))
            if math.isfinite(row_pcr) and 0.0 < row_pcr <= 10.0:
                fallback_pcrs.append(row_pcr)

        total_oi = call_oi_sum + put_oi_sum
        max_pain = 0.0
        if oi_by_strike:
            try:
                max_pain = float(max(oi_by_strike.items(), key=lambda kv: kv[1])[0])
            except Exception:
                max_pain = 0.0

        call_wall = 0.0
        put_wall = 0.0
        try:
            if call_oi_by_strike:
                call_wall = float(max(call_oi_by_strike.items(), key=lambda kv: kv[1])[0])
            if put_oi_by_strike:
                put_wall = float(max(put_oi_by_strike.items(), key=lambda kv: kv[1])[0])
        except Exception:
            logger.debug("PCR wall computation failed", exc_info=True)

        pcr_value = 0.0
        if call_oi_sum > 0 and put_oi_sum > 0:
            pcr = put_oi_sum / call_oi_sum
            if math.isfinite(pcr) and pcr > 0:
                pcr_value = float(self._clamp(pcr, 0.05, 5.0))
        if pcr_value <= 0 and fallback_pcrs:
            fallback_pcrs.sort()
            mid = len(fallback_pcrs) // 2
            pcr_value = float(fallback_pcrs[mid] if len(fallback_pcrs) % 2 == 1 else (fallback_pcrs[mid - 1] + fallback_pcrs[mid]) / 2.0)
        if pcr_value <= 0:
            pcr_value = 1.0

        top3_share = 0.0
        if oi_by_strike and total_oi > 0:
            top3 = sorted(oi_by_strike.values(), reverse=True)[:3]
            top3_share = float(sum(top3) / total_oi)

        pos_call_doi = max(0.0, call_oi_change_sum)
        pos_put_doi = max(0.0, put_oi_change_sum)
        oi_change_pcr = pcr_value
        if pos_call_doi > 0 and pos_put_doi > 0:
            oi_change_pcr = float(self._clamp(pos_put_doi / pos_call_doi, 0.05, 5.0))
        elif pos_put_doi > 0 and pos_call_doi <= 0:
            oi_change_pcr = 5.0
        elif pos_call_doi > 0 and pos_put_doi <= 0:
            oi_change_pcr = 0.05

        call_wall_dist = ((call_wall - spot) / spot * 100.0) if spot > 0 and call_wall > 0 else 0.0
        put_wall_dist = ((put_wall - spot) / spot * 100.0) if spot > 0 and put_wall > 0 else 0.0
        max_pain_dist = ((max_pain - spot) / spot * 100.0) if spot > 0 and max_pain > 0 else 0.0

        confidence = 0.0
        if total_oi > 0:
            confidence = 55.0
            confidence += 15.0 if (call_oi_sum > 0 and put_oi_sum > 0) else 0.0
            confidence += self._clamp(top3_share * 20.0, 0.0, 15.0)
            confidence += 8.0 if abs(max_pain_dist) <= 3.0 else 3.0
            confidence = self._clamp(confidence, 0.0, 100.0)

        return PcrSnapshot(
            pcr=float(pcr_value),
            max_pain=float(max_pain or 0.0),
            call_oi=float(call_oi_sum or 0.0),
            put_oi=float(put_oi_sum or 0.0),
            pcr_near=float(pcr_value),
            pcr_weighted=float(pcr_value),
            oi_change_pcr=float(oi_change_pcr),
            oi_change_call=float(call_oi_change_sum),
            oi_change_put=float(put_oi_change_sum),
            oi_concentration=float(top3_share),
            call_wall=float(call_wall),
            put_wall=float(put_wall),
            call_wall_dist_pct=float(call_wall_dist),
            put_wall_dist_pct=float(put_wall_dist),
            max_pain_dist_pct=float(max_pain_dist),
            expiry_near=str(expiry_date or ""),
            expiries_used=1,
            confidence=float(confidence),
            fetched_at=now_ist().isoformat(),
        )

    def _select_pcr_expiry_set(self, expiries: list[str]) -> tuple[str, str, str]:
        today = now_ist().date()
        parsed: list[tuple[date_cls, str]] = []
        for ex in expiries:
            d = self._parse_iso_date(ex)
            if d is not None and d >= today:
                parsed.append((d, ex))
        if not parsed:
            return "", "", ""
        parsed.sort(key=lambda x: x[0])
        ds = [d for d, _ in parsed]
        near = parsed[0][1]
        nxt = parsed[1][1] if len(parsed) > 1 else ""
        monthly = ""
        for d, ex in parsed:
            if self._is_monthly_expiry(d, ds):
                monthly = ex
                break
        if not monthly:
            monthly = parsed[min(2, len(parsed) - 1)][1]
        return near, nxt, monthly

    def fetch_pcr_with_source(self, *, spot: float = 0.0) -> tuple[PcrSnapshot, str]:
        key = (self.upstox.settings.pcr_underlying_instrument_key or self.upstox.settings.nifty50_instrument_key or "").strip()
        if not key:
            return PcrSnapshot(pcr=1.0), "fallback"
        try:
            expiries = self._list_pcr_expiries(key)
            if not expiries:
                return PcrSnapshot(pcr=1.0), "fallback"
            near_ex, next_ex, monthly_ex = self._select_pcr_expiry_set(expiries)
            picks = [e for e in [near_ex, next_ex, monthly_ex] if e]
            seen: set[str] = set()
            picks = [e for e in picks if not (e in seen or seen.add(e))]
            if not picks:
                return PcrSnapshot(pcr=1.0), "fallback"

            snaps: dict[str, PcrSnapshot] = {}
            total_weights = 0.0
            weighted_sum = 0.0
            now_i = now_ist()
            for rank, ex in enumerate(picks):
                rows = self.upstox.get_option_chain(key, ex)
                snap = self._aggregate_pcr_from_option_chain(rows, spot=spot, expiry_date=ex)
                snaps[ex] = snap
                d = self._parse_iso_date(ex)
                dte = max(0, (d - now_i.date()).days) if d else (7 * (rank + 1))
                oi_weight = max(1.0, math.log10(max(10.0, snap.call_oi + snap.put_oi)))
                rank_weight = [1.0, 0.7, 0.45][rank] if rank < 3 else 0.3
                time_weight = 1.0 / (1.0 + (dte / 7.0))
                w = oi_weight * rank_weight * time_weight
                weighted_sum += snap.pcr * w
                total_weights += w

            if not snaps:
                return PcrSnapshot(pcr=1.0), "fallback"

            near = snaps.get(near_ex) or next(iter(snaps.values()))
            weighted_pcr = float(weighted_sum / total_weights) if total_weights > 0 else float(near.pcr)
            pcr_next = snaps.get(next_ex).pcr if next_ex and next_ex in snaps else near.pcr
            pcr_month = snaps.get(monthly_ex).pcr if monthly_ex and monthly_ex in snaps else near.pcr
            term_slope = float(pcr_next - near.pcr) if next_ex and next_ex in snaps else 0.0

            out = PcrSnapshot(
                pcr=float(self._clamp(weighted_pcr, 0.05, 5.0)),
                max_pain=near.max_pain,
                call_oi=near.call_oi,
                put_oi=near.put_oi,
                pcr_near=near.pcr,
                pcr_next=float(pcr_next),
                pcr_monthly=float(pcr_month),
                pcr_weighted=float(self._clamp(weighted_pcr, 0.05, 5.0)),
                pcr_term_slope=float(term_slope),
                oi_change_pcr=near.oi_change_pcr,
                oi_change_call=near.oi_change_call,
                oi_change_put=near.oi_change_put,
                oi_concentration=near.oi_concentration,
                call_wall=near.call_wall,
                put_wall=near.put_wall,
                call_wall_dist_pct=near.call_wall_dist_pct,
                put_wall_dist_pct=near.put_wall_dist_pct,
                max_pain_dist_pct=near.max_pain_dist_pct,
                expiry_near=near_ex,
                expiry_next=next_ex,
                expiry_monthly=monthly_ex,
                expiries_used=len(snaps),
                confidence=float(self._clamp((near.confidence * 0.7) + (len(snaps) * 10.0), 0.0, 100.0)),
                fetched_at=now_i.isoformat(),
            )
            return out, "upstox_option_chain"
        except Exception:
            logger.debug("Upstox PCR fetch failed", exc_info=True)
        return PcrSnapshot(pcr=1.0, fetched_at=now_ist().isoformat()), "fallback"

    def fetch_pcr(self) -> PcrSnapshot:
        pcr, _ = self.fetch_pcr_with_source()
        return pcr

    def fetch_fii_dii(self) -> FiiDiiSnapshot:
        fii, _ = self.fetch_fii_dii_with_source()
        return fii

    def _fii_freshness_score(self, as_of_date: str, now_i: datetime, source: str) -> float:
        d = self._parse_iso_date(as_of_date) if as_of_date else None
        if d is None:
            return 25.0 if source == "cache" else 0.0
        age_days = max(0, (now_i.date() - d).days)
        if age_days == 0:
            base = 100.0
        elif age_days == 1:
            base = 88.0
        elif age_days == 2:
            base = 72.0
        elif age_days <= 4:
            base = 50.0
        else:
            base = 25.0
        if source == "cache":
            base -= 10.0
        return self._clamp(base, 0.0, 100.0)

    def fetch_fii_dii_with_source(self) -> tuple[FiiDiiSnapshot, str]:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/",
        }
        now_i = now_ist()
        try:
            with httpx.Client(timeout=10.0, follow_redirects=True, headers=headers) as client:
                try:
                    client.get("https://www.nseindia.com/", timeout=10.0)
                except Exception:
                    pass
                r = client.get("https://www.nseindia.com/api/fiidiiTradeReact", timeout=10.0)
                if r.status_code == 200:
                    data = r.json()
                    rows = data if isinstance(data, list) else (data.get("data") if isinstance(data, dict) else [])
                    if isinstance(rows, list) and rows:
                        def _row_text(row: dict[str, Any]) -> str:
                            parts = []
                            for k in ("category", "Category", "clientType", "CLIENT_TYPE", "participantType", "type", "Type", "name", "Name"):
                                v = row.get(k)
                                if v is not None:
                                    parts.append(str(v))
                            return " ".join(parts).strip().upper()

                        def _row_date(row: dict[str, Any]) -> str:
                            for k in ("date", "Date", "tradeDate", "trade_date", "asOfDate", "timestamp"):
                                v = row.get(k)
                                if v is None:
                                    continue
                                s = str(v).strip()
                                if not s:
                                    continue
                                d = self._parse_iso_date(s)
                                if d is not None:
                                    return d.isoformat()
                            return ""

                        def _row_net(row: dict[str, Any], side: str) -> float:
                            if side == "FII":
                                direct = row.get("fiiNetVal") or row.get("FII_NET") or row.get("fii")
                            else:
                                direct = row.get("diiNetVal") or row.get("DII_NET") or row.get("dii")
                            net = self._to_float(direct or row.get("netVal") or row.get("NET_BUY_SELL"))
                            if abs(net) > 1e-9:
                                return net
                            buy_keys = [f"{side.lower()}BuyValue", "buyValue", "BUY_VALUE"]
                            sell_keys = [f"{side.lower()}SellValue", "sellValue", "SELL_VALUE"]
                            buy = 0.0
                            sell = 0.0
                            for k in buy_keys:
                                buy = self._to_float(row.get(k))
                                if abs(buy) > 1e-9:
                                    break
                            for k in sell_keys:
                                sell = self._to_float(row.get(k))
                                if abs(sell) > 1e-9:
                                    break
                            return (buy - sell) if (abs(buy) > 1e-9 or abs(sell) > 1e-9) else 0.0

                        fii = 0.0
                        dii = 0.0
                        found_fii = False
                        found_dii = False
                        dates_seen: set[str] = set()
                        for row in rows:
                            if not isinstance(row, dict):
                                continue
                            label = _row_text(row)
                            d_str = _row_date(row)
                            if d_str:
                                dates_seen.add(d_str)
                            if ("FII" in label) or ("FPI" in label) or ("FOREIGN" in label):
                                val = _row_net(row, "FII")
                                fii += val
                                found_fii = True
                            elif ("DII" in label) or ("DOMESTIC" in label):
                                val = _row_net(row, "DII")
                                dii += val
                                found_dii = True

                        if (not found_fii and not found_dii):
                            t = rows[0] if isinstance(rows[0], dict) else {}
                            fii = self._to_float(t.get("netVal") or t.get("NET_BUY_SELL") or t.get("fiiNetVal") or t.get("fii"))
                            dii = self._to_float(t.get("diiNetVal") or t.get("DII_NET") or t.get("dii"))
                            if abs(fii) < 1e-9:
                                fii = _row_net(t, "FII")
                            if abs(dii) < 1e-9:
                                dii = _row_net(t, "DII")

                        as_of = ""
                        if dates_seen:
                            ds = sorted([d for d in dates_seen if d])
                            as_of = ds[-1]
                        if not as_of and isinstance(data, dict):
                            as_of = str(data.get("date") or data.get("asOfDate") or "").strip()
                            d = self._parse_iso_date(as_of)
                            as_of = d.isoformat() if d else ""

                        if found_fii or found_dii or abs(fii) > 1e-9 or abs(dii) > 1e-9:
                            snap = FiiDiiSnapshot(
                                fii=float(fii),
                                dii=float(dii),
                                as_of_date=as_of,
                                fetched_at=now_i.isoformat(),
                                freshness_score=float(self._fii_freshness_score(as_of, now_i, "nse")),
                            )
                            self._last_fii_dii = snap
                            self._last_fii_dii_fetch_ts = now_i
                            return snap, "nse"
        except Exception:
            logger.debug("NSE FII/DII fetch failed", exc_info=True)

        if self._last_fii_dii is not None:
            cached = FiiDiiSnapshot(
                fii=float(self._last_fii_dii.fii),
                dii=float(self._last_fii_dii.dii),
                as_of_date=str(self._last_fii_dii.as_of_date or ""),
                fetched_at=(self._last_fii_dii_fetch_ts.isoformat() if self._last_fii_dii_fetch_ts else str(self._last_fii_dii.fetched_at or "")),
                freshness_score=float(self._fii_freshness_score(self._last_fii_dii.as_of_date, now_i, "cache")),
            )
            return cached, "cache"

        return FiiDiiSnapshot(fii=0.0, dii=0.0, fetched_at=now_i.isoformat(), freshness_score=0.0), "fallback"

    def _derive_nifty_ohl_fallback(self, instrument_key: str) -> tuple[float, float, float]:
        ik = str(instrument_key or "").strip()
        if not ik:
            return 0.0, 0.0, 0.0
        try:
            candles = self.upstox.get_intraday_candles_v3(ik, unit="minutes", interval=15)
            if candles:
                parsed: list[tuple[datetime, list[Any]]] = []
                for c in candles:
                    if not isinstance(c, list) or len(c) < 5:
                        continue
                    ts_raw = str(c[0] or "")
                    dt = parse_any_ts(ts_raw)
                    if dt is None:
                        continue
                    parsed.append((dt.astimezone(IST), c))
                if parsed:
                    latest_date = max(dt.date() for dt, _ in parsed)
                    same_day = [(dt, c) for dt, c in parsed if dt.date() == latest_date]
                    same_day.sort(key=lambda x: x[0])
                    opens = [self._to_float(c[1]) for _, c in same_day]
                    highs = [self._to_float(c[2]) for _, c in same_day]
                    lows = [self._to_float(c[3]) for _, c in same_day]
                    if opens and highs and lows and max(highs) > 0 and min(lows) > 0:
                        return float(opens[0]), float(max(highs)), float(min(lows))
        except Exception:
            logger.debug("Nifty intraday OHL fallback failed", exc_info=True)

        try:
            to_date = now_ist().date().isoformat()
            from_date = (now_ist().date() - timedelta(days=7)).isoformat()
            candles_d = self.upstox.get_historical_candles_v3_days(ik, to_date=to_date, from_date=from_date, interval_days=1)
            if candles_d:
                latest = candles_d[0]
                if isinstance(latest, list) and len(latest) >= 5:
                    return (
                        float(self._to_float(latest[1]) or 0.0),
                        float(self._to_float(latest[2]) or 0.0),
                        float(self._to_float(latest[3]) or 0.0),
                    )
        except Exception:
            logger.debug("Nifty daily OHL fallback failed", exc_info=True)
        return 0.0, 0.0, 0.0

    def _calc_adx(self, candles: list[list[Any]], period: int = 14) -> float:
        c = normalize_candles(candles)
        if len(c) < period + 2:
            return 0.0
        highs = [x[2] for x in c]
        lows = [x[3] for x in c]
        closes = [x[4] for x in c]
        tr_list: list[float] = []
        pdm_list: list[float] = []
        ndm_list: list[float] = []
        for i in range(1, len(c)):
            up_move = highs[i] - highs[i - 1]
            down_move = lows[i - 1] - lows[i]
            pdm = up_move if (up_move > down_move and up_move > 0) else 0.0
            ndm = down_move if (down_move > up_move and down_move > 0) else 0.0
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
            tr_list.append(tr)
            pdm_list.append(pdm)
            ndm_list.append(ndm)
        if len(tr_list) < period:
            return 0.0
        tr14 = sum(tr_list[:period])
        pdm14 = sum(pdm_list[:period])
        ndm14 = sum(ndm_list[:period])
        dxs: list[float] = []
        for i in range(period, len(tr_list) + 1):
            if i > period:
                tr14 = tr14 - (tr14 / period) + tr_list[i - 1]
                pdm14 = pdm14 - (pdm14 / period) + pdm_list[i - 1]
                ndm14 = ndm14 - (ndm14 / period) + ndm_list[i - 1]
            if tr14 <= 0:
                dxs.append(0.0)
                continue
            pdi = 100.0 * (pdm14 / tr14)
            ndi = 100.0 * (ndm14 / tr14)
            denom = pdi + ndi
            dx = 0.0 if denom <= 0 else (100.0 * abs(pdi - ndi) / denom)
            dxs.append(dx)
        if not dxs:
            return 0.0
        adx = sum(dxs[:period]) / min(period, len(dxs))
        for dx in dxs[period:]:
            adx = ((adx * (period - 1)) + dx) / period
        return float(self._clamp(adx, 0.0, 100.0))

    def _compute_structure_from_candles(
        self,
        candles: list[list[Any]],
        *,
        timeframe: str,
        spot: float = 0.0,
        intraday_orb: bool = True,
    ) -> NiftyStructureSnapshot:
        candles = self._sort_candles_asc(candles)
        if not candles:
            return NiftyStructureSnapshot(timeframe=timeframe)
        ind = compute_indicators(candles, self.cfg)
        latest_dt = parse_any_ts(candles[-1][0])
        latest_ts = latest_dt.astimezone(IST).isoformat() if latest_dt else str(candles[-1][0])
        if ind is None:
            return NiftyStructureSnapshot(timeframe=timeframe, bars=len(candles), last_candle_ts=latest_ts)

        latest_date = latest_dt.astimezone(IST).date() if latest_dt else None
        same_day: list[list[Any]] = []
        if intraday_orb and latest_date is not None:
            for c in candles:
                dt = parse_any_ts(c[0])
                if dt is None:
                    continue
                if dt.astimezone(IST).date() == latest_date:
                    same_day.append(c)

        adx = self._calc_adx(candles, period=14)
        close_px = float(ind.close or (spot or 0.0) or 0.0)
        atr_pct = ((ind.atr / close_px) * 100.0) if close_px > 0 and ind.atr > 0 else 0.0
        ema_spread_pct = (abs(ind.ema_fast.curr - ind.ema_slow.curr) / close_px * 100.0) if close_px > 0 else 0.0
        vwap_gap_pct = ((close_px - ind.vwap) / ind.vwap * 100.0) if ind.vwap else 0.0

        ema_stack = "BULL_STACK" if ind.ema_stack else ("BEAR_STACK" if ind.ema_flip else "MIXED")
        st_dir = int(ind.supertrend.dir or 0)

        gap_pct = 0.0
        or_break = "NONE"
        if intraday_orb and same_day:
            first = same_day[0]
            or_high = self._to_float(first[2])
            or_low = self._to_float(first[3])
            if close_px > 0 and or_high > 0 and close_px > or_high:
                or_break = "UP_BREAK"
            elif close_px > 0 and or_low > 0 and close_px < or_low:
                or_break = "DOWN_BREAK"
            else:
                or_break = "INSIDE"
            if len(candles) > len(same_day):
                prev_day_close = self._to_float(candles[len(candles) - len(same_day) - 1][4])
                day_open = self._to_float(first[1])
                if prev_day_close > 0 and day_open > 0:
                    gap_pct = ((day_open - prev_day_close) / prev_day_close) * 100.0
        else:
            # Daily fallback: use latest bar open vs prior close as gap, ORB not applicable.
            if len(candles) >= 2:
                prev_close = self._to_float(candles[-2][4])
                day_open = self._to_float(candles[-1][1])
                if prev_close > 0 and day_open > 0:
                    gap_pct = ((day_open - prev_close) / prev_close) * 100.0

        trend_strength = 20.0
        trend_strength += self._clamp((adx - 15.0) * 1.8, 0.0, 28.0)
        trend_strength += 16.0 if ema_stack in {"BULL_STACK", "BEAR_STACK"} else 0.0
        trend_strength += self._clamp(ema_spread_pct * 30.0, 0.0, 16.0)
        trend_strength += self._clamp(abs(vwap_gap_pct) * 8.0, 0.0, 10.0)
        trend_strength += 6.0 if abs(ind.macd.hist) > 0 else 0.0
        if intraday_orb:
            trend_strength += 4.0 if or_break in {"UP_BREAK", "DOWN_BREAK"} else 0.0
        trend_strength = self._clamp(trend_strength, 0.0, 100.0)

        chop_risk = 55.0
        chop_risk += self._clamp((20.0 - adx) * 2.0, -20.0, 25.0)
        chop_risk += self._clamp((0.25 - abs(vwap_gap_pct)) * 25.0, -10.0, 15.0)
        chop_risk += self._clamp((0.18 - ema_spread_pct) * 70.0, -15.0, 15.0)
        chop_risk += 8.0 if 45.0 <= ind.rsi.curr <= 55.0 else -3.0
        if intraday_orb:
            chop_risk += 6.0 if or_break == "INSIDE" else -4.0
        chop_risk = self._clamp(chop_risk, 0.0, 100.0)

        structure_regime = "CHOPPY"
        if adx >= 25 and ema_stack == "BULL_STACK" and st_dir == 1 and vwap_gap_pct >= 0:
            structure_regime = "TRENDING_UP"
        elif adx >= 25 and ema_stack == "BEAR_STACK" and st_dir == -1 and vwap_gap_pct <= 0:
            structure_regime = "TRENDING_DOWN"
        elif chop_risk >= 70 and atr_pct < 1.2:
            structure_regime = "RANGE_COMPRESSION"
        elif atr_pct >= 1.2 and adx < 22:
            structure_regime = "RANGE_EXPANSION"
        elif trend_strength >= 65 and ema_stack == "BULL_STACK":
            structure_regime = "TREND_WEAK_UP"
        elif trend_strength >= 65 and ema_stack == "BEAR_STACK":
            structure_regime = "TREND_WEAK_DOWN"

        return NiftyStructureSnapshot(
            timeframe=timeframe,
            bars=len(candles),
            last_candle_ts=latest_ts,
            ema_stack=ema_stack,
            supertrend_dir=st_dir,
            rsi=float(ind.rsi.curr),
            macd_hist=float(ind.macd.hist),
            adx=float(adx),
            atr_pct=float(atr_pct),
            ema_spread_pct=float(ema_spread_pct),
            vwap_gap_pct=float(vwap_gap_pct),
            gap_pct=float(gap_pct),
            opening_range_break=or_break if intraday_orb else "N/A",
            trend_strength=float(trend_strength),
            chop_risk=float(chop_risk),
            structure_regime=structure_regime,
        )

    def _compute_nifty_structure(self, instrument_key: str, spot: float = 0.0) -> NiftyStructureSnapshot:
        try:
            raw: list[list[Any]] = []
            try:
                raw = self.upstox.get_intraday_candles_v3(instrument_key, unit="minutes", interval=15)
            except Exception:
                # Upstox often returns 400 for index intraday candles after-hours; fall back to daily structure below.
                logger.debug("Nifty intraday structure source unavailable; using fallback", exc_info=True)
            intraday = self._compute_structure_from_candles(raw, timeframe="15m", spot=spot, intraday_orb=True)
            if intraday.bars > 0:
                self._last_nifty_structure = intraday
                self._last_nifty_structure_fetch_ts = now_ist()
                return intraday

            # After market close or on provider gaps, intraday endpoint may return no rows for index.
            try:
                to_date = now_ist().date().isoformat()
                from_date = (now_ist().date() - timedelta(days=220)).isoformat()
                raw_d = self.upstox.get_historical_candles_v3_days(instrument_key, to_date=to_date, from_date=from_date, interval_days=1)
                daily = self._compute_structure_from_candles(raw_d, timeframe="1d_fallback", spot=spot, intraday_orb=False)
                if daily.bars > 0:
                    self._last_nifty_structure = daily
                    self._last_nifty_structure_fetch_ts = now_ist()
                    return daily
            except Exception:
                logger.debug("Nifty daily structure fallback failed", exc_info=True)

            if self._last_nifty_structure is not None:
                cached = self._last_nifty_structure
                tf = cached.timeframe if "cache" in str(cached.timeframe).lower() else f"{cached.timeframe}_cache"
                return NiftyStructureSnapshot(
                    timeframe=tf,
                    bars=cached.bars,
                    last_candle_ts=cached.last_candle_ts,
                    ema_stack=cached.ema_stack,
                    supertrend_dir=cached.supertrend_dir,
                    rsi=cached.rsi,
                    macd_hist=cached.macd_hist,
                    adx=cached.adx,
                    atr_pct=cached.atr_pct,
                    ema_spread_pct=cached.ema_spread_pct,
                    vwap_gap_pct=cached.vwap_gap_pct,
                    gap_pct=cached.gap_pct,
                    opening_range_break=cached.opening_range_break,
                    trend_strength=cached.trend_strength,
                    chop_risk=cached.chop_risk,
                    structure_regime=cached.structure_regime,
                )
            return NiftyStructureSnapshot()
        except Exception:
            logger.debug("Nifty structure computation failed", exc_info=True)
            return NiftyStructureSnapshot()

    def _source_quality_score(self, *, vix_source: str, pcr_source: str, fii_source: str) -> float:
        vix_q = {"upstox": 100.0, "yahoo": 75.0, "fallback": 20.0}.get(vix_source, 40.0)
        pcr_q = {"upstox_option_chain": 100.0, "fallback": 20.0}.get(pcr_source, 40.0)
        fii_q = {"nse": 100.0, "cache": 70.0, "fallback": 20.0}.get(fii_source, 40.0)
        nifty_q = 100.0  # current regime pipeline uses Upstox quote directly for Nifty
        return float(round((nifty_q * 0.3) + (vix_q * 0.2) + (pcr_q * 0.3) + (fii_q * 0.2), 2))

    def _freshness_snapshot(
        self,
        now_i: datetime,
        *,
        session_phase: str,
        nifty_ts: datetime | None,
        vix_ts: datetime | None,
        pcr_ts: datetime | None,
        fii_snap: FiiDiiSnapshot,
    ) -> FreshnessSnapshot:
        nifty_age = self._safe_age_sec(now_i, nifty_ts)
        vix_age = self._safe_age_sec(now_i, vix_ts)
        pcr_age = self._safe_age_sec(now_i, pcr_ts)
        fii_age_hours = 999.0
        if str(getattr(fii_snap, "as_of_date", "") or ""):
            d = self._parse_iso_date(str(fii_snap.as_of_date))
            if d is not None:
                fii_age_hours = max(0.0, (now_i.date() - d).days * 24.0)
        elif str(getattr(fii_snap, "fetched_at", "") or ""):
            dt = parse_any_ts(str(fii_snap.fetched_at))
            if dt is not None:
                fii_age_hours = max(0.0, (now_i - dt.astimezone(IST)).total_seconds() / 3600.0)

        # Session-aware freshness: during regular hours quote/PCR recency matters much more.
        if session_phase in {"REGULAR", "OPENING", "PRE_OPEN"}:
            nifty_s = self._clamp(100.0 - (nifty_age / 2.0), 0.0, 100.0)
            vix_s = self._clamp(100.0 - (vix_age / 2.0), 0.0, 100.0)
            pcr_s = self._clamp(100.0 - (pcr_age / 5.0), 0.0, 100.0)
        else:
            nifty_s = self._clamp(85.0 - (nifty_age / 60.0), 30.0, 100.0)
            vix_s = self._clamp(85.0 - (vix_age / 60.0), 30.0, 100.0)
            pcr_s = self._clamp(85.0 - (pcr_age / 120.0), 25.0, 100.0)
        fii_s = float(getattr(fii_snap, "freshness_score", 0.0) or 0.0)
        score = float(round((nifty_s * 0.35) + (vix_s * 0.2) + (pcr_s * 0.3) + (fii_s * 0.15), 2))
        return FreshnessSnapshot(
            generated_at=now_i.isoformat(),
            session_phase=session_phase,
            nifty_age_sec=float(round(nifty_age, 1)),
            vix_age_sec=float(round(vix_age, 1)),
            pcr_age_sec=float(round(pcr_age, 1)),
            fii_age_hours=float(round(fii_age_hours, 2)) if fii_age_hours < 999 else 999.0,
            score=float(score),
        )

    def _derive_bias(self, nifty_change_pct: float, structure: NiftyStructureSnapshot, pcr: PcrSnapshot, fii: FiiDiiSnapshot) -> Bias:
        if nifty_change_pct >= self.cfg.nifty_trend_pct:
            return "BULLISH"
        if nifty_change_pct <= -self.cfg.nifty_trend_pct:
            return "BEARISH"
        if structure.trend_strength >= 70:
            if structure.structure_regime in {"TRENDING_UP", "TREND_WEAK_UP"}:
                return "BULLISH"
            if structure.structure_regime in {"TRENDING_DOWN", "TREND_WEAK_DOWN"}:
                return "BEARISH"
        if pcr.pcr_weighted >= self.cfg.pcr_bull_min and fii.fii > 0:
            return "BULLISH"
        if pcr.pcr_weighted <= self.cfg.pcr_bear_max and fii.fii < 0:
            return "BEARISH"
        return "NEUTRAL"

    def _derive_regime(self, *, vix: float, nifty_change_pct: float, bias: Bias, pcr: PcrSnapshot, structure: NiftyStructureSnapshot) -> tuple[str, str]:
        regime = "TREND"
        reasons: list[str] = []
        if vix and vix > self.cfg.vix_safe_max:
            regime = "AVOID"
            reasons.append("vix_high")
        elif vix and vix > self.cfg.vix_trend_max:
            regime = "RANGE"
            reasons.append("vix_caution")
        elif abs(nifty_change_pct) < 0.15:
            regime = "RANGE"
            reasons.append("nifty_move_small")

        # Nifty structure layer (EMA/ATR/ADX/VWAP/ORB) influences final regime.
        if regime != "AVOID":
            if structure.structure_regime in {"CHOPPY", "RANGE_COMPRESSION"} or structure.chop_risk >= 70:
                regime = "RANGE"
                reasons.append("structure_chop")
            elif structure.structure_regime in {"TRENDING_UP", "TRENDING_DOWN"} and structure.trend_strength >= 70 and (not vix or vix <= self.cfg.vix_trend_max):
                regime = "TREND"
                reasons.append("structure_trend_strong")
            elif structure.atr_pct >= 1.8 and vix > self.cfg.vix_trend_max:
                regime = "AVOID"
                reasons.append("high_realized_vol")

        # PCR contradiction check (weighted PCR + term slope awareness)
        contradiction = False
        if bias == "BULLISH" and pcr.pcr_weighted < max(0.1, self.cfg.pcr_bull_min - 0.2):
            contradiction = True
        if bias == "BEARISH" and pcr.pcr_weighted > self.cfg.pcr_bear_max + 0.2:
            contradiction = True
        if contradiction and regime != "AVOID":
            regime = "RANGE"
            reasons.append("pcr_contradiction")

        sub = "RANGE_BALANCED"
        if regime == "AVOID":
            sub = "VOLATILE_RISK_OFF" if vix > self.cfg.vix_safe_max else "EVENT_RISK"
        elif regime == "RANGE":
            if structure.structure_regime == "RANGE_COMPRESSION":
                sub = "RANGE_COMPRESSION"
            elif structure.structure_regime == "RANGE_EXPANSION":
                sub = "RANGE_EXPANSION"
            else:
                sub = "RANGE_BALANCED"
        else:
            if structure.structure_regime in {"TRENDING_UP", "TREND_WEAK_UP"}:
                sub = "TREND_UP"
            elif structure.structure_regime in {"TRENDING_DOWN", "TREND_WEAK_DOWN"}:
                sub = "TREND_DOWN"
            else:
                sub = "TREND_MIXED"
        return regime, sub if sub else ("|".join(reasons) if reasons else "UNKNOWN")

    def _compute_confidence_and_health(
        self,
        *,
        now_i: datetime,
        regime: str,
        bias: Bias,
        nifty_change_pct: float,
        vix: float,
        pcr: PcrSnapshot,
        fii: FiiDiiSnapshot,
        structure: NiftyStructureSnapshot,
        freshness: FreshnessSnapshot,
        source_quality: float,
    ) -> tuple[float, float, str]:
        # Signal agreement score (0..100)
        signals = []
        if bias != "NEUTRAL":
            signals.append(1.0)
        # Structure agreement with bias
        if bias == "BULLISH":
            signals.append(1.0 if structure.structure_regime in {"TRENDING_UP", "TREND_WEAK_UP"} else (0.5 if structure.ema_stack == "BULL_STACK" else 0.0))
            signals.append(1.0 if pcr.pcr_weighted >= self.cfg.pcr_bull_min else (0.5 if pcr.pcr_weighted >= (self.cfg.pcr_bull_min - 0.2) else 0.0))
            signals.append(1.0 if fii.fii > 0 else (0.5 if fii.fii == 0 else 0.0))
        elif bias == "BEARISH":
            signals.append(1.0 if structure.structure_regime in {"TRENDING_DOWN", "TREND_WEAK_DOWN"} else (0.5 if structure.ema_stack == "BEAR_STACK" else 0.0))
            signals.append(1.0 if pcr.pcr_weighted <= self.cfg.pcr_bear_max else (0.5 if pcr.pcr_weighted <= (self.cfg.pcr_bear_max + 0.2) else 0.0))
            signals.append(1.0 if fii.fii < 0 else (0.5 if fii.fii == 0 else 0.0))
        else:
            signals.append(1.0 if abs(nifty_change_pct) < self.cfg.nifty_trend_pct else 0.4)
            signals.append(1.0 if structure.chop_risk >= 55 else 0.4)
            signals.append(1.0 if self.cfg.pcr_bull_min <= pcr.pcr_weighted <= self.cfg.pcr_bear_max else 0.4)
        signal_agreement = (sum(signals) / max(1, len(signals))) * 100.0

        # Stability from recent regime state in process memory.
        stability = 60.0
        curr_key = f"{regime}|{bias}"
        if self._last_regime_ts is not None:
            age_min = max(0.0, (now_i - self._last_regime_ts.astimezone(IST)).total_seconds() / 60.0)
            if age_min <= 30:
                if self._last_regime_key == curr_key:
                    stability = 95.0
                elif self._last_regime_key.split("|")[0] == regime:
                    stability = 72.0
                else:
                    stability = 35.0
            elif age_min <= 120:
                stability = 75.0 if self._last_regime_key == curr_key else 55.0

        # Strength score from structure + move + derivatives quality.
        strength = 35.0
        strength += self._clamp(abs(nifty_change_pct) * 20.0, 0.0, 15.0)
        strength += self._clamp(structure.trend_strength * 0.35, 0.0, 25.0)
        strength += self._clamp(pcr.confidence * 0.2, 0.0, 15.0)
        if regime == "AVOID":
            strength -= 10.0
        strength = self._clamp(strength, 0.0, 100.0)

        confidence = (
            signal_agreement * 0.40
            + freshness.score * 0.25
            + source_quality * 0.20
            + stability * 0.10
            + strength * 0.05
        )
        confidence = self._clamp(confidence, 0.0, 100.0)

        completeness = 100.0
        if pcr.call_oi <= 0 or pcr.put_oi <= 0:
            completeness -= 20.0
        if getattr(fii, "fetched_at", "") == "":
            completeness -= 20.0
        if structure.bars < 80:
            completeness -= 25.0
        if vix <= 0:
            completeness -= 10.0
        data_health = self._clamp((freshness.score * 0.45) + (source_quality * 0.35) + (completeness * 0.20), 0.0, 100.0)

        rationale = (
            f"reg={regime};bias={bias};str={structure.structure_regime};"
            f"adx={structure.adx:.1f};vix={vix:.2f};pcrW={pcr.pcr_weighted:.2f};fresh={freshness.score:.1f};srcQ={source_quality:.1f}"
        )
        return float(round(confidence, 1)), float(round(data_health, 1)), rationale

    def get_market_regime(self) -> RegimeSnapshot:
        now_i = now_ist()

        nifty_quote_fetch_ts = now_i
        nifty = self.upstox.get_quote(self.upstox.settings.nifty50_instrument_key)
        nifty_quote_ts = parse_any_ts(getattr(nifty, "ts", "")) or nifty_quote_fetch_ts
        if (not float(getattr(nifty, "open", 0.0) or 0.0)) or (not float(getattr(nifty, "high", 0.0) or 0.0)) or (not float(getattr(nifty, "low", 0.0) or 0.0)):
            try:
                o, h, l = self._derive_nifty_ohl_fallback(self.upstox.settings.nifty50_instrument_key)
                if o > 0 and not float(getattr(nifty, "open", 0.0) or 0.0):
                    nifty.open = o
                if h > 0 and not float(getattr(nifty, "high", 0.0) or 0.0):
                    nifty.high = h
                if l > 0 and not float(getattr(nifty, "low", 0.0) or 0.0):
                    nifty.low = l
            except Exception:
                logger.debug("Nifty OHL enrichment failed", exc_info=True)

        vix, vix_source, vix_ts = self.fetch_vix_with_source()
        pcr, pcr_source = self.fetch_pcr_with_source(spot=float(getattr(nifty, "ltp", 0.0) or 0.0))
        fii, fii_source = self.fetch_fii_dii_with_source()
        structure = self._compute_nifty_structure(self.upstox.settings.nifty50_instrument_key, spot=float(getattr(nifty, "ltp", 0.0) or 0.0))

        bias = self._derive_bias(float(nifty.change_pct or 0.0), structure, pcr, fii)
        regime_name, sub_regime = self._derive_regime(
            vix=float(vix or 0.0),
            nifty_change_pct=float(nifty.change_pct or 0.0),
            bias=bias,
            pcr=pcr,
            structure=structure,
        )

        source_quality = self._source_quality_score(vix_source=vix_source, pcr_source=pcr_source, fii_source=fii_source)
        pcr_ts = parse_any_ts(getattr(pcr, "fetched_at", "")) or now_i
        fii_ts = parse_any_ts(getattr(fii, "fetched_at", "")) or now_i
        freshness = self._freshness_snapshot(
            now_i,
            session_phase=self._session_phase(now_i),
            nifty_ts=nifty_quote_ts,
            vix_ts=vix_ts,
            pcr_ts=pcr_ts,
            fii_snap=fii,
        )
        confidence, data_health, rationale = self._compute_confidence_and_health(
            now_i=now_i,
            regime=regime_name,
            bias=bias,
            nifty_change_pct=float(nifty.change_pct or 0.0),
            vix=float(vix or 0.0),
            pcr=pcr,
            fii=fii,
            structure=structure,
            freshness=freshness,
            source_quality=source_quality,
        )

        out = RegimeSnapshot(
            regime=regime_name,  # type: ignore[arg-type]
            bias=bias,
            vix=float(vix or 0.0),
            pcr=pcr,
            fii=fii,
            nifty=NiftySnapshot(
                ltp=float(nifty.ltp or 0.0),
                open=float(nifty.open or 0.0),
                high=float(nifty.high or 0.0),
                low=float(nifty.low or 0.0),
                close=float(nifty.close or 0.0),
                change_pct=float(nifty.change_pct or 0.0),
                quote_ts=nifty_quote_ts.astimezone(IST).isoformat() if nifty_quote_ts else "",
                age_sec=float(round(self._safe_age_sec(now_i, nifty_quote_ts), 1)),
            ),
            nifty_structure=structure,
            freshness=freshness,
            confidence=float(confidence),
            data_health=float(data_health),
            source_quality=float(round(source_quality, 1)),
            sub_regime=sub_regime,
            rationale=rationale,
            source=f"nifty=upstox;vix={vix_source};pcr={pcr_source};fii={fii_source}",
        )

        self._last_regime_key = f"{out.regime}|{out.bias}"
        self._last_regime_ts = now_i
        return out
