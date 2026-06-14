# -*- coding: utf-8 -*-
"""
あっぷるキャピタル MARKET TERMINAL  — 軽量データプロキシ + 静的配信サーバ (v2)
=======================================================================
Bloomberg 端末ライクなリアルタイム市況ダッシュボードのバックエンド。

v2:
  - 銘柄(INSTRUMENTS)を instruments.json に永続化し、画面UIから追加/削除/並べ替え可能に。
  - 追加時は Yahoo に実際に問い合わせて「取得できるシンボルか」を検証してから保存。
  - ロゴ等の静的ファイル(.png/.svg/.ico)も配信。
  - セキュリティ堅牢化: 出力前提の入力検証(文字種)・原子的保存・書込みのロック一貫化・
    保存失敗の通知。公開時用に MT_READONLY で書込み無効化が可能。

設計方針:
  - 株価指数/為替/金利/コモディティ/暗号資産/ニュース(RSS) は CORS 非対応のため
    本サーバが代理取得して中継（ブラウザ直叩き不可）。
  - Python 標準ライブラリのみ（pip 不要・APIキー不要）。
  - 取得結果は in-memory キャッシュ（TTL付き）で Yahoo へのレート負荷と遮断を回避。

環境変数:
  MT_HOST     … bind ホスト（既定 0.0.0.0 = tailnet 内の他端末から閲覧可）。
                ローカル限定にしたい場合は 127.0.0.1 を指定。
  MT_READONLY … 1/true で銘柄の追加/削除/並べ替え(書込みAPI)を無効化（公開閲覧用）。

起動: python server.py [port]   （既定 8799）
"""
import json
import os
import re
import sys
import time
import threading
import unicodedata
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import xml.etree.ElementTree as ET

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("PORT", "8799"))
HOST = os.environ.get("MT_HOST", "0.0.0.0")
READONLY = os.environ.get("MT_READONLY", "").strip().lower() not in ("", "0", "false", "no")
PUBLIC_ORIGIN = os.environ.get("MT_CORS_ORIGIN", "*")
ROOT = Path(__file__).resolve().parent
WEBROOT = ROOT / "v3"          # v3 SPA(dist)。存在すればこちらを既定配信、無ければ v2 single-file
CONFIG_PATH = ROOT / "instruments.json"
NEWS_CONFIG_PATH = ROOT / "news_sources.json"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# 入力検証用パターン
SYM_RE = re.compile(r"^[A-Za-z0-9.^=\-]{1,24}$")  # Yahoo シンボルが取り得る文字集合
NAME_BAD = set('<>"\'')                            # HTML特殊文字（& は S&P 500 等で許可）

# ---------------------------------------------------------------------------
# 表示グループ（順序の正本）と入力バリデーション用の集合
# ---------------------------------------------------------------------------
GROUP_LABELS = [
    ("jp",        "日本株式・REIT"),
    ("world",     "世界の株価指数"),
    ("fx",        "為替"),
    ("rates",     "金利・債券"),
    ("commodity", "コモディティ"),
    ("crypto",    "暗号資産"),
]
VALID_GROUPS = {g for g, _ in GROUP_LABELS}
VALID_KINDS = {"index", "fx", "yield", "px"}
DEFAULT_KIND_BY_GROUP = {
    "jp": "index", "world": "index", "fx": "fx",
    "rates": "yield", "commodity": "px", "crypto": "px",
}

# ---------------------------------------------------------------------------
# 初期銘柄（instruments.json が無い初回のみシードとして書き出す）
# ---------------------------------------------------------------------------
DEFAULT_INSTRUMENTS = [
    {"symbol": "^N225",  "name": "日経平均株価",      "group": "jp", "kind": "index"},
    {"symbol": "1306.T", "name": "TOPIX(連動ETF)",   "group": "jp", "kind": "index"},
    {"symbol": "1343.T", "name": "東証REIT指数(ETF)", "group": "jp", "kind": "index"},
    {"symbol": "^GSPC",    "name": "S&P 500",       "group": "world", "kind": "index"},
    {"symbol": "^IXIC",    "name": "NASDAQ 総合",   "group": "world", "kind": "index"},
    {"symbol": "^DJI",     "name": "NY ダウ",       "group": "world", "kind": "index"},
    {"symbol": "^FTSE",    "name": "英 FTSE 100",   "group": "world", "kind": "index"},
    {"symbol": "^GDAXI",   "name": "独 DAX",        "group": "world", "kind": "index"},
    {"symbol": "^FCHI",    "name": "仏 CAC 40",     "group": "world", "kind": "index"},
    {"symbol": "^STOXX50E","name": "ユーロ Stoxx 50","group": "world", "kind": "index"},
    {"symbol": "^HSI",     "name": "香港 ハンセン",  "group": "world", "kind": "index"},
    {"symbol": "000001.SS","name": "上海総合",       "group": "world", "kind": "index"},
    {"symbol": "^KS11",    "name": "韓国 KOSPI",    "group": "world", "kind": "index"},
    {"symbol": "^TWII",    "name": "台湾 加権",      "group": "world", "kind": "index"},
    {"symbol": "^BSESN",   "name": "印 SENSEX",     "group": "world", "kind": "index"},
    {"symbol": "^AXJO",    "name": "豪 ASX 200",    "group": "world", "kind": "index"},
    {"symbol": "USDJPY=X", "name": "米ドル / 円",   "group": "fx", "kind": "fx"},
    {"symbol": "EURJPY=X", "name": "ユーロ / 円",   "group": "fx", "kind": "fx"},
    {"symbol": "GBPJPY=X", "name": "ポンド / 円",   "group": "fx", "kind": "fx"},
    {"symbol": "AUDJPY=X", "name": "豪ドル / 円",   "group": "fx", "kind": "fx"},
    {"symbol": "CNYJPY=X", "name": "人民元 / 円",   "group": "fx", "kind": "fx"},
    {"symbol": "EURUSD=X", "name": "ユーロ / ドル", "group": "fx", "kind": "fx"},
    {"symbol": "DX-Y.NYB", "name": "ドル指数 (DXY)","group": "fx", "kind": "index"},
    {"symbol": "^TNX", "name": "米 10年債利回り", "group": "rates", "kind": "yield"},
    {"symbol": "^TYX", "name": "米 30年債利回り", "group": "rates", "kind": "yield"},
    {"symbol": "^FVX", "name": "米 5年債利回り",  "group": "rates", "kind": "yield"},
    {"symbol": "^IRX", "name": "米 13週TB利回り", "group": "rates", "kind": "yield"},
    {"symbol": "GC=F", "name": "金 (Gold)",    "group": "commodity", "kind": "px"},
    {"symbol": "SI=F", "name": "銀 (Silver)",  "group": "commodity", "kind": "px"},
    {"symbol": "CL=F", "name": "WTI 原油",     "group": "commodity", "kind": "px"},
    {"symbol": "BZ=F", "name": "ブレント原油", "group": "commodity", "kind": "px"},
    {"symbol": "NG=F", "name": "天然ガス",     "group": "commodity", "kind": "px"},
    {"symbol": "HG=F", "name": "銅 (Copper)",  "group": "commodity", "kind": "px"},
    {"symbol": "BTC-JPY", "name": "ビットコイン / 円",  "group": "crypto", "kind": "px"},
    {"symbol": "ETH-JPY", "name": "イーサリアム / 円",  "group": "crypto", "kind": "px"},
    {"symbol": "BTC-USD", "name": "ビットコイン / ドル", "group": "crypto", "kind": "px"},
    {"symbol": "ETH-USD", "name": "イーサリアム / ドル", "group": "crypto", "kind": "px"},
]

DEFAULT_NEWS_SOURCES = [
    {"id": "jp-business", "label": "Yahoo!経済", "category": "日本経済", "enabled": True,
     "url": "https://news.yahoo.co.jp/rss/topics/business.xml"},
    {"id": "nikkei-asia", "label": "日経Asia", "category": "アジア", "enabled": True,
     "url": "https://asia.nikkei.com/rss/feed/nar"},
    {"id": "yahoo-finance-us", "label": "Yahoo Finance US", "category": "米国株", "enabled": True,
     "url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC,^N225,^DJI&region=US&lang=en-US"},
    {"id": "google-macro", "label": "Google マクロ", "category": "マクロ", "enabled": True,
     "url": "https://news.google.com/rss/search?q=" +
            urllib.parse.quote("日経平均 OR 円相場 OR 米国株 OR 金利 OR 日銀 OR FRB") +
            "&hl=ja&gl=JP&ceid=JP:ja"},
    {"id": "google-realestate", "label": "Google 不動産", "category": "不動産", "enabled": True,
     "url": "https://news.google.com/rss/search?q=" +
            urllib.parse.quote("不動産 投資 OR REIT OR 住宅ローン金利 OR 地価") +
            "&hl=ja&gl=JP&ceid=JP:ja"},
    {"id": "google-fx", "label": "Google 為替", "category": "為替", "enabled": True,
     "url": "https://news.google.com/rss/search?q=" +
            urllib.parse.quote("為替 ドル円 OR ユーロ円 OR 円安 OR 円高 OR FX") +
            "&hl=ja&gl=JP&ceid=JP:ja"},
    {"id": "google-rates", "label": "Google 金利", "category": "金利", "enabled": True,
     "url": "https://news.google.com/rss/search?q=" +
            urllib.parse.quote("米国債 利回り OR 長期金利 OR 中央銀行 OR FOMC OR 日銀") +
            "&hl=ja&gl=JP&ceid=JP:ja"},
    {"id": "google-commodities", "label": "Google 商品", "category": "商品", "enabled": True,
     "url": "https://news.google.com/rss/search?q=" +
            urllib.parse.quote("原油 金 先物 コモディティ OR OPEC OR 天然ガス") +
            "&hl=ja&gl=JP&ceid=JP:ja"},
    {"id": "google-crypto", "label": "Google Crypto", "category": "暗号資産", "enabled": True,
     "url": "https://news.google.com/rss/search?q=" +
            urllib.parse.quote("ビットコイン OR イーサリアム OR 暗号資産 OR Bitcoin") +
            "&hl=ja&gl=JP&ceid=JP:ja"},
]

_news_sources = []

# ---------------------------------------------------------------------------
# 銘柄の永続化（instruments.json）— 書込みは _inst_lock(RLock) 配下で read-modify-write
# ---------------------------------------------------------------------------
_inst_lock = threading.RLock()
_instruments = []  # list[dict]


def _normalize(inst):
    """1銘柄dictを検証・正規化。NGなら None。"""
    if not isinstance(inst, dict):
        return None
    sym = str(inst.get("symbol", "")).strip()
    name = str(inst.get("name", "")).strip()
    group = str(inst.get("group", "")).strip()
    kind = str(inst.get("kind", "")).strip()
    if not sym or not name:
        return None
    if not SYM_RE.match(sym):
        return None
    # 名前は HTML特殊文字(< > " ')と制御文字を禁止（XSS多層防御。& と日本語は許可）
    if any(c in NAME_BAD for c in name) or any(ord(c) < 0x20 for c in name):
        return None
    if len(name) > 40:
        return None
    if group not in VALID_GROUPS:
        group = "world"
    if kind not in VALID_KINDS:
        kind = DEFAULT_KIND_BY_GROUP.get(group, "index")
    return {"symbol": sym, "name": name, "group": group, "kind": kind}


def _dedup(items):
    """symbol(大文字無視)で一意化。先勝ち。"""
    seen, out = set(), []
    for x in items:
        k = x["symbol"].upper()
        if k not in seen:
            seen.add(k)
            out.append(x)
    return out


def _save_to_disk(items):
    """原子的保存（G:=Google Drive同期フォルダのため tmp→os.replace で半端書込みを防ぐ）。"""
    try:
        tmp = CONFIG_PATH.with_name(CONFIG_PATH.name + ".tmp")
        tmp.write_text(json.dumps({"instruments": items}, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        os.replace(tmp, CONFIG_PATH)
        return True
    except Exception:
        return False


def load_instruments():
    global _instruments
    items = None
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            raw = data.get("instruments") if isinstance(data, dict) else data
            if isinstance(raw, list):
                items = _dedup([n for n in (_normalize(x) for x in raw) if n])
        except Exception:
            items = None
    if not items:
        items = [dict(x) for x in DEFAULT_INSTRUMENTS]
        _save_to_disk(items)
    with _inst_lock:
        _instruments = items


def get_instruments():
    with _inst_lock:
        return [dict(x) for x in _instruments]


def _normalize_news_source(src):
    if not isinstance(src, dict):
        return None
    sid = re.sub(r"[^a-zA-Z0-9_-]", "", str(src.get("id", "")).strip())[:40]
    label = str(src.get("label", "")).strip()[:32]
    category = str(src.get("category", "")).strip()[:24]
    url = str(src.get("url", "")).strip()
    enabled = bool(src.get("enabled", True))
    if not sid or not label or not category:
        return None
    if not (url.startswith("https://") or url.startswith("http://")):
        return None
    return {"id": sid, "label": label, "category": category, "url": url, "enabled": enabled}


def _save_news_sources(items):
    try:
        tmp = NEWS_CONFIG_PATH.with_name(NEWS_CONFIG_PATH.name + ".tmp")
        tmp.write_text(json.dumps({"sources": items}, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        os.replace(tmp, NEWS_CONFIG_PATH)
        return True
    except Exception:
        return False


def load_news_sources():
    global _news_sources
    items = None
    if NEWS_CONFIG_PATH.exists():
        try:
            data = json.loads(NEWS_CONFIG_PATH.read_text(encoding="utf-8"))
            raw = data.get("sources") if isinstance(data, dict) else data
            if isinstance(raw, list):
                items = [n for n in (_normalize_news_source(x) for x in raw) if n]
        except Exception:
            items = None
    if not items:
        items = [dict(x) for x in DEFAULT_NEWS_SOURCES]
        _save_news_sources(items)
    _news_sources = items


def get_news_sources():
    if not _news_sources:
        load_news_sources()
    return [dict(x) for x in _news_sources]


# ---------------------------------------------------------------------------
# 簡易 TTL キャッシュ
# ---------------------------------------------------------------------------
_cache = {}
_cache_lock = threading.Lock()


def cache_get(key, ttl):
    with _cache_lock:
        v = _cache.get(key)
        if v and (time.time() - v[0]) < ttl:
            return v[1]
    return None


def cache_set(key, val):
    with _cache_lock:
        _cache[key] = (time.time(), val)


def cache_clear(key):
    with _cache_lock:
        _cache.pop(key, None)


def http_get(url, timeout=8):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "application/json,text/xml,application/xml,application/rss+xml,*/*",
        "Accept-Language": "ja,en;q=0.8",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


# ---------------------------------------------------------------------------
# マーケットデータ取得（Yahoo Finance v8 chart）
# ---------------------------------------------------------------------------
def fetch_quote(inst):
    """1銘柄(dict)を取得して整形 dict を返す。失敗時は ok=False。"""
    sym = inst["symbol"]
    encoded = urllib.parse.quote(sym, safe="")
    last_err = None
    for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        url = (f"https://{host}/v8/finance/chart/{encoded}"
               f"?range=1d&interval=5m&includePrePost=false")
        try:
            data = json.loads(http_get(url, timeout=8))
            res = (data.get("chart") or {}).get("result")
            if not res:
                continue
            r0 = res[0]
            meta = r0.get("meta", {})
            closes = []
            try:
                q = r0["indicators"]["quote"][0]["close"]
                closes = [c for c in q if c is not None]
            except Exception:
                closes = []
            price = meta.get("regularMarketPrice")
            if price is None and closes:
                price = closes[-1]
            prev = meta.get("chartPreviousClose") or meta.get("previousClose")
            if prev is None and len(closes) > 1:
                prev = closes[0]
            if price is None or prev is None:
                continue
            change = price - prev
            pct = (change / prev * 100.0) if prev else 0.0
            spark = closes[-120:]
            if len(spark) > 60:
                step = len(spark) / 60.0
                spark = [spark[int(i * step)] for i in range(60)]
            spark = [round(x, 4) for x in spark]
            return {
                "symbol": sym, "name": inst["name"],
                "group": inst["group"], "kind": inst["kind"],
                "price": round(price, 4), "prev": round(prev, 4),
                "change": round(change, 4), "changePct": round(pct, 3),
                "currency": meta.get("currency", ""),
                "marketState": meta.get("marketState", ""),
                "spark": spark, "ok": True,
            }
        except Exception as e:  # noqa
            last_err = str(e)
            continue
    return {"symbol": sym, "name": inst["name"], "group": inst["group"],
            "kind": inst["kind"], "ok": False, "error": last_err}


def build_markets():
    cached = cache_get("markets", ttl=30)
    if cached:
        return cached
    instruments = get_instruments()
    results = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(fetch_quote, ins): ins["symbol"] for ins in instruments}
        for fut in futs:
            try:
                q = fut.result()
            except Exception:
                continue
            results[q["symbol"]] = q
    groups = []
    for gid, glabel in GROUP_LABELS:
        items = [results[ins["symbol"]] for ins in instruments
                 if ins["group"] == gid and ins["symbol"] in results]
        if items:
            groups.append({"id": gid, "label": glabel, "items": items})
    payload = {"updated": int(time.time()), "groups": groups}
    cache_set("markets", payload)
    return payload


# ---------------------------------------------------------------------------
# 銘柄の追加・削除・並べ替え（read-modify-write を _inst_lock 配下で一貫実行）
# ---------------------------------------------------------------------------
def _result_with_save(res, saved):
    if not saved:
        res["warning"] = "保存に失敗しました（メモリ上は反映済みですが再起動で消えます）"
    return res


def add_instrument(body):
    inst = _normalize(body)
    if not inst:
        return {"ok": False, "error": "入力が不正です（シンボルは英数.^=- のみ／表示名に < > \" ' は使えません）"}
    with _inst_lock:
        if any(x["symbol"].upper() == inst["symbol"].upper() for x in _instruments):
            return {"ok": False, "error": f"{inst['symbol']} は既に登録済みです"}
    # Yahoo 実在検証はロック外（最長16秒のため）
    probe = fetch_quote(inst)
    if not probe.get("ok"):
        return {"ok": False, "error": f"{inst['symbol']} はYahooで取得できませんでした。シンボルをご確認ください"}
    with _inst_lock:
        if any(x["symbol"].upper() == inst["symbol"].upper() for x in _instruments):
            return {"ok": False, "error": f"{inst['symbol']} は既に登録済みです"}
        _instruments.append(inst)
        saved = _save_to_disk(_instruments)
    cache_clear("markets")
    return _result_with_save(
        {"ok": True, "item": inst,
         "preview": {"price": probe["price"], "changePct": probe["changePct"]}}, saved)


def delete_instrument(symbol):
    symbol = str(symbol or "").strip()
    with _inst_lock:
        new = [x for x in _instruments if x["symbol"].upper() != symbol.upper()]
        if len(new) == len(_instruments):
            return {"ok": False, "error": "該当銘柄が見つかりません"}
        _instruments[:] = new
        saved = _save_to_disk(_instruments)
    cache_clear("markets")
    return _result_with_save({"ok": True, "removed": symbol}, saved)


def reorder_instruments(order):
    if not isinstance(order, list) or len(order) > 500:
        return {"ok": False, "error": "order が不正です"}
    seen, clean = set(), []
    for s in order:
        if isinstance(s, str) and s not in seen:
            seen.add(s)
            clean.append(s)
    with _inst_lock:
        by_sym = {x["symbol"]: x for x in _instruments}
        new = [by_sym[s] for s in clean if s in by_sym]
        for x in _instruments:           # order に無い既存銘柄は末尾温存
            if x["symbol"] not in seen:
                new.append(x)
        if not new:
            return {"ok": False, "error": "並べ替え結果が空です"}
        _instruments[:] = new
        saved = _save_to_disk(_instruments)
    cache_clear("markets")
    return _result_with_save({"ok": True, "count": len(new)}, saved)


# ---------------------------------------------------------------------------
# ニュース取得（RSS → JSON）
# ---------------------------------------------------------------------------
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _clean_text(s):
    if not s:
        return ""
    s = _TAG_RE.sub("", s)
    s = (s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
           .replace("&quot;", '"').replace("&#39;", "'").replace("&apos;", "'"))
    return _WS_RE.sub(" ", s).strip()


def fetch_feed(source, url):
    out = []
    try:
        raw = http_get(url, timeout=8)
        root = ET.fromstring(raw)
        items = root.findall(".//item")
        is_atom = False
        if not items:
            items = root.findall(".//{http://www.w3.org/2005/Atom}entry")
            is_atom = True
        for it in items[:25]:
            if is_atom:
                title_el = it.find("{http://www.w3.org/2005/Atom}title")
                link_el = it.find("{http://www.w3.org/2005/Atom}link")
                link = link_el.get("href") if link_el is not None else ""
                date_el = it.find("{http://www.w3.org/2005/Atom}updated")
            else:
                title_el = it.find("title")
                link_el = it.find("link")
                link = link_el.text if link_el is not None else ""
                date_el = it.find("pubDate")
            title = _clean_text(title_el.text if title_el is not None else "")
            if not title:
                continue
            ts = 0
            if date_el is not None and date_el.text:
                try:
                    ts = int(parsedate_to_datetime(date_el.text).timestamp())
                except Exception:
                    ts = 0
            out.append({"source": source, "title": title,
                        "link": (link or "").strip(), "ts": ts})
    except Exception:
        pass
    return out


def build_news(source_ids=None):
    wanted = {x for x in (source_ids or []) if x}
    feeds = [s for s in get_news_sources() if s.get("enabled", True)]
    if wanted:
        feeds = [s for s in feeds if s["id"] in wanted]
    ck = "news:" + ",".join(sorted(s["id"] for s in feeds))
    cached = cache_get(ck, ttl=120)
    if cached:
        return cached
    all_items = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(fetch_feed, src["label"], src["url"]) for src in feeds]
        for fut in futs:
            try:
                all_items.extend(fut.result())
            except Exception:
                continue
    seen, dedup = set(), []
    for it in all_items:
        k = it["title"][:60]
        if k in seen:
            continue
        seen.add(k)
        dedup.append(it)
    dedup.sort(key=lambda x: x["ts"], reverse=True)
    payload = {"updated": int(time.time()), "sources": [s["id"] for s in feeds], "items": dedup[:80]}
    cache_set(ck, payload)
    return payload


def _symbol_news_query(symbol, name=""):
    parts = []
    if symbol:
        parts.append(symbol)
        if symbol.endswith(".T") and symbol[:4].isdigit():
            parts.append(symbol[:4])
    if name:
        parts.append(name)
    joined = " OR ".join(parts) or symbol or name
    return joined.strip()


def build_symbol_news(symbol, name=""):
    symbol = (symbol or "").strip()
    name = _clean_text(name or "")[:80]
    if not symbol and not name:
        return {"updated": int(time.time()), "symbol": symbol, "name": name, "items": []}
    if symbol and not SYM_RE.match(symbol):
        return {"updated": int(time.time()), "symbol": symbol, "name": name, "items": []}
    ck = f"news-symbol:{symbol}:{name}"
    cached = cache_get(ck, ttl=120)
    if cached:
        return cached
    query = _symbol_news_query(symbol, name)
    feeds = [
        ("Google Symbol", "https://news.google.com/rss/search?q=" +
         urllib.parse.quote(query) + "&hl=ja&gl=JP&ceid=JP:ja"),
    ]
    if symbol:
        feeds.append(("Yahoo Finance", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=" +
                     urllib.parse.quote(symbol, safe="") + "&region=US&lang=en-US"))
    all_items = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [ex.submit(fetch_feed, src, url) for src, url in feeds]
        for fut in futs:
            try:
                all_items.extend(fut.result())
            except Exception:
                continue
    seen, dedup = set(), []
    for it in all_items:
        k = it["title"][:60]
        if k in seen:
            continue
        seen.add(k)
        dedup.append(it)
    dedup.sort(key=lambda x: x["ts"], reverse=True)
    payload = {"updated": int(time.time()), "symbol": symbol, "name": name,
               "query": query, "items": dedup[:50]}
    cache_set(ck, payload)
    return payload


# ---------------------------------------------------------------------------
# 銘柄検索 — ローカル銘柄インデックス(symbols.json)による多言語オートコンプリート
#   かな/カタカナ/漢字/ローマ字/コード/日本語別名 を横断マッチ（Yahoo検索は日本語不可）
# ---------------------------------------------------------------------------
_symbols = []  # 検索母集団（_sym/_name/_kana/_en/_alias を前計算）

EXTRA_SYMBOLS = [
    # 商品先物
    {"symbol": "PL=F", "name": "プラチナ先物", "kana": "プラチナ", "en": "Platinum Futures",
     "aliases": ["白金", "platinum"], "group": "commodity", "kind": "px", "exch": "NYMEX"},
    {"symbol": "PA=F", "name": "パラジウム先物", "kana": "パラジウム", "en": "Palladium Futures",
     "aliases": ["palladium"], "group": "commodity", "kind": "px", "exch": "NYMEX"},
    {"symbol": "ZC=F", "name": "とうもろこし先物", "kana": "トウモロコシ", "en": "Corn Futures",
     "aliases": ["corn", "コーン"], "group": "commodity", "kind": "px", "exch": "CBOT"},
    {"symbol": "ZS=F", "name": "大豆先物", "kana": "ダイズ", "en": "Soybean Futures",
     "aliases": ["soybean", "soybeans"], "group": "commodity", "kind": "px", "exch": "CBOT"},
    {"symbol": "ZW=F", "name": "小麦先物", "kana": "コムギ", "en": "Wheat Futures",
     "aliases": ["wheat"], "group": "commodity", "kind": "px", "exch": "CBOT"},
    {"symbol": "KC=F", "name": "コーヒー先物", "kana": "コーヒー", "en": "Coffee Futures",
     "aliases": ["coffee"], "group": "commodity", "kind": "px", "exch": "ICE"},
    {"symbol": "CC=F", "name": "ココア先物", "kana": "ココア", "en": "Cocoa Futures",
     "aliases": ["cocoa"], "group": "commodity", "kind": "px", "exch": "ICE"},
    {"symbol": "CT=F", "name": "綿花先物", "kana": "メンカ", "en": "Cotton Futures",
     "aliases": ["cotton"], "group": "commodity", "kind": "px", "exch": "ICE"},
    {"symbol": "SB=F", "name": "砂糖先物", "kana": "サトウ", "en": "Sugar Futures",
     "aliases": ["sugar"], "group": "commodity", "kind": "px", "exch": "ICE"},
    {"symbol": "LE=F", "name": "生牛先物", "kana": "ナマウシ", "en": "Live Cattle Futures",
     "aliases": ["live cattle", "cattle"], "group": "commodity", "kind": "px", "exch": "CME"},

    # マイナー通貨・クロス円
    {"symbol": "CHFJPY=X", "name": "スイスフラン / 円", "kana": "スイスフランエン", "en": "CHF/JPY",
     "aliases": ["スイス円", "franc yen"], "group": "fx", "kind": "fx", "exch": "FX"},
    {"symbol": "CADJPY=X", "name": "カナダドル / 円", "kana": "カナダドルエン", "en": "CAD/JPY",
     "aliases": ["カナダ円", "loonie yen"], "group": "fx", "kind": "fx", "exch": "FX"},
    {"symbol": "NZDJPY=X", "name": "NZドル / 円", "kana": "ニュージーランドドルエン", "en": "NZD/JPY",
     "aliases": ["キウイ円"], "group": "fx", "kind": "fx", "exch": "FX"},
    {"symbol": "MXNJPY=X", "name": "メキシコペソ / 円", "kana": "メキシコペソエン", "en": "MXN/JPY",
     "aliases": ["ペソ円"], "group": "fx", "kind": "fx", "exch": "FX"},
    {"symbol": "ZARJPY=X", "name": "南アランド / 円", "kana": "ランドエン", "en": "ZAR/JPY",
     "aliases": ["ランド円", "南アフリカランド"], "group": "fx", "kind": "fx", "exch": "FX"},
    {"symbol": "TRYJPY=X", "name": "トルコリラ / 円", "kana": "トルコリラエン", "en": "TRY/JPY",
     "aliases": ["リラ円"], "group": "fx", "kind": "fx", "exch": "FX"},
    {"symbol": "NOKJPY=X", "name": "ノルウェークローネ / 円", "kana": "ノルウェークローネエン", "en": "NOK/JPY",
     "aliases": ["ノルウェー円"], "group": "fx", "kind": "fx", "exch": "FX"},
    {"symbol": "SEKJPY=X", "name": "スウェーデンクローナ / 円", "kana": "スウェーデンクローナエン", "en": "SEK/JPY",
     "aliases": ["スウェーデン円"], "group": "fx", "kind": "fx", "exch": "FX"},
    {"symbol": "SGDJPY=X", "name": "シンガポールドル / 円", "kana": "シンガポールドルエン", "en": "SGD/JPY",
     "aliases": ["シンガポール円"], "group": "fx", "kind": "fx", "exch": "FX"},

    # 海外株・ETF・国別指数代替
    {"symbol": "TSM", "name": "台湾セミコンダクター ADR", "kana": "タイワンセミコンダクター", "en": "Taiwan Semiconductor ADR",
     "aliases": ["TSMC", "台湾半導体"], "group": "world", "kind": "index", "exch": "NYSE"},
    {"symbol": "ASML", "name": "ASML Holding", "kana": "エーエスエムエル", "en": "ASML Holding",
     "aliases": ["半導体装置"], "group": "world", "kind": "index", "exch": "NASDAQ"},
    {"symbol": "BABA", "name": "Alibaba ADR", "kana": "アリババ", "en": "Alibaba Group ADR",
     "aliases": ["阿里巴巴"], "group": "world", "kind": "index", "exch": "NYSE"},
    {"symbol": "EWJ", "name": "iShares MSCI Japan ETF", "kana": "ニホンETF", "en": "iShares MSCI Japan ETF",
     "aliases": ["日本株ETF"], "group": "world", "kind": "index", "exch": "NYSE Arca"},
    {"symbol": "EWY", "name": "iShares MSCI Korea ETF", "kana": "カンコクETF", "en": "iShares MSCI South Korea ETF",
     "aliases": ["韓国ETF"], "group": "world", "kind": "index", "exch": "NYSE Arca"},
    {"symbol": "EWT", "name": "iShares MSCI Taiwan ETF", "kana": "タイワンETF", "en": "iShares MSCI Taiwan ETF",
     "aliases": ["台湾ETF"], "group": "world", "kind": "index", "exch": "NYSE Arca"},
    {"symbol": "EWZ", "name": "iShares MSCI Brazil ETF", "kana": "ブラジルETF", "en": "iShares MSCI Brazil ETF",
     "aliases": ["ブラジル株"], "group": "world", "kind": "index", "exch": "NYSE Arca"},
    {"symbol": "INDA", "name": "iShares MSCI India ETF", "kana": "インドETF", "en": "iShares MSCI India ETF",
     "aliases": ["インド株"], "group": "world", "kind": "index", "exch": "NYSE Arca"},
    {"symbol": "EEM", "name": "iShares MSCI Emerging Markets ETF", "kana": "シンコウコクETF", "en": "Emerging Markets ETF",
     "aliases": ["新興国株"], "group": "world", "kind": "index", "exch": "NYSE Arca"},

    # 金利・債券関連 ETF
    {"symbol": "TLT", "name": "米長期国債 ETF", "kana": "ベイチョウキコクサイ", "en": "iShares 20+ Year Treasury Bond ETF",
     "aliases": ["米国債20年", "long treasury"], "group": "rates", "kind": "px", "exch": "NASDAQ"},
    {"symbol": "IEF", "name": "米7-10年国債 ETF", "kana": "ベイコクサイ", "en": "iShares 7-10 Year Treasury Bond ETF",
     "aliases": ["米中期国債"], "group": "rates", "kind": "px", "exch": "NASDAQ"},
    {"symbol": "SHY", "name": "米短期国債 ETF", "kana": "ベイタンキコクサイ", "en": "iShares 1-3 Year Treasury Bond ETF",
     "aliases": ["米短期債"], "group": "rates", "kind": "px", "exch": "NASDAQ"},
    {"symbol": "TIP", "name": "米物価連動国債 ETF", "kana": "ベイブッカレンドウコクサイ", "en": "iShares TIPS Bond ETF",
     "aliases": ["TIPS", "物価連動債"], "group": "rates", "kind": "px", "exch": "NYSE Arca"},
    {"symbol": "HYG", "name": "米ハイイールド債 ETF", "kana": "ハイイールド", "en": "iShares iBoxx High Yield Corporate Bond ETF",
     "aliases": ["ジャンク債", "credit"], "group": "rates", "kind": "px", "exch": "NYSE Arca"},
]


def _fold(s):
    """NFKC(全角→半角)＋ひらがな→カタカナ＋小文字化。クエリ・フィールド共通の正規化。"""
    s = unicodedata.normalize("NFKC", s or "")
    s = "".join(chr(ord(c) + 0x60) if 0x3041 <= ord(c) <= 0x3096 else c for c in s)
    return s.lower()


def load_symbols():
    global _symbols
    path = ROOT / "symbols.json"
    items = []
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for s in data.get("symbols", []):
                s["_sym"] = _fold(s.get("symbol", ""))
                s["_name"] = _fold(s.get("name", ""))
                s["_kana"] = _fold(s.get("kana", ""))
                s["_en"] = _fold(s.get("en", ""))
                s["_alias"] = [_fold(a) for a in s.get("aliases", [])]
                items.append(s)
        except Exception as e:  # noqa
            print(f"[WARN] symbols.json 読込失敗: {e}", file=sys.stderr)
    by_sym = {str(s.get("symbol", "")).upper(): s for s in items if s.get("symbol")}
    for s in EXTRA_SYMBOLS:
        if str(s.get("symbol", "")).upper() not in by_sym:
            x = dict(s)
            x["_sym"] = _fold(x.get("symbol", ""))
            x["_name"] = _fold(x.get("name", ""))
            x["_kana"] = _fold(x.get("kana", ""))
            x["_en"] = _fold(x.get("en", ""))
            x["_alias"] = [_fold(a) for a in x.get("aliases", [])]
            items.append(x)
    _symbols = items


def _score(s, q):
    sc = 0
    sym = s["_sym"]
    if sym == q:
        return 1000
    if sym.startswith(q):
        sc = 200
    elif q in sym:
        sc = 70
    for a in s["_alias"]:
        if a == q:
            return 900
        if a.startswith(q):
            sc = max(sc, 190)
        elif q in a:
            sc = max(sc, 88)
    k = s["_kana"]
    if k.startswith(q):
        sc = max(sc, 180)
    elif q in k:
        sc = max(sc, 90)
    n = s["_name"]
    if n.startswith(q):
        sc = max(sc, 170)
    elif q in n:
        sc = max(sc, 85)
    e = s["_en"]
    if e.startswith(q):
        sc = max(sc, 160)
    elif q in e:
        sc = max(sc, 55)
    return sc


def local_search(q):
    qk = _fold((q or "").strip())
    if not qk:
        return {"query": q, "results": []}
    scored = []
    for s in _symbols:
        sc = _score(s, qk)
        if sc > 0:
            scored.append((sc, len(s["symbol"]), s))
    scored.sort(key=lambda x: (-x[0], x[1]))
    results = [{"symbol": s["symbol"], "name": s["name"] or s["en"],
                "exch": s.get("exch", ""), "type": s.get("group", "")}
               for _, _, s in scored[:25]]
    # ASCIIクエリで結果が乏しければ Yahoo で補完（FX/暗号/海外株などインデックス外）
    if len(results) < 6 and qk.isascii():
        try:
            have = {x["symbol"] for x in results}
            for r in yahoo_search(q).get("results", []):
                if r["symbol"] not in have:
                    results.append(r)
        except Exception:
            pass
    return {"query": q, "results": results[:25]}


def yahoo_search(q):
    q = (q or "").strip()
    if not q:
        return {"query": q, "results": []}
    ck = "search:" + q.lower()
    cached = cache_get(ck, ttl=300)
    if cached:
        return cached
    out = []
    enc = urllib.parse.quote(q, safe="")
    for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        url = (f"https://{host}/v1/finance/search?q={enc}"
               f"&quotesCount=12&newsCount=0&lang=ja-JP&region=JP")
        try:
            data = json.loads(http_get(url, timeout=6))
            for it in (data.get("quotes") or []):
                sym = it.get("symbol")
                if not sym:
                    continue
                out.append({
                    "symbol": sym,
                    "name": it.get("shortname") or it.get("longname") or sym,
                    "exch": it.get("exchDisp") or it.get("exchange") or "",
                    "type": it.get("quoteType") or "",
                })
            break
        except Exception:
            continue
    payload = {"query": q, "results": out}
    cache_set(ck, payload)
    return payload


# ---------------------------------------------------------------------------
# ヒストリカル足（Yahoo v8 chart）— v3 ローソク足チャート用
# ---------------------------------------------------------------------------
_RANGE_OK = {"1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "max"}
_INTERVAL_OK = {"2m", "5m", "15m", "30m", "60m", "1h", "1d", "1wk", "1mo"}


def yahoo_history(symbol, rng, interval):
    symbol = (symbol or "").strip()
    if not SYM_RE.match(symbol):
        return {"ok": False, "error": "bad symbol"}
    rng = rng if rng in _RANGE_OK else "6mo"
    interval = interval if interval in _INTERVAL_OK else "1d"
    ck = f"hist:{symbol}:{rng}:{interval}"
    cached = cache_get(ck, ttl=60)
    if cached:
        return cached
    enc = urllib.parse.quote(symbol, safe="")
    for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        url = (f"https://{host}/v8/finance/chart/{enc}"
               f"?range={rng}&interval={interval}")
        try:
            data = json.loads(http_get(url, timeout=8))
            res = (data.get("chart") or {}).get("result")
            if not res:
                continue
            r0 = res[0]
            meta = r0.get("meta", {})
            ts = r0.get("timestamp") or []
            q = (r0.get("indicators", {}).get("quote") or [{}])[0]
            o, h, l, c = q.get("open", []), q.get("high", []), q.get("low", []), q.get("close", [])
            v = q.get("volume", [])
            candles = []
            for i in range(len(ts)):
                if i >= len(c) or c[i] is None or o[i] is None:
                    continue
                candles.append({
                    "t": ts[i],
                    "o": round(o[i], 4), "h": round(h[i], 4),
                    "l": round(l[i], 4), "c": round(c[i], 4),
                    "v": (v[i] or 0) if i < len(v) else 0,
                })
            payload = {"ok": True, "symbol": symbol,
                       "currency": meta.get("currency", ""),
                       "name": meta.get("shortName") or meta.get("symbol") or symbol,
                       "candles": candles}
            cache_set(ck, payload)
            return payload
        except Exception:
            continue
    return {"ok": False, "error": "fetch failed", "symbol": symbol}


# ---------------------------------------------------------------------------
# HTTP ハンドラ
# ---------------------------------------------------------------------------
STATIC_TYPES = {
    ".png": "image/png", ".svg": "image/svg+xml", ".ico": "image/x-icon",
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp",
    ".css": "text/css; charset=utf-8", ".js": "text/javascript; charset=utf-8",
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", PUBLIC_ORIGIN)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.wfile.write(body)

    def do_OPTIONS(self):
        self._send(204, b"")

    def _json_body(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
            if n <= 0 or n > 65536:
                return {}
            return json.loads(self.rfile.read(n).decode("utf-8"))
        except Exception:
            return {}

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        try:
            if path in ("/", "/index.html"):
                spa = WEBROOT / "index.html"
                src = spa if spa.exists() else (ROOT / "index.html")
                self._send(200, src.read_bytes(), "text/html; charset=utf-8")
            elif path in ("/classic", "/classic.html"):
                self._send(200, (ROOT / "index.html").read_bytes(),
                           "text/html; charset=utf-8")
            elif path == "/api/markets":
                self._send(200, json.dumps(build_markets(), ensure_ascii=False))
            elif path == "/api/news":
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                symbol = (qs.get("symbol") or [""])[0]
                if symbol or (qs.get("name") or [""])[0]:
                    self._send(200, json.dumps(build_symbol_news(
                        symbol, (qs.get("name") or [""])[0]), ensure_ascii=False))
                    return
                source_ids = []
                for raw in (qs.get("source") or qs.get("sources") or []):
                    source_ids.extend([x.strip() for x in raw.split(",") if x.strip()])
                self._send(200, json.dumps(build_news(source_ids), ensure_ascii=False))
            elif path == "/api/news/sources":
                sources = get_news_sources()
                cats = sorted({s["category"] for s in sources})
                self._send(200, json.dumps(
                    {"sources": sources, "categories": cats}, ensure_ascii=False))
            elif path == "/api/config":
                self._send(200, json.dumps(
                    {"groups": GROUP_LABELS, "kinds": sorted(VALID_KINDS),
                     "readonly": READONLY, "instruments": get_instruments()},
                    ensure_ascii=False))
            elif path == "/api/search":
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                self._send(200, json.dumps(
                    local_search((qs.get("q") or [""])[0]), ensure_ascii=False))
            elif path == "/api/history":
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                self._send(200, json.dumps(yahoo_history(
                    (qs.get("symbol") or [""])[0],
                    (qs.get("range") or ["6mo"])[0],
                    (qs.get("interval") or ["1d"])[0]), ensure_ascii=False))
            elif path == "/api/health":
                self._send(200, json.dumps({"ok": True, "ts": int(time.time())}))
            else:
                # 静的ファイル配信（v3 dist 優先→ROOT）。パストラバーサル防止。
                rel = urllib.parse.unquote(path.lstrip("/"))
                ext = ("." + rel.rsplit(".", 1)[1]).lower() if "." in rel else ""
                if ext in STATIC_TYPES:
                    for base in (WEBROOT, ROOT):
                        if not base.exists():
                            continue
                        p = (base / rel).resolve()
                        try:
                            p.relative_to(base.resolve())
                        except ValueError:
                            continue
                        if p.is_file():
                            self._send(200, p.read_bytes(), STATIC_TYPES[ext])
                            return
                self._send(404, json.dumps({"error": "not found"}))
        except Exception as e:  # noqa
            self._send(500, json.dumps({"error": str(e)}))

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path.startswith("/api/instruments") and READONLY:
            self._send(403, json.dumps(
                {"ok": False, "error": "このサーバは閲覧専用モードです（銘柄編集は無効）"},
                ensure_ascii=False))
            return
        try:
            body = self._json_body()
            if path == "/api/instruments":
                self._send(200, json.dumps(add_instrument(body), ensure_ascii=False))
            elif path == "/api/instruments/delete":
                self._send(200, json.dumps(
                    delete_instrument(body.get("symbol")), ensure_ascii=False))
            elif path == "/api/instruments/reorder":
                self._send(200, json.dumps(
                    reorder_instruments(body.get("order")), ensure_ascii=False))
            else:
                self._send(404, json.dumps({"error": "not found"}))
        except Exception as e:  # noqa
            self._send(500, json.dumps({"error": str(e)}))


def main():
    load_instruments()
    load_symbols()
    load_news_sources()
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"APPLE CAPITAL MARKET TERMINAL v2")
    print(f"  bind: {HOST}:{PORT}  （ローカル: http://127.0.0.1:{PORT}/ ）")
    if HOST == "0.0.0.0":
        print(f"  ※ 0.0.0.0 待受 = 同一 LAN / Tailscale 内の他端末からも閲覧可")
    print(f"  銘柄数: {len(get_instruments())}  ニュースソース: {len(get_news_sources())}"
          f"  書込み: {'無効(READONLY)' if READONLY else '有効'}")
    print(f"  設定ファイル: {CONFIG_PATH}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()
