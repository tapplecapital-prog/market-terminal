# -*- coding: utf-8 -*-
"""
build_fundamentals.py — 企業ファンダメンタルズ スナップショット生成
================================================================
EDINET(金融庁) から主要日本株の財務指標を取得し fundamentals.json を生成する。
realestate.json / symbols.json と同じ「Claudeが生成 → repoにコミット → server.py静的配信」方式。

使い方:
    EDINET_API_KEY=<キー> python build_fundamentals.py
  （キーは公開リポジトリにコミットしない。env で渡す。キーの控えは HANDOFF_次セッション.md / .claude.json を参照）

依存: edinet_mcp（pip）。本スクリプトは edinet_mcp の EdinetClient + calculate_metrics を再利用するが、
同梱クライアントに 2 つの不具合があるため、ここで最小限のモンキーパッチを当てる:
  (1) httpx 経由のバイナリ取得が壊れたZIPを返す → urllib で取得し直す。
  (2) ダウンロードZIPのディスク書込み(cache.put_file)がZIPを破損させ BadZipFile になる → write_bytes で正しく保存。
更新頻度: 四半期〜半期（有報/四半期報告書の提出後）。地域追加と同様 COMPANIES を編集して再実行。
"""
import os, sys, json, asyncio, tempfile, pathlib, urllib.request, urllib.parse, datetime

if not os.environ.get("EDINET_API_KEY"):
    print("ERROR: set EDINET_API_KEY env var", file=sys.stderr)
    sys.exit(1)

from edinet_mcp.client import EdinetClient
from edinet_mcp._metrics import calculate_metrics

ROOT = pathlib.Path(__file__).resolve().parent
FUND = ROOT / "fundamentals.json"
PERIODS = ["2025", "2024"]  # 最新提出年から順に試す（3月決算=FY前年の有報）

# --- 同梱クライアントの不具合回避（バイナリ取得=urllib / ZIP保存=write_bytes） ---
async def _get_bytes_urllib(self, url, params):
    full = url + "?" + urllib.parse.urlencode(params)
    return await asyncio.to_thread(lambda: urllib.request.urlopen(full, timeout=120).read())
def _save_zip_binary(self, data, doc_id, cache_params, *, output_dir=None):
    p = pathlib.Path(tempfile.gettempdir()) / f"edinet_{doc_id}.zip"
    p.write_bytes(data)
    return p
EdinetClient._get_bytes = _get_bytes_urllib
EdinetClient._save_downloaded_zip = _save_zip_binary


def _pf(s):
    """'12.50%' / '1,234' / None -> float | None"""
    if s is None:
        return None
    try:
        return float(str(s).replace("%", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _extract(m):
    raw = m.get("raw_values", {}) or {}
    prof = m.get("profitability", {}) or {}
    stab = m.get("stability", {}) or {}
    cf = m.get("cash_flow", {}) or {}
    return {
        "revenue": raw.get("売上高"),
        "operating_income": raw.get("営業利益"),
        "net_income": raw.get("当期純利益"),
        "total_assets": raw.get("総資産"),
        "net_assets": raw.get("純資産"),
        "operating_cf": cf.get("営業CF"),
        "roe": _pf(prof.get("ROE")),
        "roa": _pf(prof.get("ROA")),
        "operating_margin": _pf(prof.get("営業利益率")),
        "equity_ratio": _pf(stab.get("自己資本比率")),
    }


def _fiscal_year(period_end):
    if not period_end:
        return None
    try:
        d = datetime.date.fromisoformat(str(period_end)[:10])
        return f"{d.year}年{d.month}月期"
    except ValueError:
        return None


async def fetch_one(client, item):
    code = item.get("edinet_code")
    if not code:
        return False
    for p in PERIODS:
        try:
            stmt = await client.get_financial_statements(edinet_code=code, doc_type="annual_report", period=p)
            metrics = _extract(dict(calculate_metrics(stmt)))
            pe = stmt.filing.period_end.isoformat() if stmt.filing.period_end else None
            item["fiscalYear"] = _fiscal_year(pe)
            item["dataAvailable"] = metrics.get("net_income") is not None or metrics.get("total_assets") is not None
            item["metrics"] = metrics
            print(f"  OK {item['ticker']:>5} {item['name']:<16} period={p} pe={pe}")
            return item["dataAvailable"]
        except Exception as e:
            err = repr(e)
            continue
    item["fiscalYear"] = None
    item["dataAvailable"] = False
    item["metrics"] = None
    print(f"  -- {item['ticker']:>5} {item['name']:<16} no data ({err})")
    return False


async def main():
    data = json.loads(FUND.read_text(encoding="utf-8"))
    items = data.get("items", {})
    client = EdinetClient()
    ok = 0
    try:
        for ticker, item in items.items():
            if await fetch_one(client, item):
                ok += 1
    finally:
        await client.close()
    data["updated"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    data["source"] = "EDINET (金融庁)"
    data["anyData"] = ok > 0
    FUND.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nWROTE {FUND}  ({ok}/{len(items)} companies with data)")


asyncio.run(main())
