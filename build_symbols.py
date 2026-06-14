# -*- coding: utf-8 -*-
"""
銘柄マスタ symbols.json をビルドする（多言語・高精度な銘柄検索の母集団）。
=======================================================================
データ源（いずれも無料・実URL裏取り済）:
  - 日本株: EDINET コードリスト(EdinetcodeDlInfo.csv) … コード＋漢字社名＋**かな読み(ヨミ)**＋英語名
  - 米国株: Nasdaq Trader シンボルディレクトリ(nasdaqlisted/otherlisted) … 全上場＋ETF
  - 既存の指数/為替/金利/コモディティ/暗号(DEFAULT_INSTRUMENTS) と
    主要米国株の日本語別名(US_ALIASES) を合流

Yahoo検索は日本語(かな/漢字)を受け付けない(HTTP 400)ため、この自前インデックスで
「とよた」「ソニー」「任天堂」「アップル」「7203」等を全部引けるようにする。

実行: python build_symbols.py   → 同フォルダに symbols.json を生成
標準ライブラリのみ。
"""
import csv
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "symbols.json"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

EDINET_ZIP = "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip"
NASDAQ_LISTED = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

EXCH_MAP = {"N": "NYSE", "A": "NYSE American", "P": "NYSE Arca", "Z": "Cboe BZX", "V": "IEX"}

# 主要米国株の日本語別名（カタカナで検索→ティッカー解決。ローマ字では解けない通称を補う）
US_ALIASES = [
    ("アップル", "AAPL"), ("マイクロソフト", "MSFT"), ("エヌビディア", "NVDA"), ("アマゾン", "AMZN"),
    ("グーグル", "GOOGL"), ("アルファベット", "GOOGL"), ("メタ", "META"), ("フェイスブック", "META"),
    ("テスラ", "TSLA"), ("ネットフリックス", "NFLX"), ("ネトフリ", "NFLX"), ("バークシャー", "BRK-B"),
    ("バークシャーハサウェイ", "BRK-B"), ("ブロードコム", "AVGO"), ("アバゴ", "AVGO"), ("イーライリリー", "LLY"),
    ("リリー", "LLY"), ("ジェーピーモルガン", "JPM"), ("JPモルガン", "JPM"), ("ビザ", "V"),
    ("マスターカード", "MA"), ("ウォルマート", "WMT"), ("ユナイテッドヘルス", "UNH"), ("エクソンモービル", "XOM"),
    ("エクソン", "XOM"), ("ジョンソンエンドジョンソン", "JNJ"), ("プロクターアンドギャンブル", "PG"),
    ("ピーアンドジー", "PG"), ("オラクル", "ORCL"), ("ホームデポ", "HD"), ("コストコ", "COST"),
    ("アッヴィ", "ABBV"), ("エーエムディー", "AMD"), ("コカコーラ", "KO"), ("ペプシコ", "PEP"), ("ペプシ", "PEP"),
    ("セールスフォース", "CRM"), ("バンクオブアメリカ", "BAC"), ("シェブロン", "CVX"), ("マクドナルド", "MCD"),
    ("アクセンチュア", "ACN"), ("メルク", "MRK"), ("ウォルトディズニー", "DIS"), ("ディズニー", "DIS"),
    ("シスコ", "CSCO"), ("アドビ", "ADBE"), ("クアルコム", "QCOM"), ("テキサスインスツルメンツ", "TXN"),
    ("アムジェン", "AMGN"), ("ファイザー", "PFE"), ("インテル", "INTC"), ("アイビーエム", "IBM"),
    ("アメリカンエキスプレス", "AXP"), ("アメックス", "AXP"), ("ボーイング", "BA"), ("ナイキ", "NKE"),
    ("スターバックス", "SBUX"), ("スタバ", "SBUX"), ("ペイパル", "PYPL"), ("ウーバー", "UBER"),
    ("ゴールドマンサックス", "GS"), ("ゴールドマン", "GS"), ("モルガンスタンレー", "MS"), ("キャタピラー", "CAT"),
    ("ハネウェル", "HON"), ("ロッキードマーチン", "LMT"), ("スリーエム", "MMM"), ("フォード", "F"),
    ("ゼネラルモーターズ", "GM"), ("ベライゾン", "VZ"), ("コムキャスト", "CMCSA"), ("パランティア", "PLTR"),
    ("スノーフレイク", "SNOW"), ("クラウドストライク", "CRWD"), ("サービスナウ", "NOW"), ("インテュイット", "INTU"),
    ("ブッキング", "BKNG"), ("エアビーアンドビー", "ABNB"), ("エアビー", "ABNB"), ("モデルナ", "MRNA"),
    ("スポティファイ", "SPOT"), ("ショッピファイ", "SHOP"), ("ロビンフッド", "HOOD"), ("コインベース", "COIN"),
    ("マイクロストラテジー", "MSTR"), ("ストラテジー", "MSTR"), ("アーム", "ARM"), ("マイクロン", "MU"),
    ("アプライドマテリアルズ", "AMAT"), ("ラムリサーチ", "LRCX"), ("タイワンセミコンダクター", "TSM"),
    ("ティーエスエムシー", "TSM"), ("アリババ", "BABA"), ("バイドゥ", "BIDU"), ("ニオ", "NIO"),
    ("リビアン", "RIVN"), ("デルテクノロジーズ", "DELL"), ("スーパーマイクロ", "SMCI"),
    ("スーパーマイクロコンピューター", "SMCI"), ("クラウドフレア", "NET"), ("データドッグ", "DDOG"),
    ("アスムエル", "ASML"), ("エーエスエムエル", "ASML"), ("バンガード", "VOO"), ("スパイダー", "SPY"),
    ("インベスコQQQ", "QQQ"), ("トリプルキュー", "QQQ"),
]


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _strip_company(name, suffixes):
    s = name.strip()
    for suf in suffixes:
        if s.startswith(suf):
            s = s[len(suf):]
        if s.endswith(suf):
            s = s[:-len(suf)]
    return s.strip() or name.strip()


def load_jp():
    """EDINET コードリスト → 日本株(コード/漢字/かな/英語)。"""
    out = []
    try:
        raw = http_get(EDINET_ZIP, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(raw))
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        text = zf.read(csv_name).decode("cp932", errors="replace")
        lines = text.splitlines()
        # 1行目=メタ, 2行目=ヘッダー
        header = next(csv.reader([lines[1]]))
        idx = {h: i for i, h in enumerate(header)}
        ci = idx.get("証券コード")
        ni = idx.get("提出者名")
        ei = idx.get("提出者名（英字）")
        yi = idx.get("提出者名（ヨミ）")
        li = idx.get("上場区分")
        if None in (ci, ni, yi, li):
            raise ValueError(f"EDINET header mismatch: {header}")
        for row in csv.reader(lines[2:]):
            if len(row) <= max(ci, ni, ei or 0, yi, li):
                continue
            if row[li].strip() != "上場":
                continue
            code = row[ci].strip()
            if not code:
                continue
            ticker = code[:4]
            if len(ticker) < 4:
                continue
            name = _strip_company(row[ni], ["株式会社"])
            kana = _strip_company(row[yi], ["カブシキガイシャ", "カブシキカイシャ",
                                            "カブシキガイシヤ", "カブシキカイシヤ"])
            en = (row[ei].strip() if ei is not None else "")
            out.append({
                "symbol": ticker + ".T", "name": name, "kana": kana,
                "en": en, "aliases": [], "group": "jp", "kind": "index", "exch": "東証",
            })
    except Exception as e:
        print(f"[WARN] EDINET 取得失敗: {e}", file=sys.stderr)
    return out


def _parse_nasdaq(text, sym_i, name_i, test_i, etf_i, exch_val=None, exch_i=None):
    out = []
    lines = text.splitlines()
    for ln in lines[1:]:
        if ln.startswith("File Creation Time"):
            continue
        p = ln.split("|")
        if len(p) <= max(sym_i, name_i, test_i, etf_i, exch_i or 0):
            continue
        if p[test_i].strip() == "Y":
            continue
        sym = p[sym_i].strip()
        if not sym or not all(c.isalnum() or c in ".-^" for c in sym):
            continue
        name = p[name_i].strip()
        disp = name.split(" - ")[0].strip() or name   # "Apple Inc. - Common Stock" → "Apple Inc."
        exch = exch_val or EXCH_MAP.get(p[exch_i].strip(), p[exch_i].strip()) if exch_i is not None else exch_val
        out.append({
            "symbol": sym, "name": disp[:60], "kana": "", "en": name,
            "aliases": [], "group": "world", "kind": "index", "exch": exch or "US",
            "etf": p[etf_i].strip() == "Y",
        })
    return out


def load_us():
    out = []
    try:
        t1 = http_get(NASDAQ_LISTED).decode("utf-8", errors="replace")
        out += _parse_nasdaq(t1, 0, 1, 3, 6, exch_val="NASDAQ")
    except Exception as e:
        print(f"[WARN] nasdaqlisted 取得失敗: {e}", file=sys.stderr)
    try:
        t2 = http_get(OTHER_LISTED).decode("utf-8", errors="replace")
        out += _parse_nasdaq(t2, 0, 1, 6, 4, exch_i=2)
    except Exception as e:
        print(f"[WARN] otherlisted 取得失敗: {e}", file=sys.stderr)
    return out


def load_curated():
    """既存の指数/為替/金利/コモディティ/暗号(DEFAULT_INSTRUMENTS)を流用。"""
    try:
        import server  # 同フォルダ
        return [{"symbol": x["symbol"], "name": x["name"], "kana": "", "en": x["name"],
                 "aliases": [], "group": x["group"], "kind": x["kind"], "exch": ""}
                for x in server.DEFAULT_INSTRUMENTS]
    except Exception as e:
        print(f"[WARN] curated 読込失敗: {e}", file=sys.stderr)
        return []


def main():
    t0 = time.time()
    jp = load_jp()
    us = load_us()
    curated = load_curated()
    print(f"  JP={len(jp)}  US={len(us)}  curated={len(curated)}")

    by_sym = {}
    # 採用順: curated(指数等) → US → JP（後勝ちを避け先勝ち）
    for e in curated + us + jp:
        by_sym.setdefault(e["symbol"].upper(), e)

    # 日本語別名を該当ティッカーへ付与（無ければ別名エントリを作る）
    for ja, sym in US_ALIASES:
        key = sym.upper()
        if key in by_sym:
            by_sym[key]["aliases"].append(ja)
        else:
            by_sym[key] = {"symbol": sym, "name": sym, "kana": "", "en": sym,
                           "aliases": [ja], "group": "world", "kind": "index", "exch": "US"}

    symbols = list(by_sym.values())
    if len(symbols) < 1000:
        print(f"[ERROR] 銘柄数が少なすぎます({len(symbols)})。既存 symbols.json を保持して中断。",
              file=sys.stderr)
        sys.exit(1)

    OUT.write_text(json.dumps({"updated": int(time.time()), "count": len(symbols),
                               "symbols": symbols}, ensure_ascii=False),
                   encoding="utf-8")
    print(f"✓ symbols.json 生成: {len(symbols)}銘柄  ({time.time()-t0:.1f}s)  → {OUT}")


if __name__ == "__main__":
    main()
