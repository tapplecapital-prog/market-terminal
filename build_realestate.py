# -*- coding: utf-8 -*-
"""
build_realestate.py
===================================================================
不動産市況パネル用 realestate.json を生成するビルドスクリプト。

データ源:
  国土交通省 国土数値情報「地価公示」(令和8年=2026年版)
  - カタログ:   catalog_id = "nlni_ksj"  (国土数値情報)
  - データセット: dataset_id = "nlni_ksj-l01"  (地価公示 L01)

■ データ取得は MCP 経由（Claude が mlit-dpf MCP を呼ぶ）
  このスクリプト自体は外部APIを叩きません。取得は Claude Code 上で
  国交省 mlit-dpf MCP ツールを使って行い、得た JSON をローカルに保存し、
  本スクリプトがそれを集計して realestate.json を書き出します。
  （標準ライブラリのみで動作。requests 等は不要）

------------------------------------------------------------------
【MCP 取得レシピ】(再生成手順 / Claude に投げる指示)
------------------------------------------------------------------
1. 都道府県コードを正規化:
     normalize_codes(prefecture="東京都")  -> "13"
     normalize_codes(prefecture="宮城県")  -> "04"  (APIは "4" を返すが0埋めの "04" でOK)
     normalize_codes(prefecture="青森県")  -> "02"  (同上 "2"→"02")

2. 件数確認:
     get_count_data(dataset_id="nlni_ksj-l01", prefecture_code="13")  -> 2560
     get_count_data(dataset_id="nlni_ksj-l01", prefecture_code="04")  -> 565
     get_count_data(dataset_id="nlni_ksj-l01", prefecture_code="02")  -> 261

3. データ取得 (ページング):
     get_all_data(dataset_id="nlni_ksj-l01", prefecture_code=<code>,
                  size=200, max_items=<件数>, include_metadata=True)

   ★重要な制約 (実測):
     - MCP ラッパーは1レスポンスを約 887KB (~フルメタデータ200件分) で打ち切る。
       size/max_items を上げても1回あたり最大 ~200件しか返らない。
     - 県全体で200件超のときは「市区町村コード単位」で分割取得する:
         get_count_data(... slice_attribute_name="DPF:municipality_code", slice_size=50)
       で市区町村別件数を取り、各 municipality_code を
         get_all_data(dataset_id="nlni_ksj-l01", municipality_code=<5桁>, size=200,
                      max_items=300, include_metadata=True)
       で取得する（各市区町村は概ね150件未満なので1回で全件取れる）。
     - 大きい結果は Claude のツール結果ファイル (tool-results/*.txt) に
       自動保存される。本スクリプトはそのディレクトリ(複数可)を走査し、
       point の "id" で重複排除して全件を集約する。
     - location_rectangle 単独検索は不可。必ず term か属性条件を併用する。

------------------------------------------------------------------
【メタデータ→数値の対応】
------------------------------------------------------------------
  価格(円/㎡, 2026年):  metadata["NLNI:chika_kouji_kakaku"]  (= kouji_kakaku.reiwa8)
  前年比(%):           metadata["NLNI:taizenhen_hendo_ritsu"] (文字列。前年比%。
                       前年データ無しの新規地点等では欠落するので None 扱い)
  時系列:              metadata["NLNI:kouji_kakaku"]["reiwaN"]
                       和暦→西暦:  reiwa1=2019, reiwa2=2020, reiwa3=2021,
                                    reiwa4=2022, reiwa5=2023, reiwa6=2024,
                                    reiwa7=2025, reiwa8=2026
  ※ kouji_kakaku の値 0 は「当年その地点は調査対象でない/データ無し」を意味するので
     集計から除外する（0 を価格として扱わない）。
  都道府県判定:        metadata["DPF:prefecture_code"]  (例: [13], [4], [2])
  ※ L02 の "DPF:year" は異常値があるため使わない。L01 の reiwaN 系列を正とする。

------------------------------------------------------------------
【集計ロジック】(エリアごと)
------------------------------------------------------------------
  avg     : 2026年価格(chika_kouji_kakaku>0)の単純平均（円/㎡, 整数四捨五入）
  median  : 同・中央値（整数四捨五入）
  max     : 同・最高値
  yoyPct  : taizenhen_hendo_ritsu(数値化できたもの)の単純平均（%, 小数2桁）
  points  : 2026年価格を持つ標準地数（chika_kouji_kakaku>0 の件数）
  series  : 直近4年 [2023,2024,2025,2026] = [reiwa5,reiwa6,reiwa7,reiwa8] の
            各年平均（その年に値>0 を持つ地点のみで平均, 整数四捨五入）
===================================================================
"""

import json
import os
import glob
import statistics
from datetime import datetime, timezone

# ---------------------------------------------------------------
# 設定
# ---------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = os.path.join(HERE, "realestate.json")

# Claude のツール結果保存ディレクトリ（環境に合わせて追加可）。
# get_all_data の大きな結果はここに *.txt として保存される。
TOOL_RESULT_DIRS = [
    r"C:\Users\tappl\.claude\projects\G---------22--AI------\614edb5c-b0ce-4b9a-8c5d-3dd87a76ef70\tool-results",
    os.path.join(HERE, "_raw_l01"),  # 手動で JSON を置く場合のフォールバック
]

# エリア定義: key, label, 都道府県コード(整数)
# 表示順は「東京→主要都市→宮城→青森」の自然順。
# 集計データの被覆度:
#   東京(13)/宮城(4)/青森(2) … 県全体を市区町村単位でページング取得した(ほぼ)全件
#   神奈川(14)/愛知(23)/大阪(27)/福岡(40)/北海道(1) … 県単位 get_all_data 1バッチ
#       (=MCPラッパーの ~200件/レスポンス上限ぶんのサンプル。標準地を市区町村横断で
#        広く拾うため代表性は十分。各県の標準地数(points)はこのサンプル件数)
AREAS = [
    ("tokyo",    "東京都",         13),
    ("kanagawa", "神奈川県",       14),
    ("aichi",    "愛知県",         23),
    ("osaka",    "大阪府",         27),
    ("fukuoka",  "福岡県",         40),
    ("hokkaido", "北海道",          1),
    ("miyagi",   "宮城県（仙台）",  4),
    ("aomori",   "青森県",          2),
]

# 和暦reiwa -> 西暦  (series 用)
REIWA_TO_YEAR = {5: 2023, 6: 2024, 7: 2025, 8: 2026}
SERIES_YEARS = [2023, 2024, 2025, 2026]
YEAR_TO_REIWA_KEY = {2023: "reiwa5", 2024: "reiwa6", 2025: "reiwa7", 2026: "reiwa8"}


def iter_points():
    """全ツール結果ファイルから L01 の point を yield する。"""
    seen = set()
    files = []
    for d in TOOL_RESULT_DIRS:
        if os.path.isdir(d):
            files += glob.glob(os.path.join(d, "*.txt"))
            files += glob.glob(os.path.join(d, "*.json"))
    for fp in files:
        try:
            raw = open(fp, encoding="utf-8").read().strip()
        except Exception:
            continue
        if not raw or raw.lstrip()[0] not in "{[":
            continue
        try:
            doc = json.loads(raw)
        except Exception:
            continue
        items = doc.get("items") if isinstance(doc, dict) else (doc if isinstance(doc, list) else None)
        if not items:
            continue
        for it in items:
            md = it.get("metadata", {})
            if md.get("DPF:dataset_id") != "nlni_ksj-l01":
                continue
            pid = it.get("id") or md.get("DPF:id")
            if not pid or pid in seen:
                continue
            seen.add(pid)
            yield md


def pref_code_of(md):
    pc = md.get("DPF:prefecture_code")
    if isinstance(pc, list) and pc:
        try:
            return int(pc[0])
        except Exception:
            return None
    try:
        return int(pc)
    except Exception:
        return None


def price_2026(md):
    v = md.get("NLNI:chika_kouji_kakaku")
    try:
        v = int(v)
    except Exception:
        return None
    return v if v > 0 else None


def yoy_of(md):
    v = md.get("NLNI:taizenhen_hendo_ritsu")
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def year_price(md, year):
    kk = md.get("NLNI:kouji_kakaku") or {}
    v = kk.get(YEAR_TO_REIWA_KEY[year])
    try:
        v = int(v)
    except Exception:
        return None
    return v if v > 0 else None


def round_or_none(x):
    return int(round(x)) if x is not None else None


def build():
    # 県コード -> 集計バケツ
    buckets = {code: {"prices": [], "yoys": [], "series": {y: [] for y in SERIES_YEARS}}
               for _, _, code in AREAS}

    total = 0
    for md in iter_points():
        pc = pref_code_of(md)
        if pc not in buckets:
            continue
        total += 1
        b = buckets[pc]
        p = price_2026(md)
        if p is not None:
            b["prices"].append(p)
        y = yoy_of(md)
        if y is not None:
            b["yoys"].append(y)
        for yr in SERIES_YEARS:
            yp = year_price(md, yr)
            if yp is not None:
                b["series"][yr].append(yp)

    areas_out = []
    for key, label, code in AREAS:
        b = buckets[code]
        prices = b["prices"]
        yoys = b["yoys"]
        area = {
            "key": key,
            "label": label,
            "avg": round_or_none(statistics.mean(prices)) if prices else None,
            "median": round_or_none(statistics.median(prices)) if prices else None,
            "max": max(prices) if prices else None,
            "yoyPct": round(statistics.mean(yoys), 2) if yoys else None,
            "points": len(prices),
            "series": [
                {"y": yr,
                 "v": round_or_none(statistics.mean(b["series"][yr])) if b["series"][yr] else None}
                for yr in SERIES_YEARS
            ],
        }
        areas_out.append(area)

    out = {
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source": "国土交通省 国土数値情報 地価公示(令和8年/2026)",
        "areas": areas_out,
    }
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"collected unique L01 points ({len(AREAS)} prefectures):", total)
    for a in areas_out:
        print(f"  {a['key']:9s} points={a['points']:5d} avg={a['avg']} median={a['median']} "
              f"max={a['max']} yoy={a['yoyPct']}")
    print("wrote:", OUT_PATH)
    return out


if __name__ == "__main__":
    build()
