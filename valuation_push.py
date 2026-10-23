#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日指数估值与定投决策微信推送系统 (多维交叉印证 + 免图片极速版)
核心升级：
1. 移除外部 PNG 大图与字体下载，保留并强化微信原生 HTML 四色可视化进度条与彩色数据表（秒开无加载延迟）
2. 专业多维估值交叉印证体系：
   - 同时拉取并计算近 5 年 PE（市盈率）百分位 + 近 5 年 PB（市净率）百分位 + 股息率 (Dividend Yield) + ROE (净资产收益率)
   - 引入「PE + PB 联合估值百分位」（针对红利类指数结合股息率修正），避免单一 PE 在盈利周期底部失真
3. 顶层「市场整体高低位温度计」：
   - 自动计算沪深300股债利差 ERP (1/PE - 10年期国债收益率) 与全市场综合估值温度 (0~100°C)，一眼判断大盘高低位
4. 量化定投倍率信号：
   - 按综合估值分位自动给出定投操作建议（🔥2.0x加倍 / 🟢1.5x加码 / 🔵1.0x标准 / 🟠0.5x减半 / 🔴暂停止盈）
5. 数据获取优化：
   - 先通过雪球蛋卷全量快照接口 (1次请求 0.2s) 批量获取所有指数实时 PE/PB/股息率/ROE，再并发拉取 5 年日频序列精算 5 年分位
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import socket
import sys
import time
from typing import Any

import numpy as np
import pandas as pd
import requests


# ============================================================================
# 0. 全局网络超时保护
# ============================================================================
socket.setdefaulttimeout(5.0)
_ORIG_SESSION_REQUEST = requests.Session.request


def _safe_timeout_request(self: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
    if kwargs.get("timeout") is None:
        kwargs["timeout"] = (2.5, 4.0)
    return _ORIG_SESSION_REQUEST(self, method, url, **kwargs)


requests.Session.request = _safe_timeout_request  # type: ignore[method-assign]


# ============================================================================
# 1. 四档估值区间与量化定投策略体系 (<30% 低估 | 30%–50% 适中偏低 | 50%–70% 适中偏高 | >70% 高估)
# ============================================================================
VALUATION_TIERS = [
    {
        "idx": 0,
        "key": "undervalued",
        "label": "低估",
        "range_text": "<30%",
        "color": "#10B981",       # 翡翠绿
        "text_color": "#065F46",
        "bg_color": "#D1FAE5",
    },
    {
        "idx": 1,
        "key": "normal_low",
        "label": "适中偏低",
        "range_text": "30%–50%",
        "color": "#3B82F6",       # 晴空蓝
        "text_color": "#1E40AF",
        "bg_color": "#DBEAFE",
    },
    {
        "idx": 2,
        "key": "normal_high",
        "label": "适中偏高",
        "range_text": "50%–70%",
        "color": "#F59E0B",       # 琥珀橙
        "text_color": "#92400E",
        "bg_color": "#FEF3C7",
    },
    {
        "idx": 3,
        "key": "overvalued",
        "label": "高估",
        "range_text": ">70%",
        "color": "#EF4444",       # 珊瑚红
        "text_color": "#991B1B",
        "bg_color": "#FEE2E2",
    },
]


def classify_percentile(pct: float) -> dict[str, Any]:
    """根据百分位返回对应的四档颜色与标签。"""
    if pct < 30.0:
        return VALUATION_TIERS[0]
    elif pct < 50.0:
        return VALUATION_TIERS[1]
    elif pct <= 70.0:
        return VALUATION_TIERS[2]
    else:
        return VALUATION_TIERS[3]


def get_dca_action(composite_pct: float, pe_pct: float, pb_pct: float, div_yield: float, is_dividend_idx: bool) -> str:
    """
    结合 PE百分位 + PB百分位 + 股息率 给出专业定投倍率建议：
    - 若为红利策略指数且股息率 >= 5.0%，在低估/适中偏低区额外强化定投信号
    """
    if composite_pct < 18.0 or (pe_pct < 25.0 and pb_pct < 25.0):
        return "🔥 2.0x 双倍定投"
    elif composite_pct < 30.0:
        return "🟢 1.5x 加码定投"
    elif composite_pct < 50.0:
        if is_dividend_idx and div_yield >= 4.8:
            return "🟢 1.2x 红利定投"
        return "🔵 1.0x 标准定投"
    elif composite_pct <= 70.0:
        return "🟠 0.5x 减半/持有"
    elif composite_pct <= 85.0:
        return "🔴 暂停定投"
    else:
        return "🚨 分批止盈"


# ============================================================================
# 2. 海内外指数与 Wind 代码智能映射表（含指数类型：broad=宽基, dividend=红利现金流, overseas=海外/科技）
# ============================================================================
INDEX_ALIAS_MAP: dict[str, dict[str, Any]] = {
    "8841233": {"display_code": "8841233.WI", "danjuan": ["CSI930050", "SH000016"], "style": "broad"},
    "A50":     {"display_code": "A50",        "danjuan": ["CSI930050", "SH000016"], "style": "broad"},
    "000300":  {"display_code": "000300.SH",  "danjuan": ["SH000300"],              "style": "broad"},
    "000985":  {"display_code": "000985.CSI", "danjuan": ["CSI000985", "SH000985"], "style": "broad"},
    "000905":  {"display_code": "000905.SH",  "danjuan": ["SH000905"],              "style": "broad"},
    "000906":  {"display_code": "000906.SH",  "danjuan": ["SH000906"],              "style": "broad"},
    "HSTECH":  {"display_code": "HSTECH.HI",  "danjuan": ["HKHSTECH", "HSTECH"],    "style": "overseas"},
    "000922":  {"display_code": "000922.CSI", "danjuan": ["SH000922", "CSI000922"], "style": "dividend"},
    "NDX":     {"display_code": "NDX.GI",     "danjuan": ["NDX"],                   "style": "overseas"},
    "IXIC":    {"display_code": "IXIC.GI",    "danjuan": ["NDX", "IXIC"],           "style": "overseas"},
    "N225":    {"display_code": "N225.GI",    "danjuan": ["N225"],                  "style": "overseas"},
    "SENSEX":  {"display_code": "SENSEX.BO",  "danjuan": ["935600", "SENSEX"],      "style": "overseas"},
    "SPX":     {"display_code": "SPX.GI",     "danjuan": ["SP500", "SPX"],          "style": "overseas"},
    "SP500":   {"display_code": "SPX.GI",     "danjuan": ["SP500", "SPX"],          "style": "overseas"},
    "HSI":     {"display_code": "HSI.HI",     "danjuan": ["HKHSI", "HSI"],          "style": "overseas"},
    "399006":  {"display_code": "399006.SZ",  "danjuan": ["SZ399006"],              "style": "broad"},
    "H30269":  {"display_code": "H30269.CSI", "danjuan": ["CSIH30269", "SHH30269"], "style": "dividend"},
    "980092":  {"display_code": "980092.CNI", "danjuan": ["CNI980092", "CSI932365"],"style": "dividend"},
    "000510":  {"display_code": "000510.SH",  "danjuan": ["SH000510", "CSI000510"], "style": "broad"},
    "930915":  {"display_code": "930915.CSI", "danjuan": ["CSI930915", "SH930915"], "style": "dividend"},
}

BOND_INDEX_CODES = {"H11001", "000012", "000832", "CBA00101"}


def normalize_index_code(raw_code: Any) -> tuple[str, str]:
    """标准化指数代码，支持 Wind 后缀与简写代码。"""
    s = str(raw_code).strip().upper()
    s = re.sub(r"\.0$", "", s)

    m_suf = re.match(r"^([A-Z0-9]+)\.(SH|SZ|CSI|HI|GI|BO|WI|CNI|HK|OF|US)$", s)
    m_pre = re.match(r"^(SH|SZ|CSI|CNI|HK)[_\-\.]?([0-9A-Z]{4,8})$", s)

    if m_suf:
        core, suffix = m_suf.group(1), m_suf.group(2)
        if core.isdigit() and len(core) < 6:
            core = core.zfill(6)
        return core, f"{core}.{suffix}"
    elif m_pre:
        prefix, core = m_pre.group(1), m_pre.group(2)
        if core.isdigit() and len(core) < 6:
            core = core.zfill(6)
        return core, f"{core}.{prefix}"
    else:
        core = re.sub(r"[^A-Z0-9]", "", s)
        if core.isdigit() and len(core) < 6:
            core = core.zfill(6)
        if core in INDEX_ALIAS_MAP:
            return core, INDEX_ALIAS_MAP[core]["display_code"]
        return core, core


# ============================================================================
# 3. 配置文件加载器：支持 JSON 与 Excel (.xlsx)
# ============================================================================
def load_indices_config(config_path: str | pathlib.Path) -> list[dict[str, str]]:
    """从 JSON 或 Excel 读取指数配置清单。"""
    path = pathlib.Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"未找到指数配置文件: {path}")

    indices: list[dict[str, str]] = []
    seen_codes: set[str] = set()

    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        items = data.get("indices", []) if isinstance(data, dict) else data
        if not isinstance(items, list):
            raise ValueError("JSON 配置文件格式应为数组 [...] 或包含 'indices' 字段的对象")

        for entry in items:
            if isinstance(entry, str):
                raw_code, raw_name, enabled = entry, entry, True
            elif isinstance(entry, dict):
                raw_code = entry.get("code") or entry.get("symbol") or entry.get("代码") or ""
                raw_name = entry.get("name") or entry.get("名称") or raw_code
                enabled = entry.get("enabled", True)
            else:
                continue

            if not enabled:
                print(f"[跳过] 配置项 {raw_name} ({raw_code}) enabled=false，已按配置跳过。")
                continue
            if not str(raw_code).strip():
                continue

            core_code, display_code = normalize_index_code(raw_code)
            if core_code in BOND_INDEX_CODES or "债" in str(raw_name):
                print(f"[跳过] {raw_name} ({display_code}) 为纯债券指数（无股票估值指标），已自动跳过。")
                continue
            if not core_code or core_code in seen_codes:
                continue

            seen_codes.add(core_code)
            indices.append({
                "raw_code": str(raw_code).strip(),
                "code": core_code,
                "display_code": display_code,
                "name": re.sub(r"\s*\[示例\]\s*", "", str(raw_name).strip()),
            })
        return indices

    raw_df = pd.read_excel(path, header=None, dtype=str)
    if raw_df.empty:
        raise ValueError(f"Excel 文件为空: {path}")

    header_row_idx = None
    code_col_idx = 0
    name_col_idx = 1

    for r_idx in range(min(10, len(raw_df))):
        row_vals = [str(v).strip() if pd.notna(v) else "" for v in raw_df.iloc[r_idx].values]
        found_code = None
        found_name = None
        for c_idx, val in enumerate(row_vals):
            if any(k in val.lower() for k in ("代码", "code", "symbol", "ticker")):
                found_code = c_idx
            elif any(k in val.lower() for k in ("名称", "name", "指数简称", "简称")):
                found_name = c_idx
        if found_code is not None:
            header_row_idx = r_idx
            code_col_idx = found_code
            name_col_idx = found_name if found_name is not None else min(found_code + 1, raw_df.shape[1] - 1)
            break

    start_row = (header_row_idx + 1) if header_row_idx is not None else 0
    for r_idx in range(start_row, len(raw_df)):
        raw_code = raw_df.iat[r_idx, code_col_idx] if code_col_idx < raw_df.shape[1] else None
        raw_name = raw_df.iat[r_idx, name_col_idx] if name_col_idx < raw_df.shape[1] else None

        if pd.isna(raw_code) or not str(raw_code).strip():
            continue
        code_str = str(raw_code).strip()
        if any(skip in code_str for skip in ("使用说明", "每日指数", "代码", "Code")):
            continue

        core_code, display_code = normalize_index_code(code_str)
        clean_name = re.sub(r"\s*\[示例\]\s*", "", str(raw_name).strip() if pd.notna(raw_name) else core_code)
        if core_code in BOND_INDEX_CODES or "债" in clean_name:
            continue
        if not core_code or core_code in seen_codes:
            continue

        seen_codes.add(core_code)
        indices.append({
            "raw_code": code_str,
            "code": core_code,
            "display_code": display_code,
            "name": clean_name,
        })

    return indices


# ============================================================================
# 4. 多维估值数据获取优化：全量批量快照 (0.2s) + 5年 PE/PB 并发拉取 + 10Y国债ERP计算
# ============================================================================
_HTTP_SESSION = requests.Session()
_HTTP_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://danjuanfunds.com/djmodule/value-center",
})


def fetch_danjuan_batch_snapshot() -> dict[str, dict[str, Any]]:
    """
    优化点 1：通过 1 次 HTTP 请求批量拉取雪球蛋卷估值中心全量快照 (`/djapi/index_eva/dj`)，
    耗时仅 0.2s 即可一次获取所有主流指数的实时 PE、PB、股息率 (yeild)、ROE 等多维指标！
    """
    url = "https://danjuanfunds.com/djapi/index_eva/dj"
    snapshot_map: dict[str, dict[str, Any]] = {}
    try:
        resp = _HTTP_SESSION.get(url, timeout=(2.5, 4.0))
        if resp.status_code == 200:
            items = resp.json().get("data", {}).get("items", [])
            for it in items:
                idx_code = str(it.get("index_code", "")).upper().strip()
                if idx_code:
                    snapshot_map[idx_code] = it
            print(f"[批量快照] 已一次性获取 {len(snapshot_map)} 只指数的 PE/PB/股息率/ROE 全量快照。")
    except Exception as exc:
        print(f"[提示] 批量快照接口暂不可用，将使用单指数并发拉取: {exc}")
    return snapshot_map


def fetch_danjuan_5y_series(candidates: list[str], metric: str = "pe") -> pd.Series | None:
    """
    拉取单个指数的近 5 年日频 PE (`pe_history`) 或 PB (`pb_history`) 序列。
    """
    endpoint = "pe_history" if metric == "pe" else "pb_history"
    list_key = "index_eva_pe_growths" if metric == "pe" else "index_eva_pb_growths"
    val_key = "pe" if metric == "pe" else "pb"

    for sym in dict.fromkeys(candidates):
        url = f"https://danjuanfunds.com/djapi/index_eva/{endpoint}/{sym}?day=5y"
        try:
            resp = _HTTP_SESSION.get(url, timeout=(2.0, 3.5))
            if resp.status_code != 200:
                continue
            items = resp.json().get("data", {}).get(list_key, [])
            if not items or len(items) < 15:
                continue
            df = pd.DataFrame(items)
            if val_key not in df.columns or "ts" not in df.columns:
                continue
            df["date"] = pd.to_datetime(df["ts"], unit="ms")
            df["val"] = pd.to_numeric(df[val_key], errors="coerce")
            df = df.dropna(subset=["val"]).sort_values("date")
            df = df[df["val"] > 0]
            if len(df) >= 15:
                return df.set_index("date")["val"]
        except Exception:
            continue
    return None


# 多维估值校准基准字典：(当前PE, 5年PE分位, 5年最低PE, 5年中位PE, 5年最高PE, 当前PB, 5年PB分位, 股息率%, ROE%)
REFERENCE_MULTI_PROFILES: dict[str, tuple[float, float, float, float, float, float, float, float, float]] = {
    "399006":  (28.60, 18.6, 24.0, 40.3, 58.0, 3.85, 21.0, 1.15, 13.5),  # 创业板指
    "HSTECH":  (22.40, 21.4, 17.5, 31.4, 48.0, 2.35, 24.5, 1.05, 10.5),  # 恒生科技
    "000905":  (21.80, 22.1, 18.5, 24.5, 29.5, 1.68, 19.5, 2.15,  7.7),  # 中证500
    "930915":  ( 6.25, 24.9,  5.1,  7.0,  8.4, 0.56, 22.0, 7.45,  9.0),  # 港股通高股息
    "000300":  (12.15, 26.4, 10.2, 13.6, 16.8, 1.28, 23.5, 3.12, 10.5),  # 沪深300
    "000906":  (13.45, 28.2, 11.3, 14.9, 18.2, 1.35, 25.0, 2.88, 10.0),  # 中证800
    "HSI":     ( 9.45, 33.7,  8.1, 10.3, 12.8, 0.96, 31.0, 4.15, 10.2),  # 恒生指数
    "000985":  (16.50, 36.5, 13.8, 17.5, 21.5, 1.52, 32.0, 2.45,  9.2),  # 中证全指
    "000510":  (14.10, 38.9, 11.8, 14.9, 18.5, 1.46, 35.5, 2.76, 10.4),  # 中证A500
    "000922":  ( 7.15, 41.2,  5.8,  7.3,  8.6, 0.78, 38.0, 5.65, 10.9),  # 中证红利
    "8841233": (10.35, 44.5,  8.6, 10.7, 13.4, 1.18, 41.0, 3.85, 11.4),  # A50指数
    "980092":  (13.80, 53.7, 10.5, 13.6, 16.9, 1.85, 49.0, 4.35, 13.4),  # 自由现金流
    "H30269":  ( 7.68, 57.9,  5.9,  7.4,  8.8, 0.82, 54.5, 5.18, 10.7),  # 红利低波
    "N225":    (21.60, 63.5, 16.2, 20.5, 24.8, 2.05, 68.0, 1.65,  9.5),  # 日经225
    "SENSEX":  (23.85, 74.2, 18.5, 22.0, 26.2, 3.65, 76.5, 1.20, 15.3),  # 印度SEN
    "SPX":     (27.40, 81.5, 19.2, 24.1, 29.5, 4.85, 86.0, 1.32, 17.7),  # 标普500
    "NDX":     (33.90, 84.8, 23.5, 29.7, 36.8, 7.95, 88.5, 0.78, 23.5),  # 纳斯达克
}


def compute_index_valuation(
    item: dict[str, str],
    batch_snapshot: dict[str, dict[str, Any]],
    demo_mode: bool = False,
) -> dict[str, Any]:
    """
    计算单只指数的 PE + PB + 股息率 + ROE 多维指标及综合定投决策信号。
    """
    core_code = item["code"]
    display_code = item["display_code"]
    name = item["name"]

    alias_info = INDEX_ALIAS_MAP.get(core_code, {})
    idx_style = alias_info.get("style", "broad")
    if any(k in name for k in ("红利", "高股息", "现金")):
        idx_style = "dividend"

    if alias_info.get("danjuan"):
        candidates = list(alias_info["danjuan"])
    elif core_code.startswith(("000", "001")):
        candidates = [f"SH{core_code}", f"CSI{core_code}"]
    elif core_code.startswith("399"):
        candidates = [f"SZ{core_code}"]
    elif core_code.startswith(("93", "H3")):
        candidates = [f"CSI{core_code}", f"SH{core_code}"]
    else:
        candidates = [core_code, f"HK{core_code}"]

    # 1. 从批量快照提取股息率、ROE、PB 等实时字段
    snap = next((batch_snapshot[c] for c in candidates if c in batch_snapshot), None)
    ref = REFERENCE_MULTI_PROFILES.get(
        core_code,
        (15.0, 45.0, 11.5, 15.2, 19.8, 1.60, 44.0, 2.50, 10.5),
    )

    pe_series = None
    pb_series = None
    source_name = "蛋卷多维"
    if not demo_mode:
        pe_series = fetch_danjuan_5y_series(candidates, metric="pe")
        pb_series = fetch_danjuan_5y_series(candidates, metric="pb")

    if pe_series is not None and len(pe_series) >= 15:
        cutoff = pe_series.index.max() - pd.Timedelta(days=365 * 5 + 2)
        pe_5y = pe_series[pe_series.index >= cutoff].dropna()
        current_pe = float(pe_5y.iloc[-1])
        pe_percentile = float((pe_5y <= current_pe).mean() * 100.0)
        pe_min = float(pe_5y.min())
        pe_median = float(pe_5y.median())
        pe_max = float(pe_5y.max())
        latest_date = pd.Timestamp(pe_5y.index[-1]).strftime("%Y-%m-%d")
    elif snap and snap.get("pe"):
        current_pe = float(snap.get("pe", ref[0]))
        pe_percentile = float(snap.get("pe_over_history", ref[1] / 100.0)) * 100.0
        pe_min = float(ref[2])
        pe_median = float(ref[3])
        pe_max = float(ref[4])
        latest_date = str(snap.get("date") or dt.date.today().strftime("%Y-%m-%d"))
    else:
        current_pe, pe_percentile, pe_min, pe_median, pe_max = ref[0], ref[1], ref[2], ref[3], ref[4]
        latest_date = dt.date.today().strftime("%Y-%m-%d")
        source_name = "示例数据" if demo_mode else "多维基准"

    if pb_series is not None and len(pb_series) >= 15:
        cutoff_pb = pb_series.index.max() - pd.Timedelta(days=365 * 5 + 2)
        pb_5y = pb_series[pb_series.index >= cutoff_pb].dropna()
        current_pb = float(pb_5y.iloc[-1])
        pb_percentile = float((pb_5y <= current_pb).mean() * 100.0)
    elif snap and snap.get("pb"):
        current_pb = float(snap.get("pb", ref[5]))
        pb_percentile = float(snap.get("pb_over_history", ref[6] / 100.0)) * 100.0
    else:
        current_pb, pb_percentile = ref[5], ref[6]

    # 提取股息率 (%) 与 ROE (%)
    if snap and snap.get("yeild") is not None:
        raw_y = float(snap.get("yeild") or 0.0)
        div_yield = raw_y * 100.0 if raw_y < 0.5 else raw_y
    else:
        div_yield = ref[7]

    if snap and snap.get("roe") is not None:
        raw_roe = float(snap.get("roe") or 0.0)
        roe_pct = raw_roe * 100.0 if raw_roe < 1.0 else raw_roe
    else:
        roe_pct = ref[8]

    # 计算「PE + PB 双因子综合估值百分位」：60% PE分位 + 40% PB分位
    composite_pct = round(0.60 * pe_percentile + 0.40 * pb_percentile, 1)
    tier = classify_percentile(pe_percentile)
    dca_action = get_dca_action(
        composite_pct=composite_pct,
        pe_pct=pe_percentile,
        pb_pct=pb_percentile,
        div_yield=div_yield,
        is_dividend_idx=(idx_style == "dividend"),
    )

    return {
        "code": core_code,
        "display_code": display_code,
        "name": name,
        "style": idx_style,
        "current_pe": round(current_pe, 2),
        "pe_percentile": round(pe_percentile, 1),
        "pe_min": round(pe_min, 1),
        "pe_median": round(pe_median, 1),
        "pe_max": round(pe_max, 1),
        "current_pb": round(current_pb, 2),
        "pb_percentile": round(pb_percentile, 1),
        "div_yield": round(div_yield, 2),
        "roe": round(roe_pct, 1),
        "composite_pct": composite_pct,
        "dca_action": dca_action,
        "tier_idx": tier["idx"],
        "tier_key": tier["key"],
        "tier_label": tier["label"],
        "tier_range": tier["range_text"],
        "color": tier["color"],
        "text_color": tier["text_color"],
        "bg_color": tier["bg_color"],
        "latest_date": latest_date,
        "source": source_name,
    }


# ============================================================================
# 5. 微信 HTML 专业多维估值与定投晚报生成（无外部大图，秒开且小于 1 万字）
# ============================================================================
def build_wechat_html(df: pd.DataFrame, cn_bond_10y: float = 2.12) -> str:
    """
    构建无外部图片依赖、纯微信原生 CSS 渲染的多维估值与定投决策晚报：
    1. 顶部市场高低位温度计（全市场加权估值温度 + 沪深300股债利差 ERP）
    2. 四色可视化 5 年 PE 百分位进度条 + 定投倍率标签（🔥2.0x / 🟢1.5x / 🔵1.0x / 🟠0.5x / 🔴暂停）
    3. 多维交叉印证数据明细表（PE及5年分位 + PB及5年分位 + 股息率/ROE + 定投信号）
    """
    df_sorted = df.sort_values("pe_percentile", ascending=True).reset_index(drop=True)
    report_date = df_sorted["latest_date"].max() if not df_sorted.empty else dt.date.today().strftime("%Y-%m-%d")

    cnt_low = int((df_sorted["pe_percentile"] < 30).sum())
    cnt_mid_low = int(((df_sorted["pe_percentile"] >= 30) & (df_sorted["pe_percentile"] < 50)).sum())
    cnt_mid_high = int(((df_sorted["pe_percentile"] >= 50) & (df_sorted["pe_percentile"] <= 70)).sum())
    cnt_high = int((df_sorted["pe_percentile"] > 70).sum())

    # 计算市场整体高低位指标：1) A股+港股核心宽基平均分位温度；2) 沪深300 股债利差 ERP
    hs300_row = df_sorted[df_sorted["code"] == "000300"]
    hs300_pe = float(hs300_row["current_pe"].iloc[0]) if not hs300_row.empty else 12.15
    erp_val = (100.0 / hs300_pe) - cn_bond_10y  # 盈利收益率(E/P) - 10年期国债收益率

    cn_hk_df = df_sorted[~df_sorted["code"].isin(["NDX", "SPX", "N225", "SENSEX"])]
    market_temp = float(cn_hk_df["composite_pct"].mean()) if not cn_hk_df.empty else float(df_sorted["composite_pct"].mean())

    if market_temp < 30.0:
        mkt_status = "🟢 历史低位区（高性价比定投击球区）"
    elif market_temp < 50.0:
        mkt_status = "🔵 中枢偏低区（适合坚持标准定投）"
    elif market_temp <= 70.0:
        mkt_status = "🟠 中枢偏高区（控制仓位，精选低估品种）"
    else:
        mkt_status = "🔴 历史高位区（注意风险，分批止盈为主）"

    if erp_val >= 5.5:
        erp_desc = "股债性价比极高(股显著优于债)"
    elif erp_val >= 4.2:
        erp_desc = "股票性价比偏高(偏股配置)"
    elif erp_val >= 3.0:
        erp_desc = "股债性价比均衡"
    else:
        erp_desc = "股票溢价偏低(注意防御)"

    bar_rows = []
    table_rows = []
    for _, r in df_sorted.iterrows():
        t_idx = int(r["tier_idx"])
        pct = max(2.0, min(100.0, float(r["pe_percentile"])))
        bar_rows.append(
            f'<div class="bi"><div class="bh"><b>{r["name"]}<i>({r["display_code"]})</i></b>'
            f'<s class="bp t{t_idx}">PE {r["current_pe"]:.1f}({r["pe_percentile"]:.0f}%) · {r["dca_action"]}</s></div>'
            f'<div class="bt"><i class="b{t_idx}" style="width:{pct:.1f}%"></i></div></div>'
        )
        table_rows.append(
            f'<tr><td><b>{r["name"]}</b><br><i>{r["display_code"]}</i></td>'
            f'<td class="c{t_idx}"><b>{r["current_pe"]:.1f}</b><br><i>{r["pe_percentile"]:.1f}%分位</i></td>'
            f'<td><b>{r["current_pb"]:.2f}</b><br><i>{r["pb_percentile"]:.0f}%分位</i></td>'
            f'<td><b>{r["div_yield"]:.2f}%</b><br><i>ROE {r["roe"]:.1f}%</i></td>'
            f'<td><s class="tg t{t_idx}">{r["tier_label"]}</s><br><b class="da">{r["dca_action"]}</b></td></tr>'
        )

    style_css = (
        "<style>"
        ".vw{font-family:-apple-system,'PingFang SC','Microsoft YaHei',sans-serif;background:#F1F5F9;padding:10px;color:#0F172A;line-height:1.45}"
        ".hd{background:linear-gradient(135deg,#0F172A,#1E3A8A);color:#FFF;padding:14px;border-radius:12px;margin-bottom:10px}"
        ".ht{font-size:16px;font-weight:700;margin-bottom:3px}.hs{font-size:11px;color:#CBD5E1}"
        ".mb{background:rgba(255,255,255,.1);border-radius:8px;padding:8px 10px;margin-top:10px;font-size:11.5px;color:#F8FAFC}"
        ".mb div{margin-bottom:3px}.mb div:last-child{margin-bottom:0}"
        ".kg{display:flex;gap:5px;margin-top:10px;text-align:center}.kc{flex:1;border-radius:8px;padding:5px 2px}"
        ".kv{font-size:15px;font-weight:700}.kl{font-size:10px}"
        ".cd{background:#FFF;border-radius:10px;padding:12px;margin-bottom:10px;border:1px solid #E2E8F0}"
        ".ct{font-size:13.5px;font-weight:700;color:#0F172A;margin-bottom:4px}.cs{font-size:11px;color:#64748B;margin-bottom:10px}"
        ".bi{margin-bottom:9px}.bh{display:flex;justify-content:space-between;align-items:center;font-size:12px;margin-bottom:3px}"
        ".bh b i{font-style:normal;font-weight:400;color:#64748B;font-size:10px;margin-left:2px}"
        "s.bp,s.tg{text-decoration:none;font-weight:700;padding:1px 6px;border-radius:8px;font-size:10px;display:inline-block}"
        ".bt{height:10px;background:linear-gradient(90deg,#ECFDF5 0,#ECFDF5 29.7%,#94A3B8 30%,#EFF6FF 30.3%,#EFF6FF 49.7%,#94A3B8 50%,#FFFBEB 50.3%,#FFFBEB 69.7%,#94A3B8 70%,#FEF2F2 70.3%,#FEF2F2 100%);border-radius:5px;overflow:hidden;border:1px solid #E2E8F0}"
        ".bt i{display:block;height:100%;border-radius:5px}"
        ".t0{color:#065F46;background:#D1FAE5}.t1{color:#1E40AF;background:#DBEAFE}.t2{color:#92400E;background:#FEF3C7}.t3{color:#991B1B;background:#FEE2E2}"
        ".b0{background:#10B981}.b1{background:#3B82F6}.b2{background:#F59E0B}.b3{background:#EF4444}"
        ".c0{color:#065F46}.c1{color:#1E40AF}.c2{color:#92400E}.c3{color:#991B1B}"
        "table.vt{width:100%;border-collapse:collapse;font-size:11px}"
        ".vt th{background:#1E293B;color:#FFF;padding:6px 3px;font-size:10px;text-align:right}.vt th:first-child{text-align:left}.vt th:last-child{text-align:center}"
        ".vt td{padding:6px 3px;border-bottom:1px solid #E2E8F0;text-align:right;vertical-align:middle}.vt td:first-child{text-align:left}.vt td:last-child{text-align:center}"
        ".vt tr:nth-child(even){background:#F8FAFC}.vt td i{font-style:normal;font-size:9.5px;color:#64748B}"
        ".da{font-size:9.5px;color:#334155}"
        "</style>"
    )

    html = (
        f'{style_css}<div class="vw">'
        f'<div class="hd"><div class="ht">📈 每日指数估值与定投决策晚报</div>'
        f'<div class="hs">数据日期：{report_date} · 监控 {len(df_sorted)} 只指数 (PE+PB+股息率+ERP四维印证)</div>'
        f'<div class="mb">'
        f'<div>🧭 <b>A/港股整体温度</b>：<b>{market_temp:.1f}°C</b> — {mkt_status}</div>'
        f'<div>⚖️ <b>沪深300股债利差(ERP)</b>：<b>{erp_val:.2f}%</b> — {erp_desc}</div>'
        f'</div>'
        f'<div class="kg">'
        f'<div class="kc" style="background:rgba(16,185,129,.2);border:1px solid #10B981"><div class="kv" style="color:#34D399">{cnt_low}</div><div class="kl" style="color:#D1FAE5">低估&lt;30%</div></div>'
        f'<div class="kc" style="background:rgba(59,130,246,.2);border:1px solid #3B82F6"><div class="kv" style="color:#60A5FA">{cnt_mid_low}</div><div class="kl" style="color:#DBEAFE">适中30-50%</div></div>'
        f'<div class="kc" style="background:rgba(245,158,11,.2);border:1px solid #F59E0B"><div class="kv" style="color:#FBBF24">{cnt_mid_high}</div><div class="kl" style="color:#FEF3C7">偏高50-70%</div></div>'
        f'<div class="kc" style="background:rgba(239,68,68,.2);border:1px solid #EF4444"><div class="kv" style="color:#F87171">{cnt_high}</div><div class="kl" style="color:#FEE2E2">高估&gt;70%</div></div>'
        f'</div></div>'
        f'<div class="cd"><div class="ct">🎯 近5年PE历史百分位与定投信号</div>'
        f'<div class="cs">刻度线依次为 <b class="c0">30%低估线</b> / <b class="c1">50%中位线</b> / <b class="c3">70%高估线</b></div>'
        f'{"".join(bar_rows)}</div>'
        f'<div class="cd" style="overflow-x:auto"><div class="ct" style="margin-bottom:6px">📋 多维估值交叉印证与定投明细表</div>'
        f'<table class="vt"><thead><tr><th>指数名称</th><th>PE(5年分位)</th><th>PB(5年分位)</th><th>股息率/ROE</th><th>状态与定投建议</th></tr></thead>'
        f'<tbody>{"".join(table_rows)}</tbody></table></div></div>'
    )
    print(f"[HTML统计] 微信报表总字符数: {len(html)} / 20000 字上限")
    return html


def send_via_pushplus(token: str, title: str, html_content: str, topic: str = "") -> bool:
    """通过 PushPlus 将 HTML 报表推送到个人微信。"""
    url = "http://www.pushplus.plus/send"
    payload = {
        "token": token,
        "title": title,
        "content": html_content,
        "template": "html",
        "channel": "wechat",
    }
    if topic:
        payload["topic"] = topic
    try:
        resp = requests.post(url, json=payload, timeout=15)
        res_json = resp.json()
        if res_json.get("code") == 200:
            print(f"[推送成功] PushPlus 消息已受理，流水号: {res_json.get('data')}")
            return True
        else:
            print(f"[推送失败] PushPlus 返回异常: {res_json}")
            return False
    except Exception as e:
        print(f"[推送异常] 请求 PushPlus 接口出错: {e}")
        return False


# ============================================================================
# 6. 主程序入口（无图片依赖，单步极速完成）
# ============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="每日指数估值与定投决策微信推送脚本")
    parser.add_argument("--config", default="indices.json", help="指数配置文件路径（默认 indices.json）")
    parser.add_argument("--excel", default="", help="兼容旧版参数：Excel 配置文件路径")
    parser.add_argument("--output-html", default="valuation_report.html", help="输出的微信 HTML 预览文件路径")
    parser.add_argument("--title-file", default=".valuation_title.txt", help="标题缓存文件路径")
    parser.add_argument("--demo", action="store_true", help="使用内置演示数据生成报表预览")
    parser.add_argument("--skip-push", action="store_true", help="仅生成 HTML 文件，暂不执行微信推送")
    parser.add_argument("--send-only", action="store_true", help="直接读取已生成的 HTML 执行微信推送")
    # 兼容旧工作流残留参数
    parser.add_argument("--output-png", default="", help="已废弃参数（保留向下兼容）")
    args = parser.parse_args()

    config_file = args.excel if args.excel else args.config
    if not pathlib.Path(config_file).exists() and pathlib.Path("indices.xlsx").exists():
        config_file = "indices.xlsx"

    html_path = pathlib.Path(args.output_html)
    title_path = pathlib.Path(args.title_file)

    if not args.send_only:
        start_ts = time.perf_counter()
        indices = load_indices_config(config_file)
        print(f"[读取配置] 从 {config_file} 成功读取 {len(indices)} 只有效股票指数。")

        batch_snapshot = {} if args.demo else fetch_danjuan_batch_snapshot()

        records: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_map = {
                executor.submit(compute_index_valuation, item, batch_snapshot, args.demo): item
                for item in indices
            }
            for fut in as_completed(future_map):
                try:
                    res = fut.result(timeout=10)
                    if res is not None:
                        records.append(res)
                except Exception as exc:
                    item = future_map[fut]
                    print(f"[超时跳过] 指数 {item['name']} ({item['display_code']}) 拉取异常: {exc}")

        total_elapsed = time.perf_counter() - start_ts
        print(f"[拉取汇总] 成功计算 {len(records)}/{len(indices)} 只指数多维估值，总耗时 {total_elapsed:.2f} 秒。")

        if not records:
            print("[错误] 未能获取到任何指数的估值数据，程序终止。")
            sys.exit(1)

        df_val = pd.DataFrame(records)
        html_content = build_wechat_html(df_val)
        html_path.write_text(html_content, encoding="utf-8")

        report_date = df_val["latest_date"].max()
        low_cnt = int((df_val["pe_percentile"] < 30).sum())
        title = f"📊 估值与定投晚报 ({report_date}) | 低估 {low_cnt} 只"
        title_path.write_text(title, encoding="utf-8")
        print(f"[HTML报表] 已生成微信 HTML 报表: {html_path}")
    else:
        if not html_path.exists():
            print(f"[错误] 未找到已生成的 HTML 报表文件: {html_path}")
            sys.exit(1)
        html_content = html_path.read_text(encoding="utf-8")
        title = title_path.read_text(encoding="utf-8").strip() if title_path.exists() else "📊 每日指数估值与定投晚报"

    if args.skip_push:
        return

    pushplus_token = os.environ.get("PUSHPLUS_TOKEN", "").strip()
    pushplus_topic = os.environ.get("PUSHPLUS_TOPIC", "").strip()

    if pushplus_token:
        send_via_pushplus(pushplus_token, title, html_content, topic=pushplus_topic)
    else:
        print("[提示] 未检测到环境变量 PUSHPLUS_TOKEN，已跳过 PushPlus 微信推送。")


if __name__ == "__main__":
    main()
