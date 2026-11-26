#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日指数估值与定投决策微信推送系统 (真实数据防伪 + PE-TTM 新鲜度校验 + 中证官方直连版)
核心修复与升级：
1. 彻底移除实盘模式下的静态兜底假数据：接口未收录的指数单独列在底部提示，绝不冒充实时行情
2. 新增数据新鲜度熔断（>10天自动隔离）：拦截停更的海外接口（如旧年份数据），严防误发 2.0x 定投信号
3. 新增中证指数官网原生 JSON 接口直连：自动补齐中证全指(000985)、中证800(000906)、中证A500(000510)、中证A50(930050)近5年日频 PE-TTM
4. 修复 SENSEX 触发 PushPlus 敏感词过滤被显示为 SEN*** 的问题（展示为 BSE30.BO）
5. 表头明确标注 PE-TTM(5年分位) 与 PB(MRQ)，每行展示数据更新日期；红利类高股息指数(>=4.5%)防机械止盈卖飞
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
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
# 0. 全局网络超时保护与新鲜度阈值
# ============================================================================
socket.setdefaulttimeout(5.0)
_ORIG_SESSION_REQUEST = requests.Session.request

# 超过 10 个自然日未更新（兼容国庆/春节7天长假+周末）即判定为接口数据停更
MAX_STALE_DAYS = 10


def _safe_timeout_request(self: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
    if kwargs.get("timeout") is None:
        kwargs["timeout"] = (2.5, 4.5)
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
        "color": "#10B981",
        "text_color": "#065F46",
        "bg_color": "#D1FAE5",
    },
    {
        "idx": 1,
        "key": "normal_low",
        "label": "适中偏低",
        "range_text": "30%–50%",
        "color": "#3B82F6",
        "text_color": "#1E40AF",
        "bg_color": "#DBEAFE",
    },
    {
        "idx": 2,
        "key": "normal_high",
        "label": "适中偏高",
        "range_text": "50%–70%",
        "color": "#F59E0B",
        "text_color": "#92400E",
        "bg_color": "#FEF3C7",
    },
    {
        "idx": 3,
        "key": "overvalued",
        "label": "高估",
        "range_text": ">70%",
        "color": "#EF4444",
        "text_color": "#991B1B",
        "bg_color": "#FEE2E2",
    },
]


def classify_percentile(pct: float) -> dict[str, Any]:
    """根据 PE-TTM 5年百分位返回对应的四档颜色与标签。"""
    if pct < 30.0:
        return VALUATION_TIERS[0]
    elif pct < 50.0:
        return VALUATION_TIERS[1]
    elif pct <= 70.0:
        return VALUATION_TIERS[2]
    else:
        return VALUATION_TIERS[3]


def get_dca_action(
    composite_pct: float,
    pe_pct: float,
    pb_pct: float | None,
    div_yield: float | None,
    is_dividend_idx: bool,
) -> str:
    """结合 PE-TTM百分位 + PB百分位 + 股息率 给出专业定投倍率建议。"""
    pb_check = pb_pct if pb_pct is not None else pe_pct
    dy = div_yield if div_yield is not None else 0.0

    if composite_pct < 18.0 or (pe_pct < 25.0 and pb_check < 25.0):
        return "🔥2.0x定投"
    elif composite_pct < 30.0:
        return "🟢1.5x定投"
    elif composite_pct < 50.0:
        if is_dividend_idx and dy >= 4.8:
            return "🟢1.2x红利"
        return "🔵1.0x定投"
    elif composite_pct <= 70.0:
        return "🟠0.5x减半"
    elif composite_pct <= 85.0:
        if is_dividend_idx and dy >= 4.5:
            return "🟠高息持有"
        return "🔴暂停定投"
    else:
        # 红利策略指数若股息率仍 >= 4.5%，避免因5年低基数PE分位顶格而机械卖飞
        if is_dividend_idx and dy >= 4.5:
            return "🟠高息观望"
        return "🚨分批止盈"


# ============================================================================
# 2. 海内外指数与 Wind 代码精准映射表（修正 A50 映射，新增 csindex 官方代码）
# ============================================================================
INDEX_ALIAS_MAP: dict[str, dict[str, Any]] = {
    "8841233": {"display_code": "8841233.WI", "danjuan": ["CSI930050", "XIN9"],     "csindex": "930050", "style": "broad"},
    "A50":     {"display_code": "A50",        "danjuan": ["CSI930050", "XIN9"],     "csindex": "930050", "style": "broad"},
    "000300":  {"display_code": "000300.SH",  "danjuan": ["SH000300"],              "csindex": "000300", "style": "broad"},
    "000985":  {"display_code": "000985.CSI", "danjuan": ["CSI000985", "SH000985"], "csindex": "000985", "style": "broad"},
    "000905":  {"display_code": "000905.SH",  "danjuan": ["SH000905"],              "csindex": "000905", "style": "broad"},
    "000906":  {"display_code": "000906.SH",  "danjuan": ["SH000906", "CSI000906"], "csindex": "000906", "style": "broad"},
    "HSTECH":  {"display_code": "HSTECH.HI",  "danjuan": ["HKHSTECH", "HSTECH"],    "csindex": "",       "style": "overseas"},
    "000922":  {"display_code": "000922.CSI", "danjuan": ["SH000922", "CSI000922"], "csindex": "000922", "style": "dividend"},
    "NDX":     {"display_code": "NDX.GI",     "danjuan": ["NDX"],                   "csindex": "",       "style": "overseas"},
    "IXIC":    {"display_code": "IXIC.GI",    "danjuan": ["NDX", "IXIC"],           "csindex": "",       "style": "overseas"},
    "DJI":     {"display_code": "DJI.GI",     "danjuan": ["DJI"],                   "csindex": "",       "style": "overseas"},
    "SPX":     {"display_code": "SPX.GI",     "danjuan": ["SP500", "SPX"],          "csindex": "",       "style": "overseas"},
    "SP500":   {"display_code": "SPX.GI",     "danjuan": ["SP500", "SPX"],          "csindex": "",       "style": "overseas"},
    "N225":    {"display_code": "N225.GI",    "danjuan": ["N225"],                  "csindex": "",       "style": "overseas"},
    "TOPX":    {"display_code": "TOPX.GI",    "danjuan": ["TOPX", "TOPIX"],         "csindex": "",       "style": "overseas"},
    "TOPIX":   {"display_code": "TOPX.GI",    "danjuan": ["TOPX", "TOPIX"],         "csindex": "",       "style": "overseas"},
    "FCHI":    {"display_code": "FCHI.GI",    "danjuan": ["FCHI", "CAC40"],         "csindex": "",       "style": "overseas"},
    "CAC40":   {"display_code": "FCHI.GI",    "danjuan": ["FCHI", "CAC40"],         "csindex": "",       "style": "overseas"},
    "GDAXI":   {"display_code": "GDAXI.GI",   "danjuan": ["GDAXI", "DAX"],          "csindex": "",       "style": "overseas"},
    "DAX":     {"display_code": "GDAXI.GI",   "danjuan": ["GDAXI", "DAX"],          "csindex": "",       "style": "overseas"},
    "SENSEX":  {"display_code": "BSE30.BO",   "danjuan": ["935600", "SENSEX"],      "csindex": "",       "style": "overseas"},
    "HSI":     {"display_code": "HSI.HI",     "danjuan": ["HKHSI", "HSI"],          "csindex": "",       "style": "overseas"},
    "399006":  {"display_code": "399006.SZ",  "danjuan": ["SZ399006"],              "csindex": "",       "style": "broad"},
    "H30269":  {"display_code": "H30269.CSI", "danjuan": ["CSIH30269"],             "csindex": "H30269", "style": "dividend"},
    "980092":  {"display_code": "980092.CNI", "danjuan": ["CNI980092", "CSI932365"],"csindex": "932365", "style": "dividend"},
    "000510":  {"display_code": "000510.SH",  "danjuan": ["CSI000510", "SH000510"], "csindex": "000510", "style": "broad"},
    "930915":  {"display_code": "930915.CSI", "danjuan": ["CSI930915", "SH930915"], "csindex": "930915", "style": "dividend"},
}

BOND_INDEX_CODES = {"H11001", "000012", "000832", "CBA00101"}
OVERSEAS_CODES = {"NDX", "IXIC", "DJI", "SPX", "SP500", "N225", "TOPX", "TOPIX", "FCHI", "CAC40", "GDAXI", "DAX", "SENSEX"}


def sanitize_display_code(code_text: str) -> str:
    """避免英文代码触发 PushPlus 微信网关敏感词过滤（如 SENSEX -> BSE30）。"""
    return re.sub(r"SENSEX", "BSE30", code_text, flags=re.IGNORECASE)


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
        display = INDEX_ALIAS_MAP.get(core, {}).get("display_code", f"{core}.{suffix}")
        return core, sanitize_display_code(display)
    elif m_pre:
        prefix, core = m_pre.group(1), m_pre.group(2)
        if core.isdigit() and len(core) < 6:
            core = core.zfill(6)
        display = INDEX_ALIAS_MAP.get(core, {}).get("display_code", f"{core}.{prefix}")
        return core, sanitize_display_code(display)
    else:
        core = re.sub(r"[^A-Z0-9]", "", s)
        if core.isdigit() and len(core) < 6:
            core = core.zfill(6)
        display = INDEX_ALIAS_MAP.get(core, {}).get("display_code", core)
        return core, sanitize_display_code(display)


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
# 4. 实时多源 PE-TTM 抓取：雪球蛋卷 (PE-TTM + PB-MRQ + 股息率/ROE) + 中证指数官网 JSON (PE-TTM)
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
    """1次请求拉取雪球蛋卷估值中心全量快照 (`/djapi/index_eva/dj`)。"""
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
            print(f"[批量快照] 已获取 {len(snapshot_map)} 只指数的 PE-TTM/PB/股息率/ROE 实时快照。")
    except Exception as exc:
        print(f"[提示] 蛋卷批量快照接口请求失败: {exc}")
    return snapshot_map


def fetch_danjuan_5y_series(candidates: list[str], metric: str = "pe") -> pd.Series | None:
    """拉取单个指数近 5 年日频 PE-TTM (`pe_history`) 或 PB-MRQ (`pb_history`) 序列。"""
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


def fetch_csindex_5y_pe_ttm(cs_code: str) -> pd.Series | None:
    """
    中证指数官网原生 JSON 接口（免额外依赖）：
    拉取中证全指(000985)、中证800(000906)、中证A500(000510)、中证A50(930050)等官方近5年日频滚动市盈率(PE-TTM)。
    """
    if not cs_code:
        return None
    end_dt = dt.date.today()
    start_dt = end_dt - dt.timedelta(days=365 * 5 + 15)
    url = "https://www.csindex.com.cn/csindex-home/perf/index-perf"
    params = {
        "indexCode": cs_code,
        "startDate": start_dt.strftime("%Y%m%d"),
        "endDate": end_dt.strftime("%Y%m%d"),
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": f"https://www.csindex.com.cn/zh-CN/indices/index-detail/{cs_code}",
    }
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=(3.0, 5.0))
        if resp.status_code != 200:
            return None
        rows = resp.json().get("data")
        if not rows or len(rows) < 15:
            return None
        df = pd.DataFrame(rows)
        # 中证接口返回列顺序固定：首列为 tradeDate(日期)，末列为 peg(滚动市盈率 PE-TTM)
        date_col = "tradeDate" if "tradeDate" in df.columns else df.columns[0]
        pe_col = "peg" if "peg" in df.columns else df.columns[-1]
        df["date"] = pd.to_datetime(df[date_col], errors="coerce")
        df["pe"] = pd.to_numeric(df[pe_col], errors="coerce")
        df = df.dropna(subset=["date", "pe"]).sort_values("date")
        df = df[df["pe"] > 0]
        if len(df) >= 15:
            return df.set_index("date")["pe"]
    except Exception:
        return None
    return None


def _build_demo_record(item: dict[str, str]) -> dict[str, Any]:
    """仅用于本地 --demo 离线自测，实盘运行绝不调用。"""
    today_str = dt.date.today().strftime("%Y-%m-%d")
    core_code = item["code"]
    demo_map = {
        "HSTECH": (16.6, 12.0, 1.95, 18.0, 1.1, 10.5, today_str, "蛋卷"),
        "399006": (24.7, 15.0, 3.40, 22.0, 1.2, 13.5, today_str, "蛋卷"),
        "000300": (13.4, 52.0, 1.32, 34.0, 3.0, 10.5, today_str, "蛋卷"),
        "000985": (16.8, 42.0, None, None, None, None, today_str, "中证"),
        "000922": (9.5,  96.0, 0.88, 65.0, 5.2, 10.9, today_str, "蛋卷"),
        "NDX":    (24.6,  6.0, 6.20, 45.0, 0.8, 22.0, "2023-03-15", "蛋卷"),  # 模拟停更检测
    }
    if core_code not in demo_map:
        return {
            "status": "unavailable",
            "code": core_code,
            "display_code": item["display_code"],
            "name": item["name"],
            "reason": "免费公开接口暂未收录",
        }
    pe, pe_pct, pb, pb_pct, dy, roe, ldate, src = demo_map[core_code]
    stale_days = (dt.date.today() - dt.date.fromisoformat(ldate)).days
    if stale_days > MAX_STALE_DAYS:
        return {
            "status": "stale",
            "code": core_code,
            "display_code": item["display_code"],
            "name": item["name"],
            "current_pe": pe,
            "latest_date": ldate,
            "stale_days": stale_days,
            "source": src,
        }
    is_div = INDEX_ALIAS_MAP.get(core_code, {}).get("style") == "dividend"
    comp = round(0.60 * pe_pct + 0.40 * (pb_pct if pb_pct is not None else pe_pct), 1)
    tier = classify_percentile(pe_pct)
    return {
        "status": "ok",
        "code": core_code,
        "display_code": item["display_code"],
        "name": item["name"],
        "current_pe": pe,
        "pe_percentile": pe_pct,
        "current_pb": pb,
        "pb_percentile": pb_pct,
        "div_yield": dy,
        "roe": roe,
        "composite_pct": comp,
        "dca_action": get_dca_action(comp, pe_pct, pb_pct, dy, is_div),
        "tier_idx": tier["idx"],
        "tier_label": tier["label"],
        "latest_date": ldate,
        "source": src,
    }


def compute_index_valuation(
    item: dict[str, str],
    batch_snapshot: dict[str, dict[str, Any]],
    demo_mode: bool = False,
) -> dict[str, Any]:
    """
    计算单只指数的真实 PE-TTM + PB-MRQ + 股息率/ROE。
    返回字典始终包含 status 字段：
      - 'ok': 实时有效数据（新鲜度 <= 10 天）
      - 'stale': 接口已停更/数据滞后（> 10 天），自动隔离并屏蔽定投信号
      - 'unavailable': 接口暂未收录，绝不使用写死的参考值冒充
    """
    if demo_mode:
        return _build_demo_record(item)

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

    snap = next((batch_snapshot[c] for c in candidates if c in batch_snapshot), None)

    # 1. 优先从蛋卷拉取 5 年日频 PE-TTM 与 PB-MRQ 序列
    pe_series = fetch_danjuan_5y_series(candidates, metric="pe")
    pb_series = fetch_danjuan_5y_series(candidates, metric="pb")
    source_name = "蛋卷"

    # 2. 若蛋卷未收录且属于中证/A股指数，自动切换中证指数官网原生 JSON 接口补齐 5 年 PE-TTM
    if pe_series is None:
        cs_code = alias_info.get("csindex", "")
        if not cs_code and (core_code.startswith(("000", "93", "H3")) and len(core_code) == 6):
            cs_code = core_code
        if cs_code:
            pe_series = fetch_csindex_5y_pe_ttm(cs_code)
            if pe_series is not None:
                source_name = "中证"

    # 3. 提取最新 PE-TTM、5年百分位及最新日期（绝不用写死基准值冒充）
    if pe_series is not None and len(pe_series) >= 15:
        cutoff = pe_series.index.max() - pd.Timedelta(days=365 * 5 + 2)
        pe_5y = pe_series[pe_series.index >= cutoff].dropna()
        current_pe = float(pe_5y.iloc[-1])
        pe_percentile = float((pe_5y <= current_pe).mean() * 100.0)
        latest_dt = pd.Timestamp(pe_5y.index[-1]).date()
        latest_date = latest_dt.strftime("%Y-%m-%d")
    elif snap and snap.get("pe") and snap.get("pe_over_history") is not None:
        current_pe = float(snap["pe"])
        pe_percentile = float(snap["pe_over_history"]) * 100.0
        raw_d = str(snap.get("date") or "")[:10]
        try:
            latest_dt = dt.date.fromisoformat(raw_d)
        except ValueError:
            latest_dt = dt.date.today()
        latest_date = latest_dt.strftime("%Y-%m-%d")
    else:
        print(f"[未收录] {name} ({display_code}): 蛋卷/中证公开接口暂未返回实时 PE-TTM，已归入未收录提示。")
        return {
            "status": "unavailable",
            "code": core_code,
            "display_code": display_code,
            "name": name,
            "reason": "免费公开接口暂未收录",
        }

    # 4. 新鲜度校验：若最新交易日距今超过 10 天，判定为接口已停更，禁止生成定投信号
    stale_days = (dt.date.today() - latest_dt).days
    if stale_days > MAX_STALE_DAYS:
        print(f"[数据停更] {name} ({display_code}): 最新日期为 {latest_date}（已滞后 {stale_days} 天），已自动屏蔽定投信号。")
        return {
            "status": "stale",
            "code": core_code,
            "display_code": display_code,
            "name": name,
            "current_pe": round(current_pe, 2),
            "latest_date": latest_date,
            "stale_days": stale_days,
            "source": source_name,
        }

    # 5. 提取 PB-MRQ 及 5 年分位（允许为空，绝不编造）
    current_pb: float | None = None
    pb_percentile: float | None = None
    if pb_series is not None and len(pb_series) >= 15:
        cutoff_pb = pb_series.index.max() - pd.Timedelta(days=365 * 5 + 2)
        pb_5y = pb_series[pb_series.index >= cutoff_pb].dropna()
        current_pb = round(float(pb_5y.iloc[-1]), 2)
        pb_percentile = round(float((pb_5y <= float(pb_5y.iloc[-1])).mean() * 100.0), 1)
    elif snap and snap.get("pb") and snap.get("pb_over_history") is not None:
        current_pb = round(float(snap["pb"]), 2)
        pb_percentile = round(float(snap["pb_over_history"]) * 100.0, 1)

    # 6. 提取股息率与 ROE（允许为空，绝不编造）
    div_yield: float | None = None
    if snap and snap.get("yeild") is not None:
        raw_y = float(snap["yeild"] or 0.0)
        if raw_y > 0:
            div_yield = round(raw_y * 100.0 if raw_y < 0.5 else raw_y, 2)

    roe_pct: float | None = None
    if snap and snap.get("roe") is not None:
        raw_roe = float(snap["roe"] or 0.0)
        if raw_roe > 0:
            roe_pct = round(raw_roe * 100.0 if raw_roe < 1.0 else raw_roe, 1)

    # 7. 计算综合估值分位与定投信号
    composite_pct = round(
        0.60 * pe_percentile + 0.40 * pb_percentile if pb_percentile is not None else pe_percentile,
        1,
    )
    tier = classify_percentile(pe_percentile)
    dca_action = get_dca_action(
        composite_pct=composite_pct,
        pe_pct=pe_percentile,
        pb_pct=pb_percentile,
        div_yield=div_yield,
        is_dividend_idx=(idx_style == "dividend"),
    )

    print(f"[实时有效] {name} ({display_code}): PE-TTM={current_pe:.2f} ({pe_percentile:.1f}%), 日期={latest_date} [{source_name}]")
    return {
        "status": "ok",
        "code": core_code,
        "display_code": display_code,
        "name": name,
        "current_pe": round(current_pe, 2),
        "pe_percentile": round(pe_percentile, 1),
        "current_pb": current_pb,
        "pb_percentile": pb_percentile,
        "div_yield": div_yield,
        "roe": roe_pct,
        "composite_pct": composite_pct,
        "dca_action": dca_action,
        "tier_idx": tier["idx"],
        "tier_label": tier["label"],
        "latest_date": latest_date,
        "source": source_name,
    }


# ============================================================================
# 5. 微信 HTML 专业多维估值与定投晚报生成（真实有效区 + 停更/未收录透明告警区）
# ============================================================================
def build_wechat_html(
    ok_records: list[dict[str, Any]],
    stale_records: list[dict[str, Any]],
    unavail_records: list[dict[str, Any]],
    cn_bond_10y: float = 2.12,
    compact_only: bool = False,
) -> str:
    """构建无外部图片、真实防伪、带新鲜度校验的微信 HTML 估值晚报。"""
    df_sorted = pd.DataFrame(ok_records).sort_values("pe_percentile", ascending=True).reset_index(drop=True)
    report_date = df_sorted["latest_date"].max() if not df_sorted.empty else dt.date.today().strftime("%Y-%m-%d")

    cnt_low = int((df_sorted["pe_percentile"] < 30).sum())
    cnt_mid_low = int(((df_sorted["pe_percentile"] >= 30) & (df_sorted["pe_percentile"] < 50)).sum())
    cnt_mid_high = int(((df_sorted["pe_percentile"] >= 50) & (df_sorted["pe_percentile"] <= 70)).sum())
    cnt_high = int((df_sorted["pe_percentile"] > 70).sum())

    hs300_row = df_sorted[df_sorted["code"] == "000300"]
    erp_line = ""
    if not hs300_row.empty:
        hs300_pe = float(hs300_row["current_pe"].iloc[0])
        erp_val = (100.0 / hs300_pe) - cn_bond_10y
        if erp_val >= 5.5:
            erp_desc = "股债性价比极高(股显著优于债)"
        elif erp_val >= 4.2:
            erp_desc = "股票性价比偏高(偏股配置)"
        elif erp_val >= 3.0:
            erp_desc = "股债性价比均衡"
        else:
            erp_desc = "股票溢价偏低(注意防御)"
        erp_line = f'<div>⚖️ <b>沪深300股债利差(ERP)</b>：<b>{erp_val:.2f}%</b> — {erp_desc}</div>'

    cn_hk_df = df_sorted[~df_sorted["code"].isin(OVERSEAS_CODES)]
    market_temp = float(cn_hk_df["composite_pct"].mean()) if not cn_hk_df.empty else float(df_sorted["composite_pct"].mean())

    if market_temp < 30.0:
        mkt_status = "🟢历史低位(高性价比击球区)"
    elif market_temp < 50.0:
        mkt_status = "🔵中枢偏低(适合标准定投)"
    elif market_temp <= 70.0:
        mkt_status = "🟠中枢偏高(控制仓位精选)"
    else:
        mkt_status = "🔴历史高位(分批止盈为主)"

    bar_rows = []
    table_rows = []
    for _, r in df_sorted.iterrows():
        t_idx = int(r["tier_idx"])
        pct = max(2.0, min(100.0, float(r["pe_percentile"])))
        short_dt = str(r["latest_date"])[5:]  # MM-DD

        pb_html = (
            f'<b>{r["current_pb"]:.2f}</b><br><i>{r["pb_percentile"]:.0f}%分位</i>'
            if pd.notna(r["current_pb"])
            else "<i>—</i>"
        )
        dy_html = (
            f'<b>{r["div_yield"]:.1f}%</b>' + (f'<br><i>ROE {r["roe"]:.0f}%</i>' if pd.notna(r["roe"]) else "")
            if pd.notna(r["div_yield"])
            else "<i>—</i>"
        )

        if not compact_only:
            bar_rows.append(
                f'<div class="bi"><div class="bh"><b>{r["name"]}<i>({r["display_code"]})</i></b>'
                f'<s class="bp t{t_idx}">PE-TTM {r["current_pe"]:.1f}({r["pe_percentile"]:.0f}%) · {r["dca_action"]}</s></div>'
                f'<div class="bt"><i class="b{t_idx}" style="width:{pct:.1f}%"></i></div></div>'
            )
        table_rows.append(
            f'<tr><td><b>{r["name"]}</b><br><i>{r["display_code"]} · {r["source"]}{short_dt}</i></td>'
            f'<td class="c{t_idx}"><b>{r["current_pe"]:.1f}</b><br><i>{r["pe_percentile"]:.0f}%分位</i></td>'
            f'<td>{pb_html}</td>'
            f'<td>{dy_html}</td>'
            f'<td><s class="tg t{t_idx}">{r["tier_label"]}</s><br><b class="da">{r["dca_action"]}</b></td></tr>'
        )

    # 底部异常/未收录透明告警卡片（绝不混入定投打分）
    notice_blocks = []
    if stale_records:
        stale_items = "、".join(
            f'{x["name"]}({x["display_code"]}, 停更于{x["latest_date"]})' for x in stale_records
        )
        notice_blocks.append(
            f'<div class="nw"><b>⏸️ 数据停更已隔离（{len(stale_records)}只，已屏蔽定投信号）</b>：{stale_items}</div>'
        )
    if unavail_records:
        unavail_items = "、".join(f'{x["name"]}({x["display_code"]})' for x in unavail_records)
        notice_blocks.append(
            f'<div class="nu"><b>ℹ️ 免费接口暂未收录（{len(unavail_records)}只，无实时数据）</b>：{unavail_items}</div>'
        )
    notice_html = f'<div class="cd">{"".join(notice_blocks)}</div>' if notice_blocks else ""

    style_css = (
        "<style>"
        ".vw{font-family:-apple-system,'PingFang SC',sans-serif;background:#F1F5F9;padding:8px;color:#0F172A;line-height:1.4}"
        ".hd{background:linear-gradient(135deg,#0F172A,#1E3A8A);color:#FFF;padding:12px;border-radius:10px;margin-bottom:8px}"
        ".ht{font-size:15px;font-weight:700}.hs{font-size:11px;color:#CBD5E1}"
        ".mb{background:rgba(255,255,255,.1);border-radius:7px;padding:7px 9px;margin-top:8px;font-size:11px;color:#F8FAFC}"
        ".kg{display:flex;gap:4px;margin-top:8px;text-align:center}.kc{flex:1;border-radius:7px;padding:4px 2px}"
        ".kv{font-size:14px;font-weight:700}.kl{font-size:9.5px}"
        ".cd{background:#FFF;border-radius:9px;padding:10px;margin-bottom:8px;border:1px solid #E2E8F0}"
        ".ct{font-size:13px;font-weight:700;color:#0F172A;margin-bottom:3px}.cs{font-size:10.5px;color:#64748B;margin-bottom:8px}"
        ".bi{margin-bottom:8px}.bh{display:flex;justify-content:space-between;align-items:center;font-size:11.5px;margin-bottom:2px}"
        ".bh b i{font-style:normal;font-weight:400;color:#64748B;font-size:9.5px;margin-left:2px}"
        "s.bp,s.tg{text-decoration:none;font-weight:700;padding:1px 5px;border-radius:6px;font-size:9.5px;display:inline-block}"
        ".bt{height:9px;background:linear-gradient(90deg,#ECFDF5 0,#ECFDF5 29.7%,#94A3B8 30%,#EFF6FF 30.3%,#EFF6FF 49.7%,#94A3B8 50%,#FFFBEB 50.3%,#FFFBEB 69.7%,#94A3B8 70%,#FEF2F2 70.3%,#FEF2F2 100%);border-radius:5px;overflow:hidden;border:1px solid #E2E8F0}"
        ".bt i{display:block;height:100%;border-radius:5px}"
        ".t0{color:#065F46;background:#D1FAE5}.t1{color:#1E40AF;background:#DBEAFE}.t2{color:#92400E;background:#FEF3C7}.t3{color:#991B1B;background:#FEE2E2}"
        ".b0{background:#10B981}.b1{background:#3B82F6}.b2{background:#F59E0B}.b3{background:#EF4444}"
        ".c0{color:#065F46}.c1{color:#1E40AF}.c2{color:#92400E}.c3{color:#991B1B}"
        "table.vt{width:100%;border-collapse:collapse;font-size:10.5px}"
        ".vt th{background:#1E293B;color:#FFF;padding:5px 2px;font-size:9.5px;text-align:right}.vt th:first-child{text-align:left}.vt th:last-child{text-align:center}"
        ".vt td{padding:5px 2px;border-bottom:1px solid #E2E8F0;text-align:right;vertical-align:middle}.vt td:first-child{text-align:left}.vt td:last-child{text-align:center}"
        ".vt tr:nth-child(even){background:#F8FAFC}.vt td i{font-style:normal;font-size:8.8px;color:#64748B}"
        ".da{font-size:9px;color:#334155}"
        ".nw{background:#FEF3C7;color:#92400E;padding:6px 8px;border-radius:6px;font-size:10.5px;margin-bottom:5px}"
        ".nu{background:#F1F5F9;color:#475569;padding:6px 8px;border-radius:6px;font-size:10.5px}"
        "</style>"
    )

    bar_section = ""
    if not compact_only:
        bar_section = (
            f'<div class="cd"><div class="ct">🎯 近5年PE-TTM百分位与定投信号（仅展示实时有效数据）</div>'
            f'<div class="cs">刻度线：<b class="c0">30%低估</b> / <b class="c1">50%中位</b> / <b class="c3">70%高估</b></div>'
            f'{"".join(bar_rows)}</div>'
        )

    html = (
        f'{style_css}<div class="vw">'
        f'<div class="hd"><div class="ht">📈 每日指数估值晚报 (PE-TTM滚动市盈率)</div>'
        f'<div class="hs">数据日期：{report_date} · 实时有效 {len(df_sorted)} 只（停更 {len(stale_records)} / 未收录 {len(unavail_records)}）</div>'
        f'<div class="mb">'
        f'<div>🧭 <b>A/港股整体温度</b>：<b>{market_temp:.1f}°C</b> — {mkt_status}</div>'
        f'{erp_line}'
        f'</div>'
        f'<div class="kg">'
        f'<div class="kc" style="background:rgba(16,185,129,.2);border:1px solid #10B981"><div class="kv" style="color:#34D399">{cnt_low}</div><div class="kl" style="color:#D1FAE5">低估&lt;30%</div></div>'
        f'<div class="kc" style="background:rgba(59,130,246,.2);border:1px solid #3B82F6"><div class="kv" style="color:#60A5FA">{cnt_mid_low}</div><div class="kl" style="color:#DBEAFE">适中30-50%</div></div>'
        f'<div class="kc" style="background:rgba(245,158,11,.2);border:1px solid #F59E0B"><div class="kv" style="color:#FBBF24">{cnt_mid_high}</div><div class="kl" style="color:#FEF3C7">偏高50-70%</div></div>'
        f'<div class="kc" style="background:rgba(239,68,68,.2);border:1px solid #EF4444"><div class="kv" style="color:#F87171">{cnt_high}</div><div class="kl" style="color:#FEE2E2">高估&gt;70%</div></div>'
        f'</div></div>'
        f'{bar_section}'
        f'<div class="cd" style="overflow-x:auto"><div class="ct" style="margin-bottom:5px">📋 多维估值交叉印证与定投明细表</div>'
        f'<table class="vt"><thead><tr><th>指数(来源/日期)</th><th>PE-TTM(5年分位)</th><th>PB(MRQ)</th><th>股息率/ROE</th><th>状态与定投建议</th></tr></thead>'
        f'<tbody>{"".join(table_rows)}</tbody></table></div>'
        f'{notice_html}</div>'
    )
    print(f"[HTML统计] 微信报表总字符数: {len(html)} / 20000 字上限")
    return html


def send_via_pushplus(token: str, title: str, html_content: str, fallback_html: str = "", topic: str = "") -> bool:
    """通过 PushPlus 将 HTML 报表推送到个人微信；若遇字数超限自动降级发送精简版。"""
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
        if res_json.get("code") == 999 and fallback_html:
            print("[自动降级] 检测到内容字数限制，正在使用精简版 HTML 重新发送...")
            payload["content"] = fallback_html
            resp2 = requests.post(url, json=payload, timeout=15)
            res_json2 = resp2.json()
            if res_json2.get("code") == 200:
                print(f"[推送成功] 精简版消息已受理，流水号: {res_json2.get('data')}")
                return True
        print(f"[推送失败] PushPlus 返回异常: {res_json}")
        return False
    except Exception as e:
        print(f"[推送异常] 请求 PushPlus 接口出错: {e}")
        return False


# ============================================================================
# 6. 主程序入口
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
    parser.add_argument("--output-png", default="", help="已废弃参数（保留向下兼容）")
    args = parser.parse_args()

    config_file = args.excel if args.excel else args.config
    if not pathlib.Path(config_file).exists() and pathlib.Path("indices.xlsx").exists():
        config_file = "indices.xlsx"

    html_path = pathlib.Path(args.output_html)
    title_path = pathlib.Path(args.title_file)
    fallback_html = ""

    if not args.send_only:
        start_ts = time.perf_counter()
        indices = load_indices_config(config_file)
        print(f"[读取配置] 从 {config_file} 成功读取 {len(indices)} 只有效股票指数。")

        batch_snapshot = {} if args.demo else fetch_danjuan_batch_snapshot()

        ok_records: list[dict[str, Any]] = []
        stale_records: list[dict[str, Any]] = []
        unavail_records: list[dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=8) as executor:
            future_map = {
                executor.submit(compute_index_valuation, item, batch_snapshot, args.demo): item
                for item in indices
            }
            for fut in as_completed(future_map):
                try:
                    res = fut.result(timeout=12)
                    st = res.get("status")
                    if st == "ok":
                        ok_records.append(res)
                    elif st == "stale":
                        stale_records.append(res)
                    else:
                        unavail_records.append(res)
                except Exception as exc:
                    item = future_map[fut]
                    print(f"[异常跳过] 指数 {item['name']} ({item['display_code']}) 拉取异常: {exc}")
                    unavail_records.append({
                        "status": "unavailable",
                        "code": item["code"],
                        "display_code": item["display_code"],
                        "name": item["name"],
                        "reason": "接口请求异常",
                    })

        total_elapsed = time.perf_counter() - start_ts
        print(
            f"[拉取汇总] 实时有效 {len(ok_records)} 只 | 数据停更 {len(stale_records)} 只 | "
            f"未收录 {len(unavail_records)} 只，总耗时 {total_elapsed:.2f} 秒。"
        )

        if not ok_records:
            print("[错误] 未能获取到任何实时有效的指数估值数据，程序终止。")
            sys.exit(1)

        html_content = build_wechat_html(ok_records, stale_records, unavail_records, compact_only=False)
        fallback_html = build_wechat_html(ok_records, stale_records, unavail_records, compact_only=True)
        html_path.write_text(html_content, encoding="utf-8")

        report_date = max(r["latest_date"] for r in ok_records)
        low_cnt = sum(1 for r in ok_records if r["pe_percentile"] < 30)
        title = f"📊 PE-TTM估值晚报 ({report_date}) | 低估 {low_cnt} 只"
        title_path.write_text(title, encoding="utf-8")
        print(f"[HTML报表] 已生成微信 HTML 报表: {html_path}")
    else:
        if not html_path.exists():
            print(f"[错误] 未找到已生成的 HTML 报表文件: {html_path}")
            sys.exit(1)
        html_content = html_path.read_text(encoding="utf-8")
        title = title_path.read_text(encoding="utf-8").strip() if title_path.exists() else "📊 每日PE-TTM估值晚报"

    if args.skip_push:
        return

    pushplus_token = os.environ.get("PUSHPLUS_TOKEN", "").strip()
    pushplus_topic = os.environ.get("PUSHPLUS_TOPIC", "").strip()

    if pushplus_token:
        ok = send_via_pushplus(pushplus_token, title, html_content, fallback_html=fallback_html, topic=pushplus_topic)
        if not ok:
            sys.exit(1)
    else:
        print("[错误] 未检测到环境变量 PUSHPLUS_TOKEN！请在 GitHub Settings -> Secrets and variables -> Actions 中添加 PUSHPLUS_TOKEN。")
        sys.exit(1)


if __name__ == "__main__":
    main()
