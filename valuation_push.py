#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日指数估值微信推送系统 (GitHub Actions 极速 JSON + 海外指数增强版)
功能：
1. 默认读取 JSON 配置文件 (indices.json，支持在 GitHub 网页直接在线编辑，毫秒级加载)，同时向下兼容 Excel (.xlsx)
2. 全面支持 A 股、港股及海外指数（纳斯达克 NDX.GI、标普500 SPX.GI、日经225 N225.GI、印度 SENSEX.BO、恒生科技 HSTECH.HI、恒生指数 HSI.HI 等 Wind 后缀或标准代码）
3. 多源自动拉取近 5 年历史 PE 数据（AkShare 韭圈儿 FundDB + 雪球蛋卷估值 + 中证指数 + 乐咕乐股）
4. 按四档估值区间（<30% 低估、30%–50% 适中偏低、50%–70% 适中偏高、>70% 高估）生成高清看板图与微信 HTML 晚报
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import sys
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import requests


# ============================================================================
# 1. 估值分档与专业配色体系 (<30% 低估 | 30%–50% 适中偏低 | 50%–70% 适中偏高 | >70% 高估)
# ============================================================================
VALUATION_TIERS = [
    {
        "key": "undervalued",
        "label": "低估",
        "range_text": "<30%",
        "min_pct": 0.0,
        "max_pct": 30.0,
        "color": "#10B981",       # 翡翠绿 (柱状图主色)
        "text_color": "#065F46",  # 深绿文字
        "bg_color": "#D1FAE5",    # 浅绿徽章底色
        "zone_bg": "#ECFDF5",     # 图表背景带底色
        "dot": "🟢",
    },
    {
        "key": "normal_low",
        "label": "适中偏低",
        "range_text": "30%–50%",
        "min_pct": 30.0,
        "max_pct": 50.0,
        "color": "#3B82F6",       # 晴空蓝 (柱状图主色)
        "text_color": "#1E40AF",  # 深蓝文字
        "bg_color": "#DBEAFE",    # 浅蓝徽章底色
        "zone_bg": "#EFF6FF",     # 图表背景带底色
        "dot": "🔵",
    },
    {
        "key": "normal_high",
        "label": "适中偏高",
        "range_text": "50%–70%",
        "min_pct": 50.0,
        "max_pct": 70.0,
        "color": "#F59E0B",       # 琥珀橙 (柱状图主色)
        "text_color": "#92400E",  # 深橙文字
        "bg_color": "#FEF3C7",    # 浅橙徽章底色
        "zone_bg": "#FFFBEB",     # 图表背景带底色
        "dot": "🟠",
    },
    {
        "key": "overvalued",
        "label": "高估",
        "range_text": ">70%",
        "min_pct": 70.0,
        "max_pct": 100.01,
        "color": "#EF4444",       # 珊瑚红 (柱状图主色)
        "text_color": "#991B1B",  # 深红文字
        "bg_color": "#FEE2E2",    # 浅红徽章底色
        "zone_bg": "#FEF2F2",     # 图表背景带底色
        "dot": "🔴",
    },
]


def classify_percentile(pct: float) -> dict[str, Any]:
    """根据 5 年 PE 百分位返回对应的四档估值区间配色与标签配置。"""
    if pct < 30.0:
        return VALUATION_TIERS[0]
    elif pct < 50.0:
        return VALUATION_TIERS[1]
    elif pct <= 70.0:
        return VALUATION_TIERS[2]
    else:
        return VALUATION_TIERS[3]


# ============================================================================
# 2. 海内外指数与 Wind 代码智能映射表（支持直接填写 Wind 代码、标准代码或简称）
# ============================================================================
INDEX_ALIAS_MAP: dict[str, dict[str, Any]] = {
    # A50 指数
    "8841233": {
        "display_code": "8841233.WI",
        "danjuan": ["CSI930050", "XIN9", "SH000016"],
        "funddb_names": ["富时中国A50", "中证A50", "MSCI中国A50互联互通", "上证50"],
        "funddb_codes": ["XIN9", "930050", "8841233"],
        "csindex": "930050",
    },
    "A50": {
        "display_code": "A50",
        "danjuan": ["CSI930050", "XIN9"],
        "funddb_names": ["富时中国A50", "中证A50", "MSCI中国A50互联互通"],
        "funddb_codes": ["XIN9", "930050"],
        "csindex": "930050",
    },
    # 沪深300
    "000300": {
        "display_code": "000300.SH",
        "danjuan": ["SH000300"],
        "funddb_names": ["沪深300"],
        "funddb_codes": ["000300"],
        "csindex": "000300",
    },
    # 中证全指
    "000985": {
        "display_code": "000985.CSI",
        "danjuan": ["CSI000985", "SH000985"],
        "funddb_names": ["中证全指"],
        "funddb_codes": ["000985"],
        "csindex": "000985",
    },
    # 中证500
    "000905": {
        "display_code": "000905.SH",
        "danjuan": ["SH000905"],
        "funddb_names": ["中证500"],
        "funddb_codes": ["000905"],
        "csindex": "000905",
    },
    # 中证800
    "000906": {
        "display_code": "000906.SH",
        "danjuan": ["SH000906"],
        "funddb_names": ["中证800"],
        "funddb_codes": ["000906"],
        "csindex": "000906",
    },
    # 恒生科技
    "HSTECH": {
        "display_code": "HSTECH.HI",
        "danjuan": ["HKHSTECH", "HSTECH"],
        "funddb_names": ["恒生科技", "恒生科技指数"],
        "funddb_codes": ["HSTECH", "HSTECH.HI"],
        "csindex": "",
    },
    # 中证红利
    "000922": {
        "display_code": "000922.CSI",
        "danjuan": ["SH000922", "CSI000922"],
        "funddb_names": ["中证红利", "中证红利指数"],
        "funddb_codes": ["000922"],
        "csindex": "000922",
    },
    # 纳斯达克100
    "NDX": {
        "display_code": "NDX.GI",
        "danjuan": ["NDX", "IXIC"],
        "funddb_names": ["纳斯达克100", "纳指100", "纳斯达克", "纳斯达克指数"],
        "funddb_codes": ["NDX", "NDX.GI", "IXIC"],
        "csindex": "",
    },
    "IXIC": {
        "display_code": "IXIC.GI",
        "danjuan": ["IXIC", "NDX"],
        "funddb_names": ["纳斯达克", "纳斯达克指数", "纳斯达克100"],
        "funddb_codes": ["IXIC", "NDX"],
        "csindex": "",
    },
    # 日经225
    "N225": {
        "display_code": "N225.GI",
        "danjuan": ["N225"],
        "funddb_names": ["日经225", "日经225指数"],
        "funddb_codes": ["N225", "N225.GI"],
        "csindex": "",
    },
    # 印度 SENSEX / MSCI 印度
    "SENSEX": {
        "display_code": "SENSEX.BO",
        "danjuan": ["935600", "SENSEX"],
        "funddb_names": ["印度孟买SENSEX", "孟买SENSEX30", "MSCI印度", "印度SENSEX"],
        "funddb_codes": ["SENSEX", "SENSEX.BO", "935600"],
        "csindex": "",
    },
    # 标普500
    "SPX": {
        "display_code": "SPX.GI",
        "danjuan": ["SP500", "SPX"],
        "funddb_names": ["标普500", "标准普尔500", "标普500指数"],
        "funddb_codes": ["SPX", "SP500", "SPX.GI"],
        "csindex": "",
    },
    "SP500": {
        "display_code": "SPX.GI",
        "danjuan": ["SP500", "SPX"],
        "funddb_names": ["标普500", "标准普尔500"],
        "funddb_codes": ["SPX", "SP500"],
        "csindex": "",
    },
    # 恒生指数
    "HSI": {
        "display_code": "HSI.HI",
        "danjuan": ["HKHSI", "HSI"],
        "funddb_names": ["恒生指数", "恒生"],
        "funddb_codes": ["HSI", "HSI.HI", "HKHSI"],
        "csindex": "",
    },
    # 创业板指
    "399006": {
        "display_code": "399006.SZ",
        "danjuan": ["SZ399006"],
        "funddb_names": ["创业板指", "创业板指数"],
        "funddb_codes": ["399006"],
        "csindex": "399006",
    },
    # 红利低波
    "H30269": {
        "display_code": "H30269.CSI",
        "danjuan": ["CSIH30269", "H30269", "SHH30269"],
        "funddb_names": ["红利低波", "中证红利低波", "中证红利低波动"],
        "funddb_codes": ["H30269"],
        "csindex": "H30269",
    },
    # 国证自由现金流
    "980092": {
        "display_code": "980092.CNI",
        "danjuan": ["CNI980092", "SZ980092", "980092", "CSI932365"],
        "funddb_names": ["自由现金流", "国证自由现金流", "中证全指自由现金流", "自由现金"],
        "funddb_codes": ["980092", "399368", "932365"],
        "csindex": "932365",
    },
    # 中证A500
    "000510": {
        "display_code": "000510.SH",
        "danjuan": ["SH000510", "CSI000510", "000510"],
        "funddb_names": ["中证A500", "A500"],
        "funddb_codes": ["000510"],
        "csindex": "000510",
    },
    # 港股通高股息
    "930915": {
        "display_code": "930915.CSI",
        "danjuan": ["CSI930915", "SH930915", "930915"],
        "funddb_names": ["港股通高股息", "中证港股通高股息", "港股通高股息投资", "港股通高"],
        "funddb_codes": ["930915"],
        "csindex": "930915",
    },
}

# 纯债券指数黑名单（债券指数无 PE 市盈率，自动跳过并提示）
BOND_INDEX_CODES = {"H11001", "000012", "000832", "CBA00101"}


def normalize_index_code(raw_code: Any) -> tuple[str, str]:
    """
    标准化指数代码，支持 Wind 后缀 (.WI / .SH / .SZ / .CSI / .HI / .GI / .BO / .CNI) 与简写代码：
    返回 (core_code, display_code)
    """
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
# 3. 配置文件加载器：原生支持 JSON (极速、支持 GitHub 在线编辑) 与 Excel (.xlsx)
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
        if isinstance(data, dict):
            items = data.get("indices", [])
        elif isinstance(data, list):
            items = data
        else:
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
                print(f"[跳过] {raw_name} ({display_code}) 为纯债券指数（无 PE 市盈率指标），已自动跳过。")
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
            print(f"[跳过] {clean_name} ({display_code}) 为纯债券指数（无 PE 市盈率指标），已自动跳过。")
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
# 4. 多源近 5 年 PE 历史数据抓取（全面覆盖 A股 / 港股 / 美股 / 日经 / 印度指数）
# ============================================================================
_FUNDDB_CATALOG_CACHE: pd.DataFrame | None = None


def fetch_pe_history_funddb(core_code: str, name: str) -> pd.Series | None:
    """
    数据源 1（主力多市场源）：AkShare 韭圈儿 (FundDB)
    支持 A股、港股（恒生指数、恒生科技）、美股（纳斯达克100、标普500）、日经225、印度SENSEX 等近 5 年日频 PE 历史。
    """
    global _FUNDDB_CATALOG_CACHE
    try:
        import akshare as ak
    except ImportError:
        return None

    five_years_ago = pd.Timestamp.now() - pd.Timedelta(days=365 * 5 + 5)
    alias_info = INDEX_ALIAS_MAP.get(core_code, {})
    candidate_names: list[str] = list(alias_info.get("funddb_names", []))
    candidate_codes: list[str] = list(alias_info.get("funddb_codes", []))
    if name and name not in candidate_names:
        candidate_names.append(name)
    if core_code and core_code not in candidate_codes:
        candidate_codes.append(core_code)

    try:
        if _FUNDDB_CATALOG_CACHE is None:
            _FUNDDB_CATALOG_CACHE = ak.index_value_name_funddb()
        cat_df = _FUNDDB_CATALOG_CACHE
        if cat_df is not None and not cat_df.empty:
            code_col = next((c for c in cat_df.columns if "代码" in c), None)
            name_col = next((c for c in cat_df.columns if "名称" in c or "指数" in c), None)
            if code_col and name_col:
                for cand_c in candidate_codes:
                    for _, row in cat_df.iterrows():
                        c_val = str(row[code_col]).upper().strip()
                        c_base = c_val.split(".")[0]
                        if cand_c.upper() == c_base or cand_c.upper() == c_val:
                            matched = str(row[name_col]).strip()
                            if matched not in candidate_names:
                                candidate_names.insert(0, matched)
                for cand_n in list(candidate_names):
                    for _, row in cat_df.iterrows():
                        n_val = str(row[name_col]).strip()
                        if cand_n == n_val or cand_n in n_val or n_val in cand_n:
                            if n_val not in candidate_names:
                                candidate_names.append(n_val)
    except Exception:
        pass

    for sym_name in dict.fromkeys(candidate_names):
        try:
            hist_df = ak.index_value_hist_funddb(symbol=sym_name, indicator="市盈率")
            if hist_df is None or hist_df.empty:
                continue
            date_col = next((c for c in hist_df.columns if "日期" in c or "date" in c.lower()), hist_df.columns[0])
            pe_col = next((c for c in hist_df.columns if "市盈率" in c or "pe" in c.lower()), hist_df.columns[1])
            hist_df["date"] = pd.to_datetime(hist_df[date_col], errors="coerce")
            hist_df["pe"] = pd.to_numeric(hist_df[pe_col], errors="coerce")
            hist_df = hist_df.dropna(subset=["date", "pe"]).sort_values("date")
            hist_df = hist_df[(hist_df["date"] >= five_years_ago) & (hist_df["pe"] > 0)]
            if len(hist_df) >= 30:
                return hist_df.set_index("date")["pe"]
        except Exception:
            continue

    return None


def fetch_pe_history_danjuan(core_code: str) -> pd.Series | None:
    """
    数据源 2（高速接口源）：雪球/蛋卷基金 5 年指数 PE 历史接口
    支持 SH000300、SZ399006、HKHSI、HKHSTECH、NDX、SP500、N225、935600(MSCI印度) 等。
    """
    alias_info = INDEX_ALIAS_MAP.get(core_code, {})
    candidates = list(alias_info.get("danjuan", []))
    candidates.extend([
        core_code,
        f"SH{core_code}",
        f"SZ{core_code}",
        f"CSI{core_code}",
        f"HK{core_code}",
    ])
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://danjuanfunds.com/djmodule/value-center",
    }
    for sym in dict.fromkeys(candidates):
        url = f"https://danjuanfunds.com/djapi/index_eva/pe_history/{sym}?day=5y"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            payload = resp.json()
            items = payload.get("data", {}).get("index_eva_pe_growths", [])
            if not items or len(items) < 30:
                continue
            df = pd.DataFrame(items)
            if "pe" not in df.columns or "ts" not in df.columns:
                continue
            df["date"] = pd.to_datetime(df["ts"], unit="ms")
            df["pe"] = pd.to_numeric(df["pe"], errors="coerce")
            df = df.dropna(subset=["pe"]).sort_values("date")
            df = df[df["pe"] > 0]
            if len(df) >= 30:
                return df.set_index("date")["pe"]
        except Exception:
            continue
    return None


def fetch_pe_history_csindex(core_code: str) -> pd.Series | None:
    """数据源 3（中证官方兜底）：AkShare 中证指数估值接口。"""
    try:
        import akshare as ak
        alias_info = INDEX_ALIAS_MAP.get(core_code, {})
        cs_code = alias_info.get("csindex", core_code)
        if not cs_code:
            return None
        cs_df = ak.stock_zh_index_value_csindex(symbol=cs_code)
        if cs_df is not None and not cs_df.empty:
            date_col = next((c for c in cs_df.columns if "日期" in c), cs_df.columns[0])
            pe_col = next((c for c in cs_df.columns if "市盈率1" in c or "市盈率" in c), None)
            if pe_col:
                cs_df["date"] = pd.to_datetime(cs_df[date_col], errors="coerce")
                cs_df["pe"] = pd.to_numeric(cs_df[pe_col], errors="coerce")
                cs_df = cs_df.dropna(subset=["date", "pe"]).sort_values("date")
                cs_df = cs_df[cs_df["pe"] > 0]
                if len(cs_df) >= 15:
                    return cs_df.set_index("date")["pe"]
    except Exception:
        pass
    return None


def generate_demo_pe_series(core_code: str) -> pd.Series:
    """仅当携带 --demo 参数用于离线预览调试时生成确定性 5 年历史 PE 序列。"""
    preset_profiles = {
        "399006":  (28.60, 24.0, 58.0, 18.5),   # 创业板指 -> <30% 低估
        "HSTECH":  (22.40, 17.5, 48.0, 21.3),   # 恒生科技 -> <30% 低估
        "000905":  (21.80, 18.5, 29.5, 22.1),   # 中证500  -> <30% 低估
        "930915":  (6.25,  5.1,  8.4,  24.8),   # 港股通高 -> <30% 低估
        "000300":  (12.15, 10.2, 16.8, 26.4),   # 沪深300  -> <30% 低估
        "000906":  (13.45, 11.3, 18.2, 28.2),   # 中证800  -> <30% 低估
        "HSI":     (9.45,  8.1,  12.8, 33.6),   # 恒生指数 -> 30%–50% 适中偏低
        "000985":  (16.50, 13.8, 21.5, 36.4),   # 中证全指 -> 30%–50% 适中偏低
        "000510":  (14.10, 11.8, 18.5, 38.9),   # 中证A500 -> 30%–50% 适中偏低
        "000922":  (7.15,  5.8,  8.6,  41.2),   # 中证红利 -> 30%–50% 适中偏低
        "8841233": (10.35, 8.6,  13.4, 44.5),   # A50指数  -> 30%–50% 适中偏低
        "980092":  (13.80, 10.5, 16.9, 53.6),   # 自由现金 -> 50%–70% 适中偏高
        "H30269":  (7.68,  5.9,  8.8,  57.8),   # 红利低波 -> 50%–70% 适中偏高
        "N225":    (21.60, 16.2, 24.8, 63.4),   # 日经225  -> 50%–70% 适中偏高
        "SENSEX":  (23.85, 18.5, 26.2, 74.2),   # 印度SEN  -> >70% 高估
        "SPX":     (27.40, 19.2, 29.5, 81.5),   # 标普500  -> >70% 高估
        "NDX":     (33.90, 23.5, 36.8, 84.8),   # 纳斯达克 -> >70% 高估
    }
    seed = int(hashlib.md5(core_code.encode()).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)
    n_days = 1215
    dates = pd.bdate_range(end=dt.date.today(), periods=n_days)

    if core_code in preset_profiles:
        cur_pe, low_pe, high_pe, target_pct = preset_profiles[core_code]
    else:
        low_pe = round(rng.uniform(10.0, 18.0), 2)
        high_pe = round(low_pe * rng.uniform(1.6, 2.4), 2)
        target_pct = round(rng.uniform(12.0, 88.0), 1)
        cur_pe = round(low_pe + (high_pe - low_pe) * (target_pct / 100.0), 2)

    n_below = int(round((target_pct / 100.0) * (n_days - 1)))
    n_above = (n_days - 1) - n_below
    below_vals = rng.uniform(low_pe, max(low_pe + 0.05, cur_pe - 0.01), size=max(0, n_below))
    above_vals = rng.uniform(min(high_pe - 0.05, cur_pe + 0.01), high_pe, size=max(0, n_above))
    all_vals = np.concatenate([below_vals, above_vals])
    rng.shuffle(all_vals)
    all_vals = np.append(all_vals, cur_pe)
    return pd.Series(all_vals, index=dates)


def compute_index_valuation(item: dict[str, str], demo_mode: bool = False) -> dict[str, Any] | None:
    """计算单个指数的近 5 年 PE 统计指标与四档估值状态。"""
    core_code = item["code"]
    display_code = item["display_code"]
    name = item["name"]

    pe_series = None
    source_name = "实时接口"
    if not demo_mode:
        pe_series = fetch_pe_history_funddb(core_code, name)
        if pe_series is not None:
            source_name = "韭圈儿FundDB"
        else:
            pe_series = fetch_pe_history_danjuan(core_code)
            if pe_series is not None:
                source_name = "蛋卷估值"
            else:
                pe_series = fetch_pe_history_csindex(core_code)
                if pe_series is not None:
                    source_name = "中证指数"

    if pe_series is None and demo_mode:
        pe_series = generate_demo_pe_series(core_code)
        source_name = "示例数据"

    if pe_series is None or len(pe_series) < 10:
        print(f"[警告] 无法获取指数 {display_code} ({name}) 的 5 年 PE 数据，已跳过。")
        return None

    cutoff = pe_series.index.max() - pd.Timedelta(days=365 * 5 + 2)
    pe_5y = pe_series[pe_series.index >= cutoff].dropna()
    if pe_5y.empty:
        pe_5y = pe_series.dropna()

    current_pe = float(pe_5y.iloc[-1])
    latest_date = pd.Timestamp(pe_5y.index[-1]).strftime("%Y-%m-%d")

    pe_percentile = float((pe_5y <= current_pe).mean() * 100.0)
    pe_min = float(pe_5y.min())
    pe_max = float(pe_5y.max())
    pe_median = float(pe_5y.median())
    pe_p30 = float(np.percentile(pe_5y.values, 30))
    pe_p70 = float(np.percentile(pe_5y.values, 70))

    tier = classify_percentile(pe_percentile)

    return {
        "code": core_code,
        "display_code": display_code,
        "name": name,
        "current_pe": round(current_pe, 2),
        "pe_percentile": round(pe_percentile, 1),
        "pe_min": round(pe_min, 2),
        "pe_median": round(pe_median, 2),
        "pe_max": round(pe_max, 2),
        "pe_p30": round(pe_p30, 2),
        "pe_p70": round(pe_p70, 2),
        "tier_key": tier["key"],
        "tier_label": tier["label"],
        "tier_range": tier["range_text"],
        "color": tier["color"],
        "text_color": tier["text_color"],
        "bg_color": tier["bg_color"],
        "dot": tier["dot"],
        "latest_date": latest_date,
        "source": source_name,
    }


# ============================================================================
# 5. 专业估值图表生成 (Matplotlib 5年PE百分位四色区间图 + 数据汇总看板)
# ============================================================================
def configure_chinese_font() -> str:
    """配置 Matplotlib 中文字体，兼容 Ubuntu (GitHub Actions) / macOS / Windows。"""
    import matplotlib.font_manager as fm

    preferred_fonts = [
        "Noto Sans CJK SC",
        "Noto Sans CJK JP",
        "WenQuanYi Micro Hei",
        "WenQuanYi Zen Hei",
        "Microsoft YaHei",
        "PingFang SC",
        "SimHei",
        "Droid Sans Fallback",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    chosen = next((f for f in preferred_fonts if f in available), "DejaVu Sans")
    plt.rcParams["font.sans-serif"] = [chosen, "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return chosen


def render_valuation_dashboard(df: pd.DataFrame, output_png: str | pathlib.Path) -> pathlib.Path:
    """渲染专业级「每日指数 5 年 PE 估值与百分位全景看板」PNG 图片。"""
    configure_chinese_font()
    out_path = pathlib.Path(output_png)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df_sorted = df.sort_values("pe_percentile", ascending=True).reset_index(drop=True)
    n = len(df_sorted)

    chart_h = max(4.5, n * 0.48 + 1.5)
    table_h = max(3.5, (n + 2) * 0.38 + 0.8)
    total_h = chart_h + table_h + 1.8

    fig = plt.figure(figsize=(14.2, total_h), dpi=200, facecolor="#F8FAFC")
    gs = fig.add_gridspec(
        nrows=3,
        ncols=1,
        height_ratios=[1.15, chart_h, table_h],
        hspace=0.22,
        left=0.04,
        right=0.96,
        top=0.96,
        bottom=0.03,
    )

    # ── 区域 1：顶部深色金融看板抬头与四档统计卡片 ──
    ax_header = fig.add_subplot(gs[0])
    ax_header.set_axis_off()
    header_box = mpatches.FancyBboxPatch(
        (0.0, 0.08),
        1.0,
        0.88,
        boxstyle="round,pad=0.02,rounding_size=0.02",
        facecolor="#0F172A",
        edgecolor="none",
        transform=ax_header.transAxes,
    )
    ax_header.add_patch(header_box)

    report_date = df_sorted["latest_date"].max() if not df_sorted.empty else dt.date.today().strftime("%Y-%m-%d")
    ax_header.text(
        0.025,
        0.66,
        "全球及A股核心指数估值监控看板 · 5年PE及历史百分位",
        fontsize=16.5,
        fontweight="bold",
        color="#FFFFFF",
        va="center",
        transform=ax_header.transAxes,
    )
    ax_header.text(
        0.025,
        0.28,
        f"数据基准日: {report_date}   |   估值窗口: 近5年日频PE-TTM   |   监控指数: {n} 只",
        fontsize=10.2,
        color="#94A3B8",
        va="center",
        transform=ax_header.transAxes,
    )

    tier_counts = [
        ("低估 <30%", int((df_sorted["pe_percentile"] < 30).sum()), "#10B981"),
        ("适中偏低 30%-50%", int(((df_sorted["pe_percentile"] >= 30) & (df_sorted["pe_percentile"] < 50)).sum()), "#3B82F6"),
        ("适中偏高 50%-70%", int(((df_sorted["pe_percentile"] >= 50) & (df_sorted["pe_percentile"] <= 70)).sum()), "#F59E0B"),
        ("高估 >70%", int((df_sorted["pe_percentile"] > 70).sum()), "#EF4444"),
    ]
    card_w = 0.118
    start_x = 0.48
    for idx, (t_label, t_cnt, bar_c) in enumerate(tier_counts):
        cx = start_x + idx * (card_w + 0.011)
        pill = mpatches.FancyBboxPatch(
            (cx, 0.20),
            card_w,
            0.64,
            boxstyle="round,pad=0.01,rounding_size=0.015",
            facecolor="#1E293B",
            edgecolor=bar_c,
            linewidth=1.5,
            transform=ax_header.transAxes,
        )
        ax_header.add_patch(pill)
        ax_header.text(
            cx + card_w / 2,
            0.62,
            f"{t_cnt} 只",
            fontsize=13,
            fontweight="bold",
            color=bar_c,
            ha="center",
            va="center",
            transform=ax_header.transAxes,
        )
        ax_header.text(
            cx + card_w / 2,
            0.33,
            t_label,
            fontsize=8.5,
            color="#CBD5E1",
            ha="center",
            va="center",
            transform=ax_header.transAxes,
        )

    # ── 区域 2：近 5 年 PE 百分位水平柱状图 + 四色估值背景区间 ──
    ax_chart = fig.add_subplot(gs[1])
    pos = ax_chart.get_position()
    ax_chart.set_position([0.175, pos.y0, 0.96 - 0.175, pos.height])
    ax_chart.set_facecolor("#FFFFFF")

    ax_chart.axvspan(0, 30, color="#ECFDF5", alpha=0.90, zorder=0)
    ax_chart.axvspan(30, 50, color="#EFF6FF", alpha=0.90, zorder=0)
    ax_chart.axvspan(50, 70, color="#FFFBEB", alpha=0.90, zorder=0)
    ax_chart.axvspan(70, 118, color="#FEF2F2", alpha=0.90, zorder=0)

    for thresh, line_color in [
        (30, "#10B981"),
        (50, "#3B82F6"),
        (70, "#EF4444"),
    ]:
        ax_chart.axvline(thresh, color=line_color, linestyle="--", linewidth=1.3, alpha=0.75, zorder=2)

    y_pos = np.arange(n)
    df_plot = df_sorted.iloc[::-1].reset_index(drop=True)
    y_labels = [f"{r['name']} ({r['display_code']})" for _, r in df_plot.iterrows()]
    pct_vals = df_plot["pe_percentile"].values
    bar_colors = df_plot["color"].values

    ax_chart.barh(y_pos, [100] * n, height=0.56, color="#E2E8F0", alpha=0.45, zorder=1)
    bars = ax_chart.barh(
        y_pos,
        pct_vals,
        height=0.56,
        color=bar_colors,
        edgecolor="none",
        alpha=0.92,
        zorder=3,
    )

    for bar, (_, row) in zip(bars, df_plot.iterrows()):
        w = bar.get_width()
        label_str = f"{row['pe_percentile']:.1f}%  (PE: {row['current_pe']:.2f} | {row['tier_label']})"
        ax_chart.text(
            min(w + 1.2, 92.0),
            bar.get_y() + bar.get_height() / 2,
            label_str,
            va="center",
            ha="left",
            fontsize=9.0,
            fontweight="bold",
            color=row["text_color"],
            bbox=dict(
                boxstyle="round,pad=0.20,rounding_size=0.15",
                facecolor="#FFFFFF",
                edgecolor=row["color"],
                linewidth=0.8,
                alpha=0.92,
            ),
            zorder=5,
        )

    ax_chart.set_yticks(y_pos)
    ax_chart.set_yticklabels(y_labels, fontsize=9.5, fontweight="bold", color="#1E293B")
    ax_chart.set_xlim(0, 118)
    ax_chart.set_xticks([0, 15, 30, 50, 70, 85, 100])
    ax_chart.set_xticklabels(["0%", "15%", "30% (低估线)", "50% (中位线)", "70% (高估线)", "85%", "100%"], fontsize=9, color="#475569")
    ax_chart.set_title(
        "各指数近 5 年 PE 历史百分位分布图（按估值由低到高排序）",
        fontsize=12.5,
        fontweight="bold",
        color="#0F172A",
        loc="left",
        pad=12,
    )
    for spine in ["top", "right"]:
        ax_chart.spines[spine].set_visible(False)
    ax_chart.spines["left"].set_color("#CBD5E1")
    ax_chart.spines["bottom"].set_color("#CBD5E1")

    legend_patches = [
        mpatches.Patch(color="#10B981", label="低估 (<30%)"),
        mpatches.Patch(color="#3B82F6", label="适中偏低 (30%–50%)"),
        mpatches.Patch(color="#F59E0B", label="适中偏高 (50%–70%)"),
        mpatches.Patch(color="#EF4444", label="高估 (>70%)"),
    ]
    ax_chart.legend(
        handles=legend_patches,
        loc="upper right",
        ncol=4,
        frameon=True,
        facecolor="#FFFFFF",
        edgecolor="#E2E8F0",
        fontsize=9,
    )

    # ── 区域 3：底部结构化数据明细表 ──
    ax_table = fig.add_subplot(gs[2])
    ax_table.set_axis_off()
    ax_table.set_title(
        "指数 5 年 PE 估值核心数据明细表",
        fontsize=12.5,
        fontweight="bold",
        color="#0F172A",
        loc="left",
        pad=6,
    )

    col_labels = [
        "指数代码",
        "指数名称",
        "当前 PE",
        "5年PE百分位",
        "估值区间状态",
        "5年最低 PE",
        "5年中位 PE (50%)",
        "5年最高 PE",
        "30% / 70% 分位PE",
    ]
    table_data = []
    for _, r in df_sorted.iterrows():
        table_data.append([
            r["display_code"],
            r["name"],
            f"{r['current_pe']:.2f}",
            f"{r['pe_percentile']:.1f}%",
            f"{r['tier_label']} ({r['tier_range']})",
            f"{r['pe_min']:.2f}",
            f"{r['pe_median']:.2f}",
            f"{r['pe_max']:.2f}",
            f"{r['pe_p30']:.2f} / {r['pe_p70']:.2f}",
        ])

    col_widths = [0.11, 0.13, 0.09, 0.11, 0.15, 0.09, 0.11, 0.09, 0.12]
    tbl = ax_table.table(
        cellText=table_data,
        colLabels=col_labels,
        colWidths=col_widths,
        loc="center",
        cellLoc="center",
        bbox=[0.0, 0.0, 1.0, 0.93],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9.2)

    for (r_idx, c_idx), cell in tbl.get_celld().items():
        cell.set_edgecolor("#CBD5E1")
        cell.set_linewidth(0.6)
        if r_idx == 0:
            cell.set_facecolor("#1E293B")
            cell.get_text().set_color("#FFFFFF")
            cell.get_text().set_weight("bold")
            cell.set_fontsize(9.5)
        else:
            row_obj = df_sorted.iloc[r_idx - 1]
            if c_idx in (3, 4):
                cell.set_facecolor(row_obj["bg_color"])
                cell.get_text().set_color(row_obj["text_color"])
                cell.get_text().set_weight("bold")
            elif c_idx == 2:
                cell.set_facecolor("#F8FAFC" if r_idx % 2 == 0 else "#FFFFFF")
                cell.get_text().set_color("#0F172A")
                cell.get_text().set_weight("bold")
            else:
                cell.set_facecolor("#F8FAFC" if r_idx % 2 == 0 else "#FFFFFF")
                cell.get_text().set_color("#334155")

    fig.savefig(str(out_path), dpi=200)
    plt.close(fig)
    print(f"[图表] 已生成高清估值看板图片: {out_path}")
    return out_path


# ============================================================================
# 6. 微信 HTML 专业视觉报表生成 & PushPlus / 企业微信推送
# ============================================================================
def build_wechat_html(df: pd.DataFrame, chart_image_url: str = "") -> str:
    """构建微信移动端自适应 HTML 估值推送报文（含内嵌四色条形图 + 高清图片 + 彩色明细表）。"""
    df_sorted = df.sort_values("pe_percentile", ascending=True).reset_index(drop=True)
    report_date = df_sorted["latest_date"].max() if not df_sorted.empty else dt.date.today().strftime("%Y-%m-%d")

    cnt_low = int((df_sorted["pe_percentile"] < 30).sum())
    cnt_mid_low = int(((df_sorted["pe_percentile"] >= 30) & (df_sorted["pe_percentile"] < 50)).sum())
    cnt_mid_high = int(((df_sorted["pe_percentile"] >= 50) & (df_sorted["pe_percentile"] <= 70)).sum())
    cnt_high = int((df_sorted["pe_percentile"] > 70).sum())

    bar_rows_html = []
    for _, r in df_sorted.iterrows():
        pct = max(2.0, min(100.0, float(r["pe_percentile"])))
        bar_rows_html.append(f"""
        <div style="margin-bottom:12px;">
          <div style="display:flex;justify-content:space-between;align-items:center;font-size:13px;margin-bottom:4px;">
            <span style="font-weight:700;color:#0F172A;">{r['name']} <span style="font-weight:400;color:#64748B;font-size:11px;">({r['display_code']})</span></span>
            <span style="font-weight:700;color:{r['text_color']};background:{r['bg_color']};padding:1px 7px;border-radius:10px;font-size:11px;">
              PE {r['current_pe']:.2f} · 百分位 {r['pe_percentile']:.1f}% ({r['tier_label']})
            </span>
          </div>
          <div style="position:relative;height:12px;background:linear-gradient(90deg, #ECFDF5 0%, #ECFDF5 30%, #EFF6FF 30%, #EFF6FF 50%, #FFFBEB 50%, #FFFBEB 70%, #FEF2F2 70%, #FEF2F2 100%);border-radius:6px;overflow:hidden;border:1px solid #E2E8F0;">
            <div style="position:absolute;left:30%;top:0;bottom:0;width:1px;background:#94A3B8;opacity:0.6;"></div>
            <div style="position:absolute;left:50%;top:0;bottom:0;width:1px;background:#94A3B8;opacity:0.6;"></div>
            <div style="position:absolute;left:70%;top:0;bottom:0;width:1px;background:#94A3B8;opacity:0.6;"></div>
            <div style="height:100%;width:{pct:.1f}%;background:{r['color']};border-radius:6px;"></div>
          </div>
        </div>
        """)

    table_rows_html = []
    for idx, r in df_sorted.iterrows():
        row_bg = "#FFFFFF" if idx % 2 == 0 else "#F8FAFC"
        table_rows_html.append(f"""
        <tr style="background:{row_bg};border-bottom:1px solid #E2E8F0;">
          <td style="padding:8px 6px;font-weight:600;color:#0F172A;font-size:12px;">
            {r['name']}<br/><span style="font-size:10px;color:#64748B;font-weight:400;">{r['display_code']}</span>
          </td>
          <td style="padding:8px 6px;text-align:right;font-weight:700;color:#0F172A;font-size:12px;">{r['current_pe']:.2f}</td>
          <td style="padding:8px 6px;text-align:right;font-weight:700;color:{r['text_color']};font-size:12px;">{r['pe_percentile']:.1f}%</td>
          <td style="padding:8px 6px;text-align:center;">
            <span style="display:inline-block;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:700;background:{r['bg_color']};color:{r['text_color']};">
              {r['tier_label']}
            </span>
          </td>
          <td style="padding:8px 6px;text-align:right;color:#475569;font-size:11px;">
            {r['pe_min']:.1f} / {r['pe_median']:.1f} / {r['pe_max']:.1f}
          </td>
        </tr>
        """)

    img_block = ""
    if chart_image_url:
        img_block = f"""
        <div style="margin:14px 0;background:#FFFFFF;border-radius:10px;padding:10px;border:1px solid #E2E8F0;">
          <div style="font-size:13px;font-weight:700;color:#0F172A;margin-bottom:8px;">📊 5年PE估值全景图表（点击可放大）</div>
          <img src="{chart_image_url}" alt="每日指数估值看板" style="width:100%;height:auto;border-radius:6px;display:block;" />
        </div>
        """

    html = f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'PingFang SC','Microsoft YaHei',sans-serif;background:#F1F5F9;padding:12px;color:#0F172A;line-height:1.5;">
      <div style="background:linear-gradient(135deg,#0F172A 0%,#1E3A8A 100%);color:#FFFFFF;padding:16px;border-radius:12px;margin-bottom:12px;">
        <div style="font-size:17px;font-weight:700;margin-bottom:4px;">📈 每日指数估值晚报 (5年PE百分位)</div>
        <div style="font-size:12px;color:#CBD5E1;">数据日期：{report_date} · 共监控 {len(df_sorted)} 只海内外指数</div>
        <div style="display:flex;gap:6px;margin-top:12px;text-align:center;">
          <div style="flex:1;background:rgba(16,185,129,0.18);border:1px solid #10B981;border-radius:8px;padding:6px 2px;">
            <div style="font-size:15px;font-weight:700;color:#34D399;">{cnt_low}</div>
            <div style="font-size:10px;color:#D1FAE5;">低估 &lt;30%</div>
          </div>
          <div style="flex:1;background:rgba(59,130,246,0.18);border:1px solid #3B82F6;border-radius:8px;padding:6px 2px;">
            <div style="font-size:15px;font-weight:700;color:#60A5FA;">{cnt_mid_low}</div>
            <div style="font-size:10px;color:#DBEAFE;">适中偏低 30-50%</div>
          </div>
          <div style="flex:1;background:rgba(245,158,11,0.18);border:1px solid #F59E0B;border-radius:8px;padding:6px 2px;">
            <div style="font-size:15px;font-weight:700;color:#FBBF24;">{cnt_mid_high}</div>
            <div style="font-size:10px;color:#FEF3C7;">适中偏高 50-70%</div>
          </div>
          <div style="flex:1;background:rgba(239,68,68,0.18);border:1px solid #EF4444;border-radius:8px;padding:6px 2px;">
            <div style="font-size:15px;font-weight:700;color:#F87171;">{cnt_high}</div>
            <div style="font-size:10px;color:#FEE2E2;">高估 &gt;70%</div>
          </div>
        </div>
      </div>

      {img_block}

      <div style="background:#FFFFFF;border-radius:10px;padding:14px;margin-bottom:12px;border:1px solid #E2E8F0;">
        <div style="font-size:14px;font-weight:700;color:#0F172A;margin-bottom:4px;">🎯 近5年PE历史百分位分布图</div>
        <div style="font-size:11px;color:#64748B;margin-bottom:12px;">
          竖线刻度依次为 <span style="color:#10B981;font-weight:600;">30%低估线</span> / <span style="color:#3B82F6;font-weight:600;">50%中位线</span> / <span style="color:#EF4444;font-weight:600;">70%高估线</span>
        </div>
        {''.join(bar_rows_html)}
      </div>

      <div style="background:#FFFFFF;border-radius:10px;padding:12px;border:1px solid #E2E8F0;overflow-x:auto;">
        <div style="font-size:14px;font-weight:700;color:#0F172A;margin-bottom:8px;">📋 指数估值数据明细表</div>
        <table style="width:100%;border-collapse:collapse;text-align:left;">
          <thead>
            <tr style="background:#1E293B;color:#FFFFFF;font-size:11px;">
              <th style="padding:7px 6px;border-radius:4px 0 0 0;">指数名称</th>
              <th style="padding:7px 6px;text-align:right;">当前PE</th>
              <th style="padding:7px 6px;text-align:right;">5年分位</th>
              <th style="padding:7px 6px;text-align:center;">估值状态</th>
              <th style="padding:7px 6px;text-align:right;border-radius:0 4px 0 0;">5年最低/中位/最高</th>
            </tr>
          </thead>
          <tbody>
            {''.join(table_rows_html)}
          </tbody>
        </table>
      </div>
    </div>
    """
    return html.strip()


def send_via_pushplus(token: str, title: str, html_content: str, topic: str = "") -> bool:
    """通过 PushPlus (推送加) 将 HTML 图表与数据报表推送到个人微信。"""
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


def send_via_wecom_webhook(webhook_key: str, png_path: pathlib.Path) -> bool:
    """可选：若配置了企业微信群机器人 Webhook Key，直接向微信发送高清图表图片。"""
    base_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={webhook_key}"
    try:
        img_bytes = png_path.read_bytes()
        b64_str = base64.b64encode(img_bytes).decode("utf-8")
        md5_str = hashlib.md5(img_bytes).hexdigest()
        img_payload = {
            "msgtype": "image",
            "image": {"base64": b64_str, "md5": md5_str},
        }
        requests.post(base_url, json=img_payload, timeout=15)
        print("[推送成功] 企业微信机器人图片消息已发送。")
        return True
    except Exception as e:
        print(f"[推送异常] 企业微信机器人发送失败: {e}")
        return False


# ============================================================================
# 7. 主程序入口
# ============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="每日指数估值微信推送脚本")
    parser.add_argument(
        "--config",
        default="indices.json",
        help="指数配置文件路径（默认 indices.json，也兼容传入 indices.xlsx）",
    )
    parser.add_argument("--excel", default="", help="兼容旧版参数：Excel 配置文件路径")
    parser.add_argument("--output-png", default="valuation_chart.png", help="输出的估值看板图片路径")
    parser.add_argument("--output-html", default="valuation_report.html", help="输出的微信 HTML 预览文件路径")
    parser.add_argument("--title-file", default=".valuation_title.txt", help="标题缓存文件路径")
    parser.add_argument("--demo", action="store_true", help="使用内置演示数据生成图表与报表预览")
    parser.add_argument("--skip-push", action="store_true", help="仅生成图表和 HTML 文件，暂不执行微信推送")
    parser.add_argument("--send-only", action="store_true", help="直接读取已生成的 HTML 与图表执行微信推送")
    args = parser.parse_args()

    config_file = args.excel if args.excel else args.config
    if not pathlib.Path(config_file).exists() and pathlib.Path("indices.xlsx").exists():
        config_file = "indices.xlsx"

    png_path = pathlib.Path(args.output_png)
    html_path = pathlib.Path(args.output_html)
    title_path = pathlib.Path(args.title_file)

    if not args.send_only:
        indices = load_indices_config(config_file)
        print(f"[读取配置] 从 {config_file} 成功读取 {len(indices)} 只有效股票指数。")

        records = []
        for item in indices:
            res = compute_index_valuation(item, demo_mode=args.demo)
            if res is not None:
                records.append(res)

        if not records:
            print("[错误] 未能获取到任何指数的估值数据，程序终止。")
            sys.exit(1)

        df_val = pd.DataFrame(records)

        render_valuation_dashboard(df_val, png_path)

        repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
        chart_url = os.environ.get("CHART_IMAGE_URL", "").strip()
        if not chart_url and repo:
            chart_url = f"https://fastly.jsdelivr.net/gh/{repo}@output-chart/{png_path.name}?t={int(dt.datetime.now().timestamp())}"

        html_content = build_wechat_html(df_val, chart_image_url=chart_url)
        html_path.write_text(html_content, encoding="utf-8")

        report_date = df_val["latest_date"].max()
        low_cnt = int((df_val["pe_percentile"] < 30).sum())
        title = f"📊 指数估值晚报 ({report_date}) | 低估 {low_cnt} 只"
        title_path.write_text(title, encoding="utf-8")
        print(f"[HTML报表] 已生成微信 HTML 报表: {html_path}")
    else:
        if not html_path.exists():
            print(f"[错误] 未找到已生成的 HTML 报表文件: {html_path}")
            sys.exit(1)
        html_content = html_path.read_text(encoding="utf-8")
        title = title_path.read_text(encoding="utf-8").strip() if title_path.exists() else "📊 每日指数估值晚报"

    if args.skip_push:
        print("[完成] 已按 --skip-push 模式生成图表与 HTML 报表。")
        return

    pushplus_token = os.environ.get("PUSHPLUS_TOKEN", "").strip()
    pushplus_topic = os.environ.get("PUSHPLUS_TOPIC", "").strip()
    wecom_key = os.environ.get("WECOM_WEBHOOK_KEY", "").strip()

    if pushplus_token:
        send_via_pushplus(pushplus_token, title, html_content, topic=pushplus_topic)
    else:
        print("[提示] 未检测到环境变量 PUSHPLUS_TOKEN，已跳过 PushPlus 微信推送（本地文件已生成）。")

    if wecom_key and png_path.exists():
        send_via_wecom_webhook(wecom_key, png_path)


if __name__ == "__main__":
    main()
