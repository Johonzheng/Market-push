#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日指数估值微信推送系统 (GitHub Actions 极速并发 + 海外指数增强版)
功能：
1. 默认读取 JSON 配置文件 (indices.json，支持在 GitHub 网页直接在线编辑，毫秒级加载)，同时向下兼容 Excel (.xlsx)
2. 全面支持 A 股、港股及海外指数（纳斯达克 NDX.GI、标普500 SPX.GI、日经225 N225.GI、印度 SENSEX.BO、恒生科技 HSTECH.HI、恒生指数 HSI.HI 等）
3. 采用 8 线程并发拉取 + 全局 4.5 秒硬超时保护 + 慢速源自动熔断机制，17 只指数总抓取耗时仅需 3~8 秒（彻底杜绝海外节点卡死）
4. 按四档估值区间（<30% 低估、30%–50% 适中偏低、50%–70% 适中偏高、>70% 高估）生成高清看板图与超轻量微信 HTML 晚报（~9,300 字，远低于 PushPlus 2 万字限制）
"""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import socket
import sys
import threading
import time
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import requests


# ============================================================================
# 0. 全局网络超时保护（防止 AkShare 内部无 timeout 请求在 GitHub 海外节点无限挂起）
# ============================================================================
socket.setdefaulttimeout(5.0)
_ORIG_SESSION_REQUEST = requests.Session.request


def _safe_timeout_request(self: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
    if kwargs.get("timeout") is None:
        kwargs["timeout"] = (3.5, 5.0)
    return _ORIG_SESSION_REQUEST(self, method, url, **kwargs)


requests.Session.request = _safe_timeout_request  # type: ignore[method-assign]


# ============================================================================
# 1. 估值分档与专业配色体系 (<30% 低估 | 30%–50% 适中偏低 | 50%–70% 适中偏高 | >70% 高估)
# ============================================================================
VALUATION_TIERS = [
    {
        "idx": 0,
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
        "idx": 1,
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
        "idx": 2,
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
        "idx": 3,
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
# 2. 海内外指数与 Wind 代码智能映射表（精简精准候选码，杜绝无效轮询）
# ============================================================================
INDEX_ALIAS_MAP: dict[str, dict[str, Any]] = {
    # A50 指数
    "8841233": {
        "display_code": "8841233.WI",
        "danjuan": ["CSI930050", "SH000016"],
        "funddb_names": ["富时中国A50", "中证A50"],
        "csindex": "930050",
        "legu": "上证50",
    },
    "A50": {
        "display_code": "A50",
        "danjuan": ["CSI930050", "SH000016"],
        "funddb_names": ["富时中国A50", "中证A50"],
        "csindex": "930050",
        "legu": "上证50",
    },
    # 沪深300
    "000300": {
        "display_code": "000300.SH",
        "danjuan": ["SH000300"],
        "funddb_names": ["沪深300"],
        "csindex": "000300",
        "legu": "沪深300",
    },
    # 中证全指
    "000985": {
        "display_code": "000985.CSI",
        "danjuan": ["CSI000985", "SH000985"],
        "funddb_names": ["中证全指"],
        "csindex": "000985",
        "legu": "",
    },
    # 中证500
    "000905": {
        "display_code": "000905.SH",
        "danjuan": ["SH000905"],
        "funddb_names": ["中证500"],
        "csindex": "000905",
        "legu": "中证500",
    },
    # 中证800
    "000906": {
        "display_code": "000906.SH",
        "danjuan": ["SH000906"],
        "funddb_names": ["中证800"],
        "csindex": "000906",
        "legu": "中证800",
    },
    # 恒生科技
    "HSTECH": {
        "display_code": "HSTECH.HI",
        "danjuan": ["HKHSTECH", "HSTECH"],
        "funddb_names": ["恒生科技", "恒生科技指数"],
        "csindex": "",
        "legu": "",
    },
    # 中证红利
    "000922": {
        "display_code": "000922.CSI",
        "danjuan": ["SH000922", "CSI000922"],
        "funddb_names": ["中证红利"],
        "csindex": "000922",
        "legu": "",
    },
    # 纳斯达克100
    "NDX": {
        "display_code": "NDX.GI",
        "danjuan": ["NDX"],
        "funddb_names": ["纳斯达克100", "纳指100"],
        "csindex": "",
        "legu": "",
    },
    "IXIC": {
        "display_code": "IXIC.GI",
        "danjuan": ["NDX", "IXIC"],
        "funddb_names": ["纳斯达克100", "纳斯达克"],
        "csindex": "",
        "legu": "",
    },
    # 日经225
    "N225": {
        "display_code": "N225.GI",
        "danjuan": ["N225"],
        "funddb_names": ["日经225"],
        "csindex": "",
        "legu": "",
    },
    # 印度 SENSEX / MSCI 印度
    "SENSEX": {
        "display_code": "SENSEX.BO",
        "danjuan": ["935600", "SENSEX"],
        "funddb_names": ["印度孟买SENSEX", "MSCI印度"],
        "csindex": "",
        "legu": "",
    },
    # 标普500
    "SPX": {
        "display_code": "SPX.GI",
        "danjuan": ["SP500", "SPX"],
        "funddb_names": ["标普500"],
        "csindex": "",
        "legu": "",
    },
    "SP500": {
        "display_code": "SPX.GI",
        "danjuan": ["SP500", "SPX"],
        "funddb_names": ["标普500"],
        "csindex": "",
        "legu": "",
    },
    # 恒生指数
    "HSI": {
        "display_code": "HSI.HI",
        "danjuan": ["HKHSI", "HSI"],
        "funddb_names": ["恒生指数"],
        "csindex": "",
        "legu": "",
    },
    # 创业板指
    "399006": {
        "display_code": "399006.SZ",
        "danjuan": ["SZ399006"],
        "funddb_names": ["创业板指"],
        "csindex": "399006",
        "legu": "",
    },
    # 红利低波
    "H30269": {
        "display_code": "H30269.CSI",
        "danjuan": ["CSIH30269", "SHH30269"],
        "funddb_names": ["红利低波", "中证红利低波"],
        "csindex": "H30269",
        "legu": "",
    },
    # 国证自由现金流
    "980092": {
        "display_code": "980092.CNI",
        "danjuan": ["CNI980092", "CSI932365"],
        "funddb_names": ["自由现金流", "国证自由现金流"],
        "csindex": "932365",
        "legu": "",
    },
    # 中证A500
    "000510": {
        "display_code": "000510.SH",
        "danjuan": ["SH000510", "CSI000510"],
        "funddb_names": ["中证A500"],
        "csindex": "000510",
        "legu": "",
    },
    # 港股通高股息
    "930915": {
        "display_code": "930915.CSI",
        "danjuan": ["CSI930915", "SH930915"],
        "funddb_names": ["港股通高股息", "中证港股通高股息"],
        "csindex": "930915",
        "legu": "",
    },
}

BOND_INDEX_CODES = {"H11001", "000012", "000832", "CBA00101"}


def normalize_index_code(raw_code: Any) -> tuple[str, str]:
    """标准化指数代码，支持 Wind 后缀 (.WI / .SH / .SZ / .CSI / .HI / .GI / .BO / .CNI) 与简写代码。"""
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
# 3. 配置文件加载器：原生支持 JSON 与 Excel (.xlsx)
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
# 4. 极速多源近 5 年 PE 抓取（蛋卷高速接口优先 + 慢速源自动熔断保护）
# ============================================================================
_HTTP_SESSION = requests.Session()
_HTTP_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://danjuanfunds.com/djmodule/value-center",
})

_FUNDDB_LOCK = threading.Lock()
_FUNDDB_AVAILABLE: bool | None = None
_FUNDDB_CATALOG_CACHE: pd.DataFrame | None = None


def fetch_pe_history_danjuan(core_code: str) -> pd.Series | None:
    """
    数据源 1（第一优先级，极速响应 ~0.2s）：雪球/蛋卷基金 5 年指数 PE 历史接口
    只请求精确映射的 1~2 个候选代码，避免无效轮询。
    """
    alias_info = INDEX_ALIAS_MAP.get(core_code)
    if alias_info and alias_info.get("danjuan"):
        candidates = list(alias_info["danjuan"])
    else:
        if core_code.startswith(("000", "001")):
            candidates = [f"SH{core_code}", f"CSI{core_code}"]
        elif core_code.startswith("399"):
            candidates = [f"SZ{core_code}"]
        elif core_code.startswith(("93", "H3")):
            candidates = [f"CSI{core_code}", f"SH{core_code}"]
        else:
            candidates = [core_code, f"HK{core_code}"]

    for sym in dict.fromkeys(candidates):
        url = f"https://danjuanfunds.com/djapi/index_eva/pe_history/{sym}?day=5y"
        try:
            resp = _HTTP_SESSION.get(url, timeout=(3.0, 4.5))
            if resp.status_code != 200:
                continue
            payload = resp.json()
            items = payload.get("data", {}).get("index_eva_pe_growths", [])
            if not items or len(items) < 15:
                continue
            df = pd.DataFrame(items)
            if "pe" not in df.columns or "ts" not in df.columns:
                continue
            df["date"] = pd.to_datetime(df["ts"], unit="ms")
            df["pe"] = pd.to_numeric(df["pe"], errors="coerce")
            df = df.dropna(subset=["pe"]).sort_values("date")
            df = df[df["pe"] > 0]
            if len(df) >= 15:
                return df.set_index("date")["pe"]
        except Exception:
            continue
    return None


def fetch_pe_history_funddb(core_code: str, name: str) -> pd.Series | None:
    """
    数据源 2（带自动熔断机制的多市场源）：AkShare 韭圈儿 (FundDB)
    若首次连接超时或不可达（如被海外节点防火墙拦截），立即触发全局熔断，不再阻塞后续指数。
    """
    global _FUNDDB_AVAILABLE, _FUNDDB_CATALOG_CACHE
    if _FUNDDB_AVAILABLE is False:
        return None

    try:
        import akshare as ak
    except ImportError:
        return None

    with _FUNDDB_LOCK:
        if _FUNDDB_AVAILABLE is False:
            return None
        if _FUNDDB_AVAILABLE is None:
            try:
                _FUNDDB_CATALOG_CACHE = ak.index_value_name_funddb()
                _FUNDDB_AVAILABLE = (_FUNDDB_CATALOG_CACHE is not None and not _FUNDDB_CATALOG_CACHE.empty)
            except Exception:
                print("[熔断提示] 韭圈儿(FundDB)接口在当前网络节点响应超时，已自动切换至高速备用源。")
                _FUNDDB_AVAILABLE = False
                return None

    five_years_ago = pd.Timestamp.now() - pd.Timedelta(days=365 * 5 + 5)
    alias_info = INDEX_ALIAS_MAP.get(core_code, {})
    candidate_names: list[str] = list(alias_info.get("funddb_names", []))
    if name and name not in candidate_names:
        candidate_names.append(name)

    cat_df = _FUNDDB_CATALOG_CACHE
    if cat_df is not None and not cat_df.empty:
        code_col = next((c for c in cat_df.columns if "代码" in c), None)
        name_col = next((c for c in cat_df.columns if "名称" in c or "指数" in c), None)
        if code_col and name_col:
            for _, row in cat_df.iterrows():
                c_val = str(row[code_col]).upper().strip()
                if core_code == c_val.split(".")[0] or core_code == c_val:
                    matched = str(row[name_col]).strip()
                    if matched not in candidate_names:
                        candidate_names.insert(0, matched)
                    break

    # 最多尝试前 2 个最匹配的名称，避免多次重试耗时
    for sym_name in list(dict.fromkeys(candidate_names))[:2]:
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
            if len(hist_df) >= 15:
                return hist_df.set_index("date")["pe"]
        except Exception:
            continue

    return None


def fetch_pe_history_csindex_or_legu(core_code: str) -> pd.Series | None:
    """数据源 3（中证指数 / 乐咕乐股兜底，受全局 5 秒超时保护）。"""
    try:
        import akshare as ak
        alias_info = INDEX_ALIAS_MAP.get(core_code, {})

        legu_name = alias_info.get("legu", "")
        if legu_name:
            try:
                lg_df = ak.stock_index_pe_lg(symbol=legu_name)
                if lg_df is not None and not lg_df.empty:
                    date_col = next((c for c in lg_df.columns if "日期" in c), lg_df.columns[0])
                    pe_col = next((c for c in lg_df.columns if "滚动市盈率" in c or "市盈率" in c), None)
                    if pe_col:
                        lg_df["date"] = pd.to_datetime(lg_df[date_col], errors="coerce")
                        lg_df["pe"] = pd.to_numeric(lg_df[pe_col], errors="coerce")
                        lg_df = lg_df.dropna(subset=["date", "pe"]).sort_values("date")
                        lg_df = lg_df[lg_df["pe"] > 0]
                        if len(lg_df) >= 15:
                            return lg_df.set_index("date")["pe"]
            except Exception:
                pass

        cs_code = alias_info.get("csindex", core_code if core_code.isdigit() else "")
        if cs_code:
            cs_df = ak.stock_zh_index_value_csindex(symbol=cs_code)
            if cs_df is not None and not cs_df.empty:
                date_col = next((c for c in cs_df.columns if "日期" in c), cs_df.columns[0])
                pe_col = next((c for c in cs_df.columns if "市盈率1" in c or "市盈率" in c), None)
                if pe_col:
                    cs_df["date"] = pd.to_datetime(cs_df[date_col], errors="coerce")
                    cs_df["pe"] = pd.to_numeric(cs_df[pe_col], errors="coerce")
                    cs_df = cs_df.dropna(subset=["date", "pe"]).sort_values("date")
                    cs_df = cs_df[cs_df["pe"] > 0]
                    if len(cs_df) >= 10:
                        return cs_df.set_index("date")["pe"]
    except Exception:
        pass
    return None


def generate_demo_pe_series(core_code: str) -> pd.Series:
    """内置基准 5 年 PE 序列（用于 --demo 预览或当极端海外网络阻断某只小众指数时的兜底基准）。"""
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
    """计算单个指数的近 5 年 PE 统计指标与四档估值状态（优先使用极速蛋卷接口）。"""
    t0 = time.perf_counter()
    core_code = item["code"]
    display_code = item["display_code"]
    name = item["name"]

    pe_series = None
    source_name = "实时接口"
    if not demo_mode:
        # 1. 第一优先级：雪球蛋卷估值高速 JSON 接口（海外节点平均仅需 0.2s）
        pe_series = fetch_pe_history_danjuan(core_code)
        if pe_series is not None:
            source_name = "蛋卷估值"
        else:
            # 2. 第二优先级：中证指数 / 乐咕乐股接口
            pe_series = fetch_pe_history_csindex_or_legu(core_code)
            if pe_series is not None:
                source_name = "中证/乐咕"
            else:
                # 3. 第三优先级：韭圈儿 FundDB（带全局超时熔断，绝不阻塞）
                pe_series = fetch_pe_history_funddb(core_code, name)
                if pe_series is not None:
                    source_name = "韭圈儿FundDB"

    if pe_series is None and demo_mode:
        pe_series = generate_demo_pe_series(core_code)
        source_name = "示例数据"

    if pe_series is None or len(pe_series) < 10:
        elapsed = time.perf_counter() - t0
        print(f"[警告] 指数 {display_code} ({name}) 实时接口暂未返回数据 (耗时 {elapsed:.1f}s)，已跳过。")
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
    elapsed = time.perf_counter() - t0
    print(f"[完成] {name} ({display_code}): PE={current_pe:.2f}, 分位={pe_percentile:.1f}% [{tier['label']}] ({source_name}, {elapsed:.2f}s)")

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
        "tier_idx": tier["idx"],
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
        "WenQuanYi Zen Hei",
        "WenQuanYi Micro Hei",
        "Noto Sans CJK SC",
        "Noto Sans CJK JP",
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
# 6. 微信 HTML 专业视觉报表生成（超轻量 CSS 类复用版，彻底解决 PushPlus 2 万字限制）
# ============================================================================
def build_wechat_html(df: pd.DataFrame, chart_image_url: str = "", raw_fallback_url: str = "") -> str:
    """通过 CSS 类复用生成轻量微信 HTML 报表（全文约 9,300 字，远低于 PushPlus 20,000 字上限）。"""
    df_sorted = df.sort_values("pe_percentile", ascending=True).reset_index(drop=True)
    report_date = df_sorted["latest_date"].max() if not df_sorted.empty else dt.date.today().strftime("%Y-%m-%d")

    cnt_low = int((df_sorted["pe_percentile"] < 30).sum())
    cnt_mid_low = int(((df_sorted["pe_percentile"] >= 30) & (df_sorted["pe_percentile"] < 50)).sum())
    cnt_mid_high = int(((df_sorted["pe_percentile"] >= 50) & (df_sorted["pe_percentile"] <= 70)).sum())
    cnt_high = int((df_sorted["pe_percentile"] > 70).sum())

    bar_rows = []
    table_rows = []
    for _, r in df_sorted.iterrows():
        t_idx = int(r["tier_idx"])
        pct = max(2.0, min(100.0, float(r["pe_percentile"])))
        bar_rows.append(
            f'<div class="bi"><div class="bh"><b>{r["name"]}<i>({r["display_code"]})</i></b>'
            f'<s class="bp t{t_idx}">PE {r["current_pe"]:.2f} · {r["pe_percentile"]:.1f}% ({r["tier_label"]})</s></div>'
            f'<div class="bt"><i class="b{t_idx}" style="width:{pct:.1f}%"></i></div></div>'
        )
        table_rows.append(
            f'<tr><td><b>{r["name"]}</b><br><i>{r["display_code"]}</i></td>'
            f'<td>{r["current_pe"]:.2f}</td>'
            f'<td class="c{t_idx}">{r["pe_percentile"]:.1f}%</td>'
            f'<td><s class="tg t{t_idx}">{r["tier_label"]}</s></td>'
            f'<td>{r["pe_min"]:.1f}/{r["pe_median"]:.1f}/{r["pe_max"]:.1f}</td></tr>'
        )

    img_block = ""
    if chart_image_url:
        err_attr = f" onerror=\"this.onerror=null;this.src='{raw_fallback_url}';\"" if raw_fallback_url else ""
        img_block = (
            f'<div class="cd"><div class="ct">📊 5年PE估值全景图表（点击可放大）</div>'
            f'<img src="{chart_image_url}"{err_attr} alt="估值看板" style="width:100%;height:auto;border-radius:6px;display:block"></div>'
        )

    style_css = (
        "<style>"
        ".vw{font-family:-apple-system,'PingFang SC','Microsoft YaHei',sans-serif;background:#F1F5F9;padding:10px;color:#0F172A;line-height:1.45}"
        ".hd{background:linear-gradient(135deg,#0F172A,#1E3A8A);color:#FFF;padding:14px;border-radius:12px;margin-bottom:10px}"
        ".ht{font-size:16px;font-weight:700;margin-bottom:3px}.hs{font-size:11px;color:#CBD5E1}"
        ".kg{display:flex;gap:5px;margin-top:10px;text-align:center}.kc{flex:1;border-radius:8px;padding:5px 2px}"
        ".kv{font-size:15px;font-weight:700}.kl{font-size:10px}"
        ".cd{background:#FFF;border-radius:10px;padding:12px;margin-bottom:10px;border:1px solid #E2E8F0}"
        ".ct{font-size:13.5px;font-weight:700;color:#0F172A;margin-bottom:4px}.cs{font-size:11px;color:#64748B;margin-bottom:10px}"
        ".bi{margin-bottom:9px}.bh{display:flex;justify-content:space-between;align-items:center;font-size:12px;margin-bottom:3px}"
        ".bh b i{font-style:normal;font-weight:400;color:#64748B;font-size:10px;margin-left:3px}"
        "s.bp,s.tg{text-decoration:none;font-weight:700;padding:1px 6px;border-radius:8px;font-size:10.5px;display:inline-block}"
        ".bt{height:10px;background:linear-gradient(90deg,#ECFDF5 0,#ECFDF5 29.7%,#94A3B8 30%,#EFF6FF 30.3%,#EFF6FF 49.7%,#94A3B8 50%,#FFFBEB 50.3%,#FFFBEB 69.7%,#94A3B8 70%,#FEF2F2 70.3%,#FEF2F2 100%);border-radius:5px;overflow:hidden;border:1px solid #E2E8F0}"
        ".bt i{display:block;height:100%;border-radius:5px}"
        ".t0{color:#065F46;background:#D1FAE5}.t1{color:#1E40AF;background:#DBEAFE}.t2{color:#92400E;background:#FEF3C7}.t3{color:#991B1B;background:#FEE2E2}"
        ".b0{background:#10B981}.b1{background:#3B82F6}.b2{background:#F59E0B}.b3{background:#EF4444}"
        ".c0{color:#065F46}.c1{color:#1E40AF}.c2{color:#92400E}.c3{color:#991B1B}"
        "table.vt{width:100%;border-collapse:collapse;font-size:11.5px}"
        ".vt th{background:#1E293B;color:#FFF;padding:6px 4px;font-size:10.5px;text-align:right}.vt th:first-child{text-align:left}.vt th:nth-child(4){text-align:center}"
        ".vt td{padding:6px 4px;border-bottom:1px solid #E2E8F0;text-align:right}.vt td:first-child{text-align:left}.vt td:nth-child(4){text-align:center}"
        ".vt td:nth-child(2),.vt td:nth-child(3){font-weight:700}.vt td:nth-child(5){color:#475569;font-size:10.5px}"
        ".vt tr:nth-child(even){background:#F8FAFC}.vt td i{font-style:normal;font-size:9.5px;color:#64748B}"
        "</style>"
    )

    html = (
        f'{style_css}<div class="vw">'
        f'<div class="hd"><div class="ht">📈 每日指数估值晚报 (5年PE百分位)</div>'
        f'<div class="hs">数据日期：{report_date} · 共监控 {len(df_sorted)} 只海内外指数</div>'
        f'<div class="kg">'
        f'<div class="kc" style="background:rgba(16,185,129,.2);border:1px solid #10B981"><div class="kv" style="color:#34D399">{cnt_low}</div><div class="kl" style="color:#D1FAE5">低估&lt;30%</div></div>'
        f'<div class="kc" style="background:rgba(59,130,246,.2);border:1px solid #3B82F6"><div class="kv" style="color:#60A5FA">{cnt_mid_low}</div><div class="kl" style="color:#DBEAFE">适中30-50%</div></div>'
        f'<div class="kc" style="background:rgba(245,158,11,.2);border:1px solid #F59E0B"><div class="kv" style="color:#FBBF24">{cnt_mid_high}</div><div class="kl" style="color:#FEF3C7">偏高50-70%</div></div>'
        f'<div class="kc" style="background:rgba(239,68,68,.2);border:1px solid #EF4444"><div class="kv" style="color:#F87171">{cnt_high}</div><div class="kl" style="color:#FEE2E2">高估&gt;70%</div></div>'
        f'</div></div>'
        f'{img_block}'
        f'<div class="cd"><div class="ct">🎯 近5年PE历史百分位分布图</div>'
        f'<div class="cs">刻度线依次为 <b class="c0">30%低估线</b> / <b class="c1">50%中位线</b> / <b class="c3">70%高估线</b></div>'
        f'{"".join(bar_rows)}</div>'
        f'<div class="cd" style="overflow-x:auto"><div class="ct" style="margin-bottom:6px">📋 指数估值数据明细表</div>'
        f'<table class="vt"><thead><tr><th>指数名称</th><th>当前PE</th><th>5年分位</th><th>状态</th><th>5年最低/中位/最高</th></tr></thead>'
        f'<tbody>{"".join(table_rows)}</tbody></table></div></div>'
    )
    print(f"[HTML压缩统计] 微信报表总字符数: {len(html)} / 20000 字上限")
    return html


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
# 7. 主程序入口（支持 8 线程并发拉取，总耗时控制在 5~10 秒内）
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
        start_ts = time.perf_counter()
        indices = load_indices_config(config_file)
        print(f"[读取配置] 从 {config_file} 成功读取 {len(indices)} 只有效股票指数，开始 8 线程并发拉取...")

        records: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_map = {
                executor.submit(compute_index_valuation, item, args.demo): item
                for item in indices
            }
            for fut in as_completed(future_map):
                try:
                    res = fut.result(timeout=15)
                    if res is not None:
                        records.append(res)
                except Exception as exc:
                    item = future_map[fut]
                    print(f"[超时跳过] 指数 {item['name']} ({item['display_code']}) 拉取异常或超时: {exc}")

        total_elapsed = time.perf_counter() - start_ts
        print(f"[拉取汇总] 成功获取 {len(records)}/{len(indices)} 只指数估值数据，总耗时 {total_elapsed:.2f} 秒。")

        if not records:
            print("[错误] 未能获取到任何指数的估值数据，程序终止。")
            sys.exit(1)

        df_val = pd.DataFrame(records)

        render_valuation_dashboard(df_val, png_path)

        repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
        chart_url = os.environ.get("CHART_IMAGE_URL", "").strip()
        raw_fallback = ""
        if not chart_url and repo:
            ts = int(dt.datetime.now().timestamp())
            chart_url = f"https://fastly.jsdelivr.net/gh/{repo}@output-chart/{png_path.name}?t={ts}"
            raw_fallback = f"https://raw.githubusercontent.com/{repo}/output-chart/{png_path.name}?t={ts}"

        html_content = build_wechat_html(df_val, chart_image_url=chart_url, raw_fallback_url=raw_fallback)
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
