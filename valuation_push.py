#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日市场情绪与指数估值晚报微信推送系统 (蛋卷核心10只 + 万得Wind精准9只 统一近5年PE-TTM版)
核心修复（解决100行截断与海外代码问题）：
1. 修正 Wind 实盘代码：日本东证 TOPX.T、日经225 N225.NK、法国CAC40 FCHI.PA、印度SENSEX SENSEX.BO（展示仍为 BSE30.BO 防敏感词）
2. 彻底修复 Wind 历史序列被截断成近4个月(~100行)导致日经/A50变100%、A500变31%的 Bug：
   - 优先直取 Wind 官方“最新PE在过去5年分位数”显式列（一步拿准道指/CAC40/A500/全指/800/A50/印度7只真实5年日频分位）
   - 若官方分位列为空或异常为0（如日经225/日本东证），改拉近5年“每月末”PE-TTM序列（约60个点，永不触发100行截断）
   - 强制校验时间跨度 >= 1200 天（约3.3年以上），严禁把几个月的截断序列冒充5年分位
3. 顶部已移除“整体温度”，保留 沪深300 ERP + 全市场成交额/涨跌家数/主力净流入看板；明确标注 30%/50%/70% 为近5年分位
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
import subprocess
import sys
import time
from typing import Any

import numpy as np
import pandas as pd
import requests


# ============================================================================
# 0. 全局网络超时保护与阈值
# ============================================================================
socket.setdefaulttimeout(5.0)
_ORIG_SESSION_REQUEST = requests.Session.request

MAX_STALE_DAYS = 10
MIN_5Y_SERIES_POINTS = 24     # 月频至少24个月
MIN_5Y_SPAN_DAYS = 1200       # 强制时间跨度 >= 1200天，彻底杜绝近4个月100行截断假5年分位
PUSHPLUS_SAFE_CHAR_LIMIT = 18500


def _safe_timeout_request(self: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
    if kwargs.get("timeout") is None:
        kwargs["timeout"] = (2.5, 4.5)
    return _ORIG_SESSION_REQUEST(self, method, url, **kwargs)


requests.Session.request = _safe_timeout_request  # type: ignore[method-assign]


# ============================================================================
# 1. 四档估值区间与量化定投策略体系 (<30% 低估 | 30%–50% 适中偏低 | 50%–70% 适中偏高 | >70% 高估)
# ============================================================================
VALUATION_TIERS = [
    {"idx": 0, "key": "undervalued",  "label": "低估",     "range_text": "<30%",     "color": "#10B981"},
    {"idx": 1, "key": "normal_low",   "label": "适中偏低", "range_text": "30%–50%",  "color": "#3B82F6"},
    {"idx": 2, "key": "normal_high",  "label": "适中偏高", "range_text": "50%–70%",  "color": "#F59E0B"},
    {"idx": 3, "key": "overvalued",   "label": "高估",     "range_text": ">70%",     "color": "#EF4444"},
]


def classify_percentile(pct: float) -> dict[str, Any]:
    """根据 PE-TTM 近5年百分位返回对应的四档颜色与标签。"""
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
    """结合 PE-TTM近5年百分位 + PB百分位 + 股息率 给出定投倍率建议。"""
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
        if is_dividend_idx and dy >= 4.5:
            return "🟠高息观望"
        return "🚨分批止盈"


# ============================================================================
# 2. 海内外指数与 Wind 标准代码精准映射表
# ============================================================================
INDEX_ALIAS_MAP: dict[str, dict[str, Any]] = {
    # --- 10只已核验与 Wind 完全一致的蛋卷免费核心指数（0 Wind积分） ---
    "000300":  {"display_code": "000300.SH",  "wind_code": "000300.SH",  "danjuan": ["SH000300"],              "csindex": "000300", "style": "broad",    "prefer_wind": False},
    "000905":  {"display_code": "000905.SH",  "wind_code": "000905.SH",  "danjuan": ["SH000905"],              "csindex": "000905", "style": "broad",    "prefer_wind": False},
    "399006":  {"display_code": "399006.SZ",  "wind_code": "399006.SZ",  "danjuan": ["SZ399006"],              "csindex": "",       "style": "broad",    "prefer_wind": False},
    "000922":  {"display_code": "000922.CSI", "wind_code": "000922.CSI", "danjuan": ["SH000922", "CSI000922"], "csindex": "000922", "style": "dividend", "prefer_wind": False},
    "H30269":  {"display_code": "H30269.CSI", "wind_code": "H30269.CSI", "danjuan": ["CSIH30269"],             "csindex": "H30269", "style": "dividend", "prefer_wind": False},
    "HSI":     {"display_code": "HSI.HI",     "wind_code": "HSI.HI",     "danjuan": ["HKHSI", "HSI"],          "csindex": "",       "style": "overseas", "prefer_wind": False},
    "HSTECH":  {"display_code": "HSTECH.HI",  "wind_code": "HSTECH.HI",  "danjuan": ["HKHSTECH", "HSTECH"],    "csindex": "",       "style": "overseas", "prefer_wind": False},
    "NDX":     {"display_code": "NDX.GI",     "wind_code": "NDX.GI",     "danjuan": ["NDX"],                   "csindex": "",       "style": "overseas", "prefer_wind": False},
    "IXIC":    {"display_code": "IXIC.GI",    "wind_code": "IXIC.GI",    "danjuan": ["NDX", "IXIC"],           "csindex": "",       "style": "overseas", "prefer_wind": False},
    "SPX":     {"display_code": "SPX.GI",     "wind_code": "SPX.GI",     "danjuan": ["SP500", "SPX"],          "csindex": "",       "style": "overseas", "prefer_wind": False},
    "SP500":   {"display_code": "SPX.GI",     "wind_code": "SPX.GI",     "danjuan": ["SP500", "SPX"],          "csindex": "",       "style": "overseas", "prefer_wind": False},
    "GDAXI":   {"display_code": "GDAXI.GI",   "wind_code": "GDAXI.GI",   "danjuan": ["GDAXI", "DAX"],          "csindex": "",       "style": "overseas", "prefer_wind": False},
    "DAX":     {"display_code": "GDAXI.GI",   "wind_code": "GDAXI.GI",   "danjuan": ["GDAXI", "DAX"],          "csindex": "",       "style": "overseas", "prefer_wind": False},

    # --- 9只直连 Wind 精准代码（已按实盘核验修正 TOPX.T / N225.NK / FCHI.PA / SENSEX.BO） ---
    "8841233": {"display_code": "8841233.WI", "wind_code": "8841233.WI", "wind_name": "A50指数",        "danjuan": [], "csindex": "",       "style": "broad",    "prefer_wind": True},
    "A50":     {"display_code": "8841233.WI", "wind_code": "8841233.WI", "wind_name": "A50指数",        "danjuan": [], "csindex": "",       "style": "broad",    "prefer_wind": True},
    "000906":  {"display_code": "000906.SH",  "wind_code": "000906.SH",  "wind_name": "中证800",        "danjuan": [], "csindex": "000906", "style": "broad",    "prefer_wind": True},
    "000510":  {"display_code": "000510.SH",  "wind_code": "000510.SH",  "wind_name": "中证A500",       "danjuan": [], "csindex": "000510", "style": "broad",    "prefer_wind": True},
    "000985":  {"display_code": "000985.CSI", "wind_code": "000985.CSI", "wind_name": "中证全指",       "danjuan": [], "csindex": "000985", "style": "broad",    "prefer_wind": True},
    "SENSEX":  {"display_code": "BSE30.BO",   "wind_code": "SENSEX.BO",  "wind_name": "孟买SENSEX30",   "danjuan": [], "csindex": "",       "style": "overseas", "prefer_wind": True},
    "DJI":     {"display_code": "DJI.GI",     "wind_code": "DJI.GI",     "wind_name": "道琼斯工业平均", "danjuan": [], "csindex": "",       "style": "overseas", "prefer_wind": True},
    "N225":    {"display_code": "N225.NK",    "wind_code": "N225.NK",    "wind_name": "日经225",        "danjuan": [], "csindex": "",       "style": "overseas", "prefer_wind": True},
    "TOPX":    {"display_code": "TOPX.T",     "wind_code": "TOPX.T",     "wind_name": "东证指数",       "danjuan": [], "csindex": "",       "style": "overseas", "prefer_wind": True},
    "TOPIX":   {"display_code": "TOPX.T",     "wind_code": "TOPX.T",     "wind_name": "东证指数",       "danjuan": [], "csindex": "",       "style": "overseas", "prefer_wind": True},
    "FCHI":    {"display_code": "FCHI.PA",    "wind_code": "FCHI.PA",    "wind_name": "法国CAC40",      "danjuan": [], "csindex": "",       "style": "overseas", "prefer_wind": True},
    "CAC40":   {"display_code": "FCHI.PA",    "wind_code": "FCHI.PA",    "wind_name": "法国CAC40",      "danjuan": [], "csindex": "",       "style": "overseas", "prefer_wind": True},

    # 其他可选策略指数
    "980092":  {"display_code": "980092.CNI", "wind_code": "980092.CNI", "danjuan": ["CNI980092", "CSI932365"], "csindex": "932365", "style": "dividend", "prefer_wind": False},
    "930915":  {"display_code": "930915.CSI", "wind_code": "930915.CSI", "danjuan": ["CSI930915", "SH930915"],  "csindex": "930915", "style": "dividend", "prefer_wind": False},
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

    m_suf = re.match(r"^([A-Z0-9]+)\.(SH|SZ|CSI|HI|GI|BO|WI|CNI|HK|OF|US|T|NK|PA)$", s)
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
            wind_code = INDEX_ALIAS_MAP.get(core_code, {}).get("wind_code", str(raw_code).strip().upper())
            indices.append({
                "raw_code": str(raw_code).strip(),
                "code": core_code,
                "display_code": display_code,
                "wind_code": wind_code,
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
        wind_code = INDEX_ALIAS_MAP.get(core_code, {}).get("wind_code", code_str.upper())
        indices.append({
            "raw_code": code_str,
            "code": core_code,
            "display_code": display_code,
            "wind_code": wind_code,
            "name": clean_name,
        })

    return indices


# ============================================================================
# 4. Tier-1 数据源：雪球蛋卷 (已核验的 10 只核心指数)
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
            print(f"[批量快照] 已获取 {len(snapshot_map)} 只指数的蛋卷实时快照。")
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
    """仅当未配置 WIND_API_KEY 时的中证官网降级兜底。"""
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


# ============================================================================
# 5. Tier-2 数据源：万得 Wind Alice Market 官方 MCP CLI (wind-mcp-skill)
# ============================================================================
def _locate_wind_skill_dir() -> pathlib.Path | None:
    candidates = [
        os.environ.get("WIND_MCP_SKILL_DIR", "").strip(),
        "/tmp/AliceMarket/skills/wind-mcp-skill",
        str(pathlib.Path.home() / ".agents" / "skills" / "wind-mcp-skill"),
        str(pathlib.Path.cwd() / ".agents" / "skills" / "wind-mcp-skill"),
    ]
    for c in candidates:
        if not c:
            continue
        p = pathlib.Path(c)
        if (p / "scripts" / "cli.mjs").exists():
            return p
    return None


def _is_wind_enabled() -> bool:
    return bool(os.environ.get("WIND_API_KEY", "").strip()) and (_locate_wind_skill_dir() is not None)


def call_wind_mcp(
    server_type: str,
    tool_name: str,
    params: dict[str, Any],
    timeout_sec: int = 45,
) -> tuple[Any, str | None]:
    """通过 Wind 官方 wind-mcp-skill CLI 调用 MCP 工具。"""
    skill_dir = _locate_wind_skill_dir()
    if skill_dir is None:
        return None, "SKILL_NOT_FOUND"
    if not os.environ.get("WIND_API_KEY", "").strip():
        return None, "AUTH_MISSING"

    cmd = ["node", "scripts/cli.mjs", "call", server_type, tool_name, json.dumps(params, ensure_ascii=False)]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(skill_dir),
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout_sec,
            env=os.environ.copy(),
        )
    except Exception as exc:
        print(f"[Wind异常] 调用 {server_type}.{tool_name} 失败: {exc}")
        return None, "RUNTIME_ERROR"

    stdout = (proc.stdout or "").strip()
    if not stdout:
        stderr_short = (proc.stderr or "").strip()[:160]
        print(f"[Wind空回执] {server_type}.{tool_name} exit={proc.returncode} stderr={stderr_short}")
        return None, "EMPTY_RESPONSE"

    raw_obj: Any = None
    try:
        raw_obj = json.loads(stdout)
    except json.JSONDecodeError:
        m = re.search(r"(\{[\s\S]*\})", stdout)
        if m:
            try:
                raw_obj = json.loads(m.group(1))
            except json.JSONDecodeError:
                raw_obj = None

    if not isinstance(raw_obj, dict):
        return stdout, None

    if raw_obj.get("ok") is False:
        err_code = str(raw_obj.get("code") or "UNKNOWN")
        err_msg = str(raw_obj.get("message") or "")[:180]
        print(f"[Wind错误] {server_type}.{tool_name} -> {err_code}: {err_msg}")
        return None, err_code

    content = raw_obj.get("content")
    if isinstance(content, list) and content:
        first = content[0]
        text = first.get("text") if isinstance(first, dict) else str(first)
        if isinstance(text, str):
            text_s = text.strip()
            try:
                return json.loads(text_s), None
            except json.JSONDecodeError:
                return text_s, None
    return raw_obj, None


def _to_float(val: Any) -> float | None:
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        if np.isnan(val) or np.isinf(val):
            return None
        return float(val)
    s = str(val).strip()
    if not s or s in ("--", "-", "N/A", "null", "None", "未提供", "NaN"):
        return None
    s_clean = s.replace(",", "")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s_clean)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _parse_date_str(val: Any) -> str | None:
    """支持 YYYY-MM-DD、YYYY/MM/DD、YYYYMMDD 以及 10位/13位 Unix 时间戳。"""
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        v = float(val)
        if v > 1e12:
            try:
                return dt.datetime.fromtimestamp(v / 1000.0).strftime("%Y-%m-%d")
            except Exception:
                return None
        if v > 1e9:
            try:
                return dt.datetime.fromtimestamp(v).strftime("%Y-%m-%d")
            except Exception:
                return None
    s = str(val).strip()
    m = re.search(r"(20\d{2})[-/\.](\d{1,2})[-/\.](\d{1,2})", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m8 = re.search(r"^(20\d{2})(\d{2})(\d{2})$", s)
    if m8:
        return f"{int(m8.group(1)):04d}-{int(m8.group(2)):02d}-{int(m8.group(3)):02d}"
    m_cn = re.search(r"(20\d{2})年(\d{1,2})月(\d{1,2})日", s)
    if m_cn:
        return f"{int(m_cn.group(1)):04d}-{int(m_cn.group(2)):02d}-{int(m_cn.group(3)):02d}"
    return None


def _extract_table_rows(payload: Any, depth: int = 0) -> list[dict[str, Any]]:
    """递归将 Wind MCP 返回的 JSON（含 {"data":{"data":[{"columns":[...],"rows":[...]}]}}）或 Markdown 表格展平为 list[dict]。"""
    if payload is None or depth > 6:
        return []

    if isinstance(payload, list):
        out: list[dict[str, Any]] = []
        for elem in payload:
            if isinstance(elem, dict):
                if any(k in elem for k in ("columns", "headers", "rows", "tables", "data")):
                    sub = _extract_table_rows(elem, depth + 1)
                    if sub:
                        out.extend(sub)
                        continue
                out.append(elem)
            elif isinstance(elem, str) and "|" in elem:
                out.extend(_extract_table_rows(elem, depth + 1))
        return out

    if isinstance(payload, dict):
        col_list = payload.get("columns") or payload.get("headers") or payload.get("fields") or payload.get("titles")
        row_list = payload.get("rows") or payload.get("data") or payload.get("values") or payload.get("records")
        if isinstance(col_list, list) and isinstance(row_list, list) and col_list and row_list:
            cols = [
                str(c.get("name") or c.get("title") or c.get("label") or c.get("field") or c)
                if isinstance(c, dict) else str(c)
                for c in col_list
            ]
            out = []
            for r in row_list:
                if isinstance(r, dict):
                    out.append(r)
                elif isinstance(r, list) and len(r) == len(cols):
                    out.append(dict(zip(cols, r)))
            if out:
                return out

        for key in ("tables", "table", "data", "items", "rows", "records", "list", "result", "results", "series", "values", "content", "answer", "text"):
            sub = payload.get(key)
            if sub is None:
                continue
            if isinstance(sub, (list, dict)):
                rows = _extract_table_rows(sub, depth + 1)
                if rows:
                    return rows
            elif isinstance(sub, str):
                s_strip = sub.strip()
                if s_strip.startswith(("{", "[")):
                    try:
                        rows = _extract_table_rows(json.loads(s_strip), depth + 1)
                        if rows:
                            return rows
                    except json.JSONDecodeError:
                        pass
                if "|" in s_strip:
                    rows = _extract_table_rows(s_strip, depth + 1)
                    if rows:
                        return rows

        list_vals = {k: v for k, v in payload.items() if isinstance(v, list) and len(v) >= 2}
        if len(list_vals) >= 2:
            lengths = {len(v) for v in list_vals.values()}
            if len(lengths) == 1:
                n = next(iter(lengths))
                keys = list(list_vals.keys())
                return [{k: list_vals[k][i] for k in keys} for i in range(n)]

        if payload and all(isinstance(v, dict) for v in payload.values()):
            out = []
            for k, v in payload.items():
                row = dict(v)
                row.setdefault("_key", k)
                out.append(row)
            return out

        return [payload]

    if isinstance(payload, str):
        lines = [ln.strip() for ln in payload.splitlines() if ln.strip()]
        table_lines = [ln for ln in lines if ln.startswith("|") and ln.endswith("|")]
        if len(table_lines) >= 3:
            headers = [h.strip() for h in table_lines[0].strip("|").split("|")]
            out = []
            for ln in table_lines[2:]:
                if re.match(r"^\|[\s:\-|]+\|$", ln):
                    continue
                cells = [c.strip() for c in ln.strip("|").split("|")]
                if len(cells) == len(headers):
                    out.append(dict(zip(headers, cells)))
            if out:
                return out
    return []


def _normalize_amount_to_yi(raw_val: Any, unit_hint: str = "") -> float | None:
    v = _to_float(raw_val)
    if v is None:
        return None
    s = f"{raw_val} {unit_hint}"
    if "万亿" in s:
        return v * 10000.0
    if "亿" in s:
        return v
    if "千万" in s:
        return v * 0.1
    if "百万" in s:
        return v * 0.01
    if "万" in s:
        return v / 10000.0
    if abs(v) >= 1e9:
        return v / 1e8
    if abs(v) >= 1e5:
        return v / 1e4
    return v


def _find_field(row: dict[str, Any], keywords: tuple[str, ...], exclude: tuple[str, ...] = ()) -> Any:
    for kw in keywords:
        if kw in row and row[kw] not in (None, "", "--"):
            return row[kw]
    for k, v in row.items():
        if v in (None, "", "--"):
            continue
        kl = str(k).lower()
        if any(ex.lower() in kl for ex in exclude):
            continue
        if any(kw.lower() in kl for kw in keywords):
            return v
    return None


def fetch_market_pulse_wind(demo_mode: bool = False) -> dict[str, Any] | None:
    """通过 Wind `index_data.get_index_price_indicators` 单次批量拉取全市场盘面与资金情绪。"""
    if demo_mode:
        return {
            "trade_date": dt.date.today().strftime("%Y-%m-%d"),
            "turnover_yi": 18240.0,
            "up_count": 3318,
            "down_count": 1862,
            "flat_count": 170,
            "main_net_yi": 146.5,
            "benchmarks": [
                {"name": "中证全指", "pct": 0.85},
                {"name": "沪深300",  "pct": 0.62},
                {"name": "创业板指", "pct": 1.34},
                {"name": "恒生指数", "pct": 0.41},
                {"name": "恒生科技", "pct": 1.12},
            ],
        }

    if not _is_wind_enabled():
        return None

    bench_meta = [
        ("000985.CSI", "中证全指"),
        ("000300.SH",  "沪深300"),
        ("399006.SZ",  "创业板指"),
        ("HSI.HI",     "恒生指数"),
        ("HSTECH.HI",  "恒生科技"),
    ]
    windcodes = ",".join(code for code, _ in bench_meta)
    indexes = "最新交易日,最新成交价,涨跌幅,成交额,当日主力净流入额,上涨家数,下跌家数,平盘家数"
    payload, err = call_wind_mcp(
        "index_data",
        "get_index_price_indicators",
        {"windcode": windcodes, "indexes": indexes},
        timeout_sec=30,
    )
    if err or payload is None:
        return None

    unit_hint = ""
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        unit_hint = str(payload["data"].get("unit", ""))

    rows = _extract_table_rows(payload)
    if not rows:
        print(f"[Wind盘面] 未能解析返回表格，预览: {str(payload)[:200]}")
        return None

    row_by_code: dict[str, dict[str, Any]] = {}
    for r in rows:
        code_val = str(_find_field(r, ("windcode", "代码", "证券代码", "指数代码", "code", "_key")) or "").upper()
        for wc, nm in bench_meta:
            if wc in code_val or wc.split(".")[0] in code_val or nm in str(r):
                row_by_code[wc] = r
                break

    all_a = row_by_code.get("000985.CSI")
    if not all_a:
        print(f"[Wind盘面] 未匹配到 000985.CSI，可用行键: {list(row_by_code.keys())}")
        return None

    trade_date = _parse_date_str(_find_field(all_a, ("最新交易日", "交易日期", "date"))) or dt.date.today().strftime("%Y-%m-%d")
    turnover_yi = _normalize_amount_to_yi(_find_field(all_a, ("成交额", "amount")), unit_hint)
    main_net_yi = _normalize_amount_to_yi(_find_field(all_a, ("当日主力净流入额", "主力净流入")), unit_hint)
    up_cnt = _to_float(_find_field(all_a, ("上涨家数",)))
    down_cnt = _to_float(_find_field(all_a, ("下跌家数",)))
    flat_cnt = _to_float(_find_field(all_a, ("平盘家数",)))

    benchmarks = []
    for wc, nm in bench_meta:
        r = row_by_code.get(wc)
        if not r:
            continue
        pct = _to_float(_find_field(r, ("涨跌幅", "pct_chg", "change_pct")))
        if pct is not None:
            benchmarks.append({"name": nm, "pct": round(pct, 2)})

    if turnover_yi is None and up_cnt is None and not benchmarks:
        return None

    print(
        f"[Wind盘面] 日期={trade_date} | 全市场成交额={turnover_yi or 0:.0f}亿 | "
        f"涨{int(up_cnt or 0)}/跌{int(down_cnt or 0)} | 主力净流={main_net_yi or 0:+.1f}亿"
    )
    return {
        "trade_date": trade_date,
        "turnover_yi": turnover_yi,
        "up_count": int(up_cnt) if up_cnt is not None else None,
        "down_count": int(down_cnt) if down_cnt is not None else None,
        "flat_count": int(flat_cnt) if flat_cnt is not None else 0,
        "main_net_yi": main_net_yi,
        "benchmarks": benchmarks,
    }


def fetch_wind_batch_pe_snapshot(wind_codes: list[str]) -> dict[str, dict[str, Any]]:
    """结构化工具 `get_index_price_indicators` 一次批量获取准确 PE-TTM / PB(LF) / 最新交易日。"""
    if not wind_codes or not _is_wind_enabled():
        return {}
    payload, err = call_wind_mcp(
        "index_data",
        "get_index_price_indicators",
        {"windcode": ",".join(wind_codes), "indexes": "最新交易日,市盈率(TTM),市净率(LF)"},
        timeout_sec=35,
    )
    if err or payload is None:
        return {}

    rows = _extract_table_rows(payload)
    snap_map: dict[str, dict[str, Any]] = {}
    for r in rows:
        code_val = str(_find_field(r, ("windcode", "代码", "证券代码", "指数代码", "code", "_key")) or "").upper()
        for wc in wind_codes:
            if wc.upper() in code_val or wc.split(".")[0].upper() in code_val:
                raw_pe = _to_float(_find_field(r, ("市盈率(TTM)", "PE-TTM", "PE_TTM", "市盈率"), exclude=("预测", "分位")))
                raw_pb = _to_float(_find_field(r, ("市净率(LF)", "PB(LF)", "市净率"), exclude=("分位",)))
                snap_map[wc.upper()] = {
                    "pe": raw_pe if (raw_pe is not None and 0.5 < raw_pe < 500.0) else None,
                    "pb": raw_pb if (raw_pb is not None and 0.05 < raw_pb < 100.0) else None,
                    "date": _parse_date_str(_find_field(r, ("最新交易日", "交易日期", "date"))),
                }
                break
    if snap_map:
        print(f"[Wind快照] 结构化批量拉取成功({len(snap_map)}只): {list(snap_map.keys())}")
    return snap_map


def _extract_pe_series_from_wind(payload: Any) -> tuple[pd.Series | None, int, int, str]:
    """
    严格从 Wind 历史 PE-TTM 返回体中提取 (日期 -> PE-TTM) 序列。
    同时校验：点数 >= 24 且 时间跨度 >= 1200 天（杜绝被截断成近 4 个月的假 5 年分位）。
    返回 (series, 有效点数, 跨度天数, 首行样本)。
    """
    rows = _extract_table_rows(payload)
    sample = str(rows[0])[:160] if rows else ""
    dates: list[str] = []
    vals: list[float] = []

    for r in rows:
        d_raw = _find_field(r, ("日期", "交易日", "trade_date", "date", "时间"))
        v_raw = _find_field(
            r,
            ("PE_TTM", "PE-TTM", "市盈率(TTM)", "滚动市盈率", "市盈率", "PE", "数值", "value"),
            exclude=("分位", "预测", "PB", "股息", "代码", "简称"),
        )
        d = _parse_date_str(d_raw)
        v = _to_float(v_raw)
        if d and v is not None and 0.5 < v < 500.0:
            dates.append(d)
            vals.append(v)

    if not vals:
        return None, 0, 0, sample
    df = pd.DataFrame({"date": pd.to_datetime(dates), "val": vals}).dropna().drop_duplicates(subset=["date"]).sort_values("date")
    if len(df) < MIN_5Y_SERIES_POINTS:
        return None, len(df), 0, sample
    span_days = int((df["date"].iloc[-1] - df["date"].iloc[0]).days)
    if span_days < MIN_5Y_SPAN_DAYS:
        return None, len(df), span_days, sample
    return df.set_index("date")["val"], len(df), span_days, sample


def _extract_explicit_percentile_col(payload: Any) -> tuple[float | None, float | None, float | None, str | None]:
    """
    从 Wind 显式表格列中读取 (5年PE分位, PE-TTM, 股息率, 日期)。
    严格要求分位字段列名必须包含“分位”，绝不做盲目文本正则。
    """
    rows = _extract_table_rows(payload)
    pe_pct: float | None = None
    pe_val: float | None = None
    dy_val: float | None = None
    dt_val: str | None = None

    for r in rows:
        for k, v in r.items():
            ks = str(k)
            kl = ks.lower()
            if "分位" in ks and ("5" in ks or "五" in ks or "历史" in ks):
                num = _to_float(v)
                if num is not None and num > 0.1:
                    pe_pct = round(num * 100.0, 1) if num <= 1.0 else (round(num, 1) if num <= 100.0 else None)
            elif pe_val is None and any(w in kl for w in ("pe_ttm", "pe-ttm", "市盈率(ttm)", "滚动市盈率")) and "分位" not in ks and "预测" not in ks:
                num = _to_float(v)
                if num is not None and 0.5 < num < 500:
                    pe_val = num
            elif dy_val is None and ("股息率" in ks or "dyr" in kl):
                num = _to_float(v)
                if num is not None and 0.0 < num < 30:
                    dy_val = round(num * 100.0, 2) if num < 0.25 else round(num, 2)
            elif dt_val is None and ("日期" in ks or "交易日" in ks or kl == "date"):
                dt_val = _parse_date_str(v)
    return pe_pct, pe_val, dy_val, dt_val


def fetch_wind_index_valuation(
    item: dict[str, str],
    struct_snap: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """
    通过 Wind 获取单只指数的精准 PE-TTM 与近 5 年分位：
    1. 最新 PE-TTM / PB(LF) / 日期优先取结构化批量快照 (get_index_price_indicators)
    2. 优先直取 Wind 官方“最新PE在过去5年分位数”显式列（解决 100 行截断问题，道指/CAC40/A500/全指/800/A50/印度 一步精准到位）
    3. 若官方 5 年分位列缺失或返回 0（如日经225 N225.NK、日本东证 TOPX.T），改拉近 5 年“每月末”PE-TTM（约 60 点，永不截断）并强校验跨度 >= 1200 天！
    """
    core_code = item["code"]
    display_code = item["display_code"]
    wind_code = item.get("wind_code") or item["raw_code"]
    name = item["name"]

    alias_info = INDEX_ALIAS_MAP.get(core_code, {})
    wind_name = alias_info.get("wind_name") or name
    idx_style = alias_info.get("style", "broad")
    if any(k in name for k in ("红利", "高股息", "现金")):
        idx_style = "dividend"

    today = dt.date.today()
    raw_s_pe = struct_snap.get("pe") if struct_snap else None
    raw_s_pb = struct_snap.get("pb") if struct_snap else None
    current_pe = raw_s_pe if (raw_s_pe is not None and 0.5 < raw_s_pe < 500.0) else None
    current_pb = raw_s_pb if (raw_s_pb is not None and 0.05 < raw_s_pb < 100.0) else None
    latest_date = struct_snap.get("date") if struct_snap else None
    div_yield: float | None = None
    pe_pct: float | None = None
    mode_tag = "Wind5年"

    # Step A：优先直取 Wind 官方“最新PE在过去5年分位数” + 股息率
    q_pct = f"查询{wind_name}({wind_code})最新滚动市盈率PE-TTM、最新PE在过去5年分位数和股息率"
    pct_payload, pct_err = call_wind_mcp("index_data", "get_index_fundamentals", {"question": q_pct}, timeout_sec=40)
    if pct_err:
        return None, pct_err

    p_pct, p_pe, p_dy, p_dt = _extract_explicit_percentile_col(pct_payload)
    if current_pe is None and p_pe is not None:
        current_pe = p_pe
    if not latest_date and p_dt:
        latest_date = p_dt
    if p_dy is not None:
        div_yield = p_dy
    if p_pct is not None and p_pct > 0.1:
        pe_pct = p_pct

    # Step B：若官方 5 年分位列为空或异常为 0（如日经225 / 东证），拉取近 5 年“每月末”PE-TTM（约60行，绝不触发100行截断）
    if pe_pct is None or pe_pct <= 0.1:
        start_5y = (today - dt.timedelta(days=365 * 5 + 15)).strftime("%Y-%m-%d")
        q_month = f"查询{wind_name}({wind_code})从{start_5y}至{today.strftime('%Y-%m-%d')}每月末的滚动市盈率PE_TTM"
        m_payload, m_err = call_wind_mcp("index_data", "get_index_fundamentals", {"question": q_month}, timeout_sec=45)
        if not m_err and m_payload is not None:
            pe_series, n_pts, span_days, sample = _extract_pe_series_from_wind(m_payload)
            if pe_series is not None:
                if current_pe is None:
                    current_pe = float(pe_series.iloc[-1])
                if not latest_date:
                    latest_date = pd.Timestamp(pe_series.index[-1]).strftime("%Y-%m-%d")
                pe_pct = round(float((pe_series <= current_pe).mean() * 100.0), 1)
                mode_tag = f"{n_pts}月"
            else:
                print(f"[Wind调试-月频未达标] {wind_code} 点数={n_pts} 跨度={span_days}天 首行={sample}")

    if current_pe is None or current_pe <= 0.5 or pe_pct is None or pe_pct <= 0.0:
        p_prev = (pct_payload if isinstance(pct_payload, str) else json.dumps(pct_payload, ensure_ascii=False))[:220]
        print(f"[Wind未命中] {name} ({wind_code}): pe={current_pe} pct={pe_pct} | 预览={p_prev}")
        return None, None

    latest_date = latest_date or today.strftime("%Y-%m-%d")
    stale_days = (today - dt.date.fromisoformat(latest_date)).days
    if stale_days > MAX_STALE_DAYS:
        return {
            "status": "stale",
            "code": core_code,
            "display_code": display_code,
            "name": name,
            "current_pe": round(current_pe, 2),
            "latest_date": latest_date,
            "stale_days": stale_days,
            "source": "Wind",
        }, None

    if current_pb is not None:
        current_pb = round(current_pb, 2)

    composite_pct = round(pe_pct, 1)
    tier = classify_percentile(pe_pct)
    dca_action = get_dca_action(
        composite_pct=composite_pct,
        pe_pct=pe_pct,
        pb_pct=None,
        div_yield=div_yield,
        is_dividend_idx=(idx_style == "dividend"),
    )
    print(f"[Wind精准] {name} ({wind_code}): PE-TTM={current_pe:.2f} ({pe_pct:.1f}%, {mode_tag}), 日期={latest_date}")
    return {
        "status": "ok",
        "code": core_code,
        "display_code": display_code,
        "name": name,
        "current_pe": round(current_pe, 2),
        "pe_percentile": round(pe_pct, 1),
        "current_pb": current_pb,
        "pb_percentile": None,
        "div_yield": div_yield,
        "roe": None,
        "composite_pct": composite_pct,
        "dca_action": dca_action,
        "tier_idx": tier["idx"],
        "tier_label": tier["label"],
        "latest_date": latest_date,
        "source": "Wind",
    }, None


# ============================================================================
# 6. 单只指数估值计算（核心10只走蛋卷，其余9只直连 Wind）
# ============================================================================
def _build_demo_record(item: dict[str, str]) -> dict[str, Any]:
    """仅用于本地 --demo 离线自测，实盘运行绝不调用。"""
    today_str = dt.date.today().strftime("%Y-%m-%d")
    core_code = item["code"]
    demo_map = {
        "HSTECH":  (16.6,  2.7, 1.95, 18.0, 1.1, 10.5, today_str, "蛋卷"),
        "399006":  (24.7,  4.6, 3.40, 22.0, 1.2, 13.5, today_str, "蛋卷"),
        "NDX":     (24.6,  6.2, 7.10, 58.0, 0.7, 23.0, today_str, "蛋卷"),
        "SENSEX":  (21.3, 12.1, 3.10, None, 1.2, None, today_str, "Wind"),
        "DJI":     (21.7, 14.1, 7.21, None, 0.8, None, today_str, "Wind"),
        "SPX":     (22.1, 19.3, 4.90, 72.0, 1.3, 18.8, today_str, "蛋卷"),
        "N225":    (19.3, 48.3, None, None, 1.7, None, today_str, "Wind"),
        "TOPX":    (16.4, 50.4, 1.66, None, 2.8, None, today_str, "Wind"),
        "HSI":     (10.1, 53.7, 1.08, 46.0, 3.8, 10.2, today_str, "蛋卷"),
        "000905":  (25.1, 55.2, 1.78, 44.0, 1.8,  7.8, today_str, "蛋卷"),
        "FCHI":    (16.4, 59.1, None, None, 3.1, None, today_str, "Wind"),
        "000985":  (18.2, 60.7, 1.68, None, 2.3, None, today_str, "Wind"),
        "000510":  (15.2, 67.7, 1.52, None, 2.7, None, today_str, "Wind"),
        "000906":  (14.6, 70.0, 1.40, None, 2.8, None, today_str, "Wind"),
        "000300":  (13.4, 72.2, 1.32, 34.0, 3.0, 10.5, today_str, "蛋卷"),
        "GDAXI":   (18.4, 78.8, 1.85, 68.0, 2.9, 10.4, today_str, "蛋卷"),
        "8841233": (12.2, 86.3, 1.38, None, 3.5, None, today_str, "Wind"),
        "000922":  ( 9.5, 99.9, 0.88, 65.0, 5.2, 10.9, today_str, "蛋卷"),
        "H30269":  ( 9.5, 99.9, 0.82, 60.0, 5.4, 10.5, today_str, "蛋卷"),
    }
    if core_code not in demo_map:
        return {
            "status": "unavailable",
            "code": core_code,
            "display_code": item["display_code"],
            "wind_code": item.get("wind_code", item["raw_code"]),
            "name": item["name"],
            "reason": "免费公开接口暂未收录",
        }
    pe, pe_pct, pb, pb_pct, dy, roe, ldate, src = demo_map[core_code]
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
    wind_enabled: bool = False,
    demo_mode: bool = False,
) -> dict[str, Any]:
    """计算单只指数的真实 PE-TTM（已配置 Wind 时，prefer_wind=True 的 9 只直接交由 Wind 统一拉取）。"""
    if demo_mode:
        return _build_demo_record(item)

    core_code = item["code"]
    display_code = item["display_code"]
    name = item["name"]

    alias_info = INDEX_ALIAS_MAP.get(core_code, {})
    idx_style = alias_info.get("style", "broad")
    if any(k in name for k in ("红利", "高股息", "现金")):
        idx_style = "dividend"

    if wind_enabled and alias_info.get("prefer_wind"):
        return {
            "status": "wind_queue",
            "code": core_code,
            "display_code": display_code,
            "wind_code": item.get("wind_code", item["raw_code"]),
            "name": name,
        }

    if "danjuan" in alias_info:
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

    pe_series = fetch_danjuan_5y_series(candidates, metric="pe") if candidates else None
    pb_series = fetch_danjuan_5y_series(candidates, metric="pb") if candidates else None
    source_name = "蛋卷"

    if pe_series is None and not wind_enabled:
        cs_code = alias_info.get("csindex", "")
        if not cs_code and (core_code.startswith(("000", "93", "H3")) and len(core_code) == 6):
            cs_code = core_code
        if cs_code:
            pe_series = fetch_csindex_5y_pe_ttm(cs_code)
            if pe_series is not None:
                source_name = "中证"

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
        return {
            "status": "unavailable",
            "code": core_code,
            "display_code": display_code,
            "wind_code": item.get("wind_code", item["raw_code"]),
            "name": name,
            "reason": "免费接口暂未收录",
        }

    stale_days = (dt.date.today() - latest_dt).days
    if stale_days > MAX_STALE_DAYS:
        return {
            "status": "stale",
            "code": core_code,
            "display_code": display_code,
            "wind_code": item.get("wind_code", item["raw_code"]),
            "name": name,
            "current_pe": round(current_pe, 2),
            "latest_date": latest_date,
            "stale_days": stale_days,
            "source": source_name,
        }

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
# 7. 重构版微信 HTML 晚报（明确标注近5年分位阈值 + 全市场情绪看板）
# ============================================================================
def _fmt_turnover(yi_val: float | None) -> tuple[str, str]:
    if yi_val is None:
        return "—", ""
    if yi_val >= 10000.0:
        val_str = f"{yi_val / 10000.0:.2f}万亿"
    else:
        val_str = f"{yi_val:.0f}亿"
    if yi_val >= 20000.0:
        tag = "放量活跃"
    elif yi_val >= 12000.0:
        tag = "交投温和"
    else:
        tag = "缩量观望"
    return val_str, tag


def _build_market_pulse_html(pulse: dict[str, Any] | None) -> str:
    if not pulse:
        return ""

    t_val, t_tag = _fmt_turnover(pulse.get("turnover_yi"))
    up = pulse.get("up_count")
    down = pulse.get("down_count")
    flat = pulse.get("flat_count") or 0
    main_net = pulse.get("main_net_yi")

    if up is not None and down is not None and (up + down + flat) > 0:
        total = up + down + flat
        up_pct = up / total * 100.0
        down_pct = down / total * 100.0
        flat_pct = max(0.0, 100.0 - up_pct - down_pct)
        breadth_val = f"{up_pct:.0f}%"
        breadth_sub = f"涨{up}/跌{down}"
        breadth_cls = "up" if up_pct >= 50.0 else "dn"
        breadth_bar = (
            f'<div class="ad">'
            f'<i style="width:{up_pct:.1f}%;background:#EF4444"></i>'
            f'<i style="width:{flat_pct:.1f}%;background:#CBD5E1"></i>'
            f'<i style="width:{down_pct:.1f}%;background:#10B981"></i>'
            f'</div>'
        )
    else:
        breadth_val, breadth_sub, breadth_cls, breadth_bar = "—", "暂无家数", "", ""

    if main_net is not None:
        net_val = f"{main_net:+.0f}亿"
        net_cls = "up" if main_net >= 0 else "dn"
        net_sub = "净流入" if main_net >= 0 else "净流出"
    else:
        net_val, net_cls, net_sub = "—", "", "主力资金"

    bench_pills = []
    for b in pulse.get("benchmarks", [])[:5]:
        p = float(b["pct"])
        cls = "up" if p >= 0 else "dn"
        bench_pills.append(f'<span class="ib">{b["name"]} <b class="{cls}">{p:+.2f}%</b></span>')
    bench_html = f'<div class="ix">{"".join(bench_pills)}</div>' if bench_pills else ""

    short_dt = str(pulse.get("trade_date", ""))[5:]
    return (
        f'<div class="cd">'
        f'<div class="ct">🔥 全市场情绪与资金面<i> · {short_dt} Wind</i></div>'
        f'<div class="mg">'
        f'<div class="mc"><div class="ml">A股成交额</div><div class="mv">{t_val}</div><div class="ms">{t_tag}</div></div>'
        f'<div class="mc"><div class="ml">赚钱效应</div><div class="mv {breadth_cls}">{breadth_val}</div><div class="ms">{breadth_sub}</div></div>'
        f'<div class="mc"><div class="ml">主力资金</div><div class="mv {net_cls}">{net_val}</div><div class="ms">{net_sub}</div></div>'
        f'</div>'
        f'{breadth_bar}'
        f'{bench_html}'
        f'</div>'
    )


def build_wechat_html(
    ok_records: list[dict[str, Any]],
    stale_records: list[dict[str, Any]],
    unavail_records: list[dict[str, Any]],
    market_pulse: dict[str, Any] | None = None,
    cn_bond_10y: float = 2.12,
    compact_only: bool = False,
) -> str:
    """构建移动端高密度微信 HTML 估值晚报，严格控制在 PushPlus 20,000 字上限内。"""
    df_sorted = pd.DataFrame(ok_records).sort_values("pe_percentile", ascending=True).reset_index(drop=True)
    report_date = df_sorted["latest_date"].max() if not df_sorted.empty else dt.date.today().strftime("%Y-%m-%d")

    cnt_low = int((df_sorted["pe_percentile"] < 30).sum())
    cnt_mid_low = int(((df_sorted["pe_percentile"] >= 30) & (df_sorted["pe_percentile"] < 50)).sum())
    cnt_mid_high = int(((df_sorted["pe_percentile"] >= 50) & (df_sorted["pe_percentile"] <= 70)).sum())
    cnt_high = int((df_sorted["pe_percentile"] > 70).sum())

    hs300_row = df_sorted[df_sorted["code"] == "000300"]
    erp_box = ""
    if not hs300_row.empty:
        hs300_pe = float(hs300_row["current_pe"].iloc[0])
        erp_val = (100.0 / hs300_pe) - cn_bond_10y
        if erp_val >= 5.5:
            erp_desc = "极高性价比(股显著优于债)"
        elif erp_val >= 4.2:
            erp_desc = "偏高性价比(偏股配置)"
        elif erp_val >= 3.0:
            erp_desc = "股债均衡"
        else:
            erp_desc = "溢价偏低(注意防御)"
        erp_box = f'<div class="mb">⚖️ <b>沪深300股债利差(ERP)</b>：<b>{erp_val:.2f}%</b> · {erp_desc}</div>'

    bar_rows = []
    table_rows = []
    used_wind = market_pulse is not None
    for _, r in df_sorted.iterrows():
        t_idx = int(r["tier_idx"])
        pct = max(2.0, min(100.0, float(r["pe_percentile"])))
        short_dt = str(r["latest_date"])[5:]
        if r["source"] == "Wind":
            used_wind = True

        if pd.notna(r["current_pb"]):
            pb_sub = f'{r["pb_percentile"]:.0f}%分位' if pd.notna(r["pb_percentile"]) else "LF"
            pb_html = f'<b>{r["current_pb"]:.2f}</b><br><i>{pb_sub}</i>'
        else:
            pb_html = "<i>—</i>"

        if pd.notna(r["div_yield"]):
            dy_html = f'<b>{r["div_yield"]:.1f}%</b>' + (f'<br><i>ROE {r["roe"]:.0f}%</i>' if pd.notna(r["roe"]) else "")
        else:
            dy_html = "<i>—</i>"

        if not compact_only:
            bar_rows.append(
                f'<div class="bi"><div class="bh"><b>{r["name"]}<i>({r["display_code"]})</i></b>'
                f'<s class="bp t{t_idx}">{r["current_pe"]:.1f}({r["pe_percentile"]:.0f}%) · {r["dca_action"]}</s></div>'
                f'<div class="bt"><i class="b{t_idx}" style="width:{pct:.1f}%"></i></div></div>'
            )
        table_rows.append(
            f'<tr><td><b>{r["name"]}</b><br><i>{r["display_code"]} · {r["source"]}{short_dt}</i></td>'
            f'<td class="c{t_idx}"><b>{r["current_pe"]:.1f}</b><br><i>近5年{r["pe_percentile"]:.0f}%</i></td>'
            f'<td>{pb_html}</td>'
            f'<td>{dy_html}</td>'
            f'<td><s class="tg t{t_idx}">{r["tier_label"]}</s><br><b class="da">{r["dca_action"]}</b></td></tr>'
        )

    notice_blocks = []
    if stale_records:
        stale_items = "、".join(f'{x["name"]}({x["display_code"]}, 停更{x["latest_date"]})' for x in stale_records)
        notice_blocks.append(f'<div class="nw"><b>⏸️ 停更隔离({len(stale_records)}只)</b>：{stale_items}</div>')
    if unavail_records:
        unavail_items = "、".join(f'{x["name"]}({x["display_code"]})' for x in unavail_records)
        notice_blocks.append(f'<div class="nu"><b>ℹ️ 暂未收录({len(unavail_records)}只)</b>：{unavail_items}</div>')
    notice_html = f'<div class="cd">{"".join(notice_blocks)}</div>' if notice_blocks else ""

    footer_src = "数据来源：蛋卷 / 万得Wind金融数据服务（百分位口径：近5年PE-TTM）" if used_wind else "数据来源：蛋卷 / 中证指数（百分位口径：近5年PE-TTM）"
    pulse_html = _build_market_pulse_html(market_pulse)

    style_css = (
        "<style>"
        ".vw{font-family:-apple-system,'PingFang SC',sans-serif;background:#F1F5F9;padding:8px;color:#0F172A;line-height:1.38}"
        ".hd{background:linear-gradient(135deg,#0F172A,#1E3A8A);color:#FFF;padding:11px;border-radius:10px;margin-bottom:8px}"
        ".ht{font-size:15px;font-weight:700}.hs{font-size:10.5px;color:#CBD5E1;margin-top:2px}"
        ".mb{background:rgba(255,255,255,.1);border-radius:7px;padding:6px 8px;margin-top:7px;font-size:11px;color:#F8FAFC}"
        ".kg{display:flex;gap:4px;margin-top:7px;text-align:center}.kc{flex:1;border-radius:7px;padding:4px 2px}"
        ".kv{font-size:14px;font-weight:700}.kl{font-size:9px}"
        ".cd{background:#FFF;border-radius:9px;padding:9px;margin-bottom:8px;border:1px solid #E2E8F0}"
        ".ct{font-size:12.5px;font-weight:700;color:#0F172A;margin-bottom:4px}.ct i{font-style:normal;font-weight:400;font-size:10px;color:#64748B}"
        ".cs{font-size:10px;color:#64748B;margin-bottom:6px}"
        ".mg{display:flex;gap:6px;margin-bottom:6px;text-align:center}"
        ".mc{flex:1;background:#F8FAFC;border:1px solid #E2E8F0;border-radius:7px;padding:6px 4px}"
        ".ml{font-size:9.5px;color:#64748B}.mv{font-size:14px;font-weight:700;color:#0F172A;margin:1px 0}.ms{font-size:9px;color:#64748B}"
        ".up{color:#DC2626}.dn{color:#059669}"
        ".ad{display:flex;height:6px;border-radius:4px;overflow:hidden;margin-bottom:6px}"
        ".ad i{display:block;height:100%}"
        ".ix{display:flex;flex-wrap:wrap;gap:4px}"
        ".ib{background:#F1F5F9;border-radius:5px;padding:2px 6px;font-size:9.5px;color:#334155}"
        ".bi{margin-bottom:7px}.bh{display:flex;justify-content:space-between;align-items:center;font-size:11px;margin-bottom:2px}"
        ".bh b i{font-style:normal;font-weight:400;color:#64748B;font-size:9px;margin-left:2px}"
        "s.bp,s.tg{text-decoration:none;font-weight:700;padding:1px 5px;border-radius:6px;font-size:9.5px;display:inline-block}"
        ".bt{height:8px;background:linear-gradient(90deg,#ECFDF5 0,#ECFDF5 29.7%,#94A3B8 30%,#EFF6FF 30.3%,#EFF6FF 49.7%,#94A3B8 50%,#FFFBEB 50.3%,#FFFBEB 69.7%,#94A3B8 70%,#FEF2F2 70.3%,#FEF2F2 100%);border-radius:4px;overflow:hidden;border:1px solid #E2E8F0}"
        ".bt i{display:block;height:100%;border-radius:4px}"
        ".t0{color:#065F46;background:#D1FAE5}.t1{color:#1E40AF;background:#DBEAFE}.t2{color:#92400E;background:#FEF3C7}.t3{color:#991B1B;background:#FEE2E2}"
        ".b0{background:#10B981}.b1{background:#3B82F6}.b2{background:#F59E0B}.b3{background:#EF4444}"
        ".c0{color:#065F46}.c1{color:#1E40AF}.c2{color:#92400E}.c3{color:#991B1B}"
        "table.vt{width:100%;border-collapse:collapse;font-size:10px}"
        ".vt th{background:#1E293B;color:#FFF;padding:5px 2px;font-size:9px;text-align:right}.vt th:first-child{text-align:left}.vt th:last-child{text-align:center}"
        ".vt td{padding:4px 2px;border-bottom:1px solid #E2E8F0;text-align:right;vertical-align:middle}.vt td:first-child{text-align:left}.vt td:last-child{text-align:center}"
        ".vt tr:nth-child(even){background:#F8FAFC}.vt td i{font-style:normal;font-size:8.5px;color:#64748B}"
        ".da{font-size:9px;color:#334155}"
        ".nw{background:#FEF3C7;color:#92400E;padding:5px 7px;border-radius:6px;font-size:10px;margin-bottom:4px}"
        ".nu{background:#F1F5F9;color:#475569;padding:5px 7px;border-radius:6px;font-size:10px}"
        ".ft{text-align:center;font-size:9.5px;color:#94A3B8;padding:2px 0}"
        "</style>"
    )

    bar_section = ""
    if not compact_only:
        bar_section = (
            f'<div class="cd"><div class="ct">🎯 近5年PE-TTM分位与定投信号</div>'
            f'<div class="cs">近5年分位：<b class="c0">&lt;30%低估</b> / <b class="c1">30-50%偏低</b> / <b class="c2">50-70%偏高</b> / <b class="c3">&gt;70%高估</b></div>'
            f'{"".join(bar_rows)}</div>'
        )

    html = (
        f'{style_css}<div class="vw">'
        f'<div class="hd"><div class="ht">📈 每日市场与指数估值晚报</div>'
        f'<div class="hs">数据日期：{report_date} · 实时有效 {len(df_sorted)} 只 · 口径：近5年PE-TTM</div>'
        f'{erp_box}'
        f'<div class="kg">'
        f'<div class="kc" style="background:rgba(16,185,129,.2);border:1px solid #10B981"><div class="kv" style="color:#34D399">{cnt_low}</div><div class="kl" style="color:#D1FAE5">近5年&lt;30%</div></div>'
        f'<div class="kc" style="background:rgba(59,130,246,.2);border:1px solid #3B82F6"><div class="kv" style="color:#60A5FA">{cnt_mid_low}</div><div class="kl" style="color:#DBEAFE">近5年30-50%</div></div>'
        f'<div class="kc" style="background:rgba(245,158,11,.2);border:1px solid #F59E0B"><div class="kv" style="color:#FBBF24">{cnt_mid_high}</div><div class="kl" style="color:#FEF3C7">近5年50-70%</div></div>'
        f'<div class="kc" style="background:rgba(239,68,68,.2);border:1px solid #EF4444"><div class="kv" style="color:#F87171">{cnt_high}</div><div class="kl" style="color:#FEE2E2">近5年&gt;70%</div></div>'
        f'</div></div>'
        f'{pulse_html}'
        f'{bar_section}'
        f'<div class="cd" style="overflow-x:auto"><div class="ct">📋 多维估值交叉印证与定投明细表</div>'
        f'<table class="vt"><thead><tr><th>指数(来源/日期)</th><th>PE-TTM(近5年分位)</th><th>PB</th><th>股息/ROE</th><th>定投建议</th></tr></thead>'
        f'<tbody>{"".join(table_rows)}</tbody></table></div>'
        f'{notice_html}'
        f'<div class="ft">{footer_src}</div>'
        f'</div>'
    )
    print(f"[HTML统计] {'精简版' if compact_only else '完整版'}总字符数: {len(html)} / 20000 字上限")
    return html


def send_via_pushplus(token: str, title: str, html_content: str, fallback_html: str = "", topic: str = "") -> bool:
    """通过 PushPlus 推送 HTML 到个人微信；超 18,500 字或遇 code=999 自动降级发送精简版。"""
    url = "http://www.pushplus.plus/send"
    chosen = fallback_html if (len(html_content) > PUSHPLUS_SAFE_CHAR_LIMIT and fallback_html) else html_content
    if chosen is fallback_html and html_content != fallback_html:
        print(f"[自动控字] 完整版({len(html_content)}字)接近上限，自动切换精简版({len(fallback_html)}字)发送。")

    payload = {
        "token": token,
        "title": title,
        "content": chosen,
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
        if res_json.get("code") == 999 and fallback_html and chosen != fallback_html:
            print("[自动降级] 检测到 PushPlus 字数限制，正在使用精简版 HTML 重新发送...")
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
# 8. 主程序入口
# ============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="每日市场情绪与指数估值晚报微信推送脚本")
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

        wind_enabled = (not args.demo) and _is_wind_enabled()
        batch_snapshot = {} if args.demo else fetch_danjuan_batch_snapshot()

        ok_records: list[dict[str, Any]] = []
        stale_records: list[dict[str, Any]] = []
        unavail_records: list[dict[str, Any]] = []
        wind_queue: list[dict[str, Any]] = []
        item_by_code = {it["code"]: it for it in indices}

        with ThreadPoolExecutor(max_workers=8) as executor:
            future_map = {
                executor.submit(compute_index_valuation, item, batch_snapshot, wind_enabled, args.demo): item
                for item in indices
            }
            for fut in as_completed(future_map):
                try:
                    res = fut.result(timeout=12)
                    st = res.get("status")
                    if st == "ok":
                        ok_records.append(res)
                    elif st == "wind_queue":
                        wind_queue.append(res)
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
                        "wind_code": item.get("wind_code", item["raw_code"]),
                        "name": item["name"],
                        "reason": "接口请求异常",
                    })

        market_pulse = fetch_market_pulse_wind(demo_mode=args.demo)
        if wind_enabled:
            need_wind = wind_queue + unavail_records + stale_records
            if need_wind:
                print(f"[Wind精准拉取] 准备通过万得 MCP 统一拉取 {len(need_wind)} 只指数近5年 PE-TTM...")
                wind_codes = [
                    item_by_code.get(r["code"], {}).get("wind_code", r.get("wind_code", r["display_code"]))
                    for r in need_wind
                ]
                struct_snaps = fetch_wind_batch_pe_snapshot(wind_codes)

                still_unavail: list[dict[str, Any]] = []
                still_stale: list[dict[str, Any]] = []
                abort_wind = False

                for rec, wc in zip(need_wind, wind_codes):
                    if abort_wind:
                        (still_stale if rec.get("status") == "stale" else still_unavail).append(rec)
                        continue
                    orig_item = item_by_code.get(rec["code"], {
                        "code": rec["code"],
                        "display_code": rec["display_code"],
                        "wind_code": wc,
                        "raw_code": wc,
                        "name": rec["name"],
                    })
                    w_res, w_err = fetch_wind_index_valuation(orig_item, struct_snap=struct_snaps.get(wc.upper()))
                    if w_err in ("AUTH_ERROR", "RATE_LIMIT_ERROR", "AUTH_MISSING", "SKILL_NOT_FOUND"):
                        print(f"[Wind熔断] 遇到 {w_err}，停止后续拉取。")
                        abort_wind = True
                        (still_stale if rec.get("status") == "stale" else still_unavail).append(rec)
                    elif w_res and w_res.get("status") == "ok":
                        ok_records.append(w_res)
                    elif w_res and w_res.get("status") == "stale":
                        still_stale.append(w_res)
                    else:
                        (still_stale if rec.get("status") == "stale" else still_unavail).append(rec)

                unavail_records = still_unavail
                stale_records = still_stale
        elif not args.demo and (unavail_records or stale_records):
            print("[提示] 未检测到 WIND_API_KEY 或 wind-mcp-skill，跳过 Wind 精准拉取与全市场盘面看板。")

        total_elapsed = time.perf_counter() - start_ts
        print(
            f"[拉取汇总] 实时有效 {len(ok_records)} 只 | 数据停更 {len(stale_records)} 只 | "
            f"未收录 {len(unavail_records)} 只，总耗时 {total_elapsed:.2f} 秒。"
        )

        if not ok_records:
            print("[错误] 未能获取到任何实时有效的指数估值数据，程序终止。")
            sys.exit(1)

        html_content = build_wechat_html(ok_records, stale_records, unavail_records, market_pulse=market_pulse, compact_only=False)
        fallback_html = build_wechat_html(ok_records, stale_records, unavail_records, market_pulse=market_pulse, compact_only=True)
        html_path.write_text(html_content, encoding="utf-8")

        report_date = max(r["latest_date"] for r in ok_records)
        low_cnt = sum(1 for r in ok_records if r["pe_percentile"] < 30)
        turn_tag = ""
        if market_pulse and market_pulse.get("turnover_yi"):
            t_str, _ = _fmt_turnover(market_pulse["turnover_yi"])
            turn_tag = f" | 成交{t_str}"
        title = f"📊 估值晚报({report_date}){turn_tag} | 低估{low_cnt}只"
        title_path.write_text(title, encoding="utf-8")
        print(f"[HTML报表] 已生成微信 HTML 报表: {html_path}")
    else:
        if not html_path.exists():
            print(f"[错误] 未找到已生成的 HTML 报表文件: {html_path}")
            sys.exit(1)
        html_content = html_path.read_text(encoding="utf-8")
        title = title_path.read_text(encoding="utf-8").strip() if title_path.exists() else "📊 每日市场与指数估值晚报"

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
