"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { motion } from "framer-motion";
import {
  Activity,
  Calendar,
  ChevronLeft,
  ChevronRight,
  Info,
  Moon,
  RefreshCw,
  Search,
  ShieldAlert,
  Sparkles,
  Sun,
  TrendingDown,
  TrendingUp,
} from "lucide-react";

import type {
  DailyEquityMovers,
  DebugInfo,
  DerivativesResponse,
  EventPoint,
  FlowPanel,
  ForecastMetrics,
  ForecastView,
  ForecastPanel,
  EventClusterResponse,
  EventClusterView,
  IntelResponse,
  MarketSnapshot,
  PortfolioResponse,
  RiskPanel,
} from "@/app/api/types";
import { cx, fmtNum, formatCurrencyCompact, formatNumber, formatPercent, toTSIString } from "@/lib/utils";
import { buildRuleInsights } from "@/lib/insightRules";
import RiskPanelV2 from "@/components/RiskPanelV2";

const BACKEND_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080";
const API_PROXY = "/api";
const PORTFOLIO_ENABLED =
  (process.env.NEXT_PUBLIC_PORTFOLIO_ENABLED ?? "true").toLowerCase() in
  { "1": true, "true": true, "yes": true, "on": true };
type AssetClass = "BIST" | "NASDAQ" | "CRYPTO";
type DebateOutput = {
  executiveSummary?: string[];
  portfolioMode?: string;
  topImpacted?: Array<{ symbol?: string; direction?: string; why?: string[] | string }>;
  watchMetrics?: string[];
  actions?: Array<{ symbol?: string; action?: string; reason?: string[] | string }>;
  trimSignals?: Array<{ symbol?: string; action?: string; deltaWeight?: number; evidence_ids?: string[] }>;
  sectorFocus?: Array<{ sector?: string; why?: string[] | string; evidence_ids?: string[] }>;
  scenarios?: { base?: string[]; risk?: string[] };
  note?: string;
};
type DebateResponse = {
  generatedAtTSI?: string;
  dataStatus?: string;
  reason?: string;
  cache?: { hit?: boolean; ttl_seconds?: number; cooldown_remaining_seconds?: number };
  providers?: { openrouter?: string; openai?: string };
  provider_meta?: {
    openrouter?: { status?: string; model_used?: string; error_code?: string | null; error_message?: string | null };
    openai?: { status?: string; model_used?: string; error_code?: string | null; error_message?: string | null };
  };
  winner?: string;
  consensus?: any;
  disagreements?: any;
  raw?: { openrouter?: DebateOutput; openai?: DebateOutput };
  referee?: { status?: string; model_used?: string; error?: string | null; result?: any };
  executiveSummary?: string[];
  portfolioMode?: string;
  topImpacted?: Array<{ symbol?: string; direction?: string; why?: string[] | string }>;
  watchMetrics?: string[];
  actions?: Array<{ symbol?: string; action?: string; reason?: string[] | string }>;
  debug_notes?: string[];
};
const TF_ORDER = ["15m", "1h", "3h", "6h"];
const ENV_KEYS = [
  "FINNHUB_API_KEY",
  "TWELVEDATA_API_KEY",
  "OPENAI_API_KEY",
  "OPENAI_MODEL",
  "ENABLE_OPENAI_SUMMARY",
  "PY_INTEL_BASE_URL",
  "DATABASE_URL",
  "NEXT_PUBLIC_API_BASE",
];
const BAR_ASSETS = [
  "BTC",
  "ETH",
  "NASDAQ",
  "FTSE",
  "EUROSTOXX",
  "OIL",
  "GOLD",
  "SILVER",
  "COPPER",
  "BIST",
  "DXY",
  "AAPL",
  "MSFT",
  "AMZN",
  "GOOGL",
  "META",
  "NVDA",
  "TSLA",
  "MSTR",
  "COIN",
  "XOM",
  "CVX",
  "COP",
  "OXY",
  "SLB",
  "EOG",
  "MPC",
  "PSX",
  "VLO",
];
const PORTFOLIO_BIST_SYMBOLS = new Set(["ASTOR", "SOKM", "TUPRS", "ENJSA"]);
const PORTFOLIO_NASDAQ_SYMBOLS = new Set(["SIL", "AMD", "PLTR", "HL"]);
const PORTFOLIO_CRYPTO_SYMBOLS = new Set(["BTC", "NEAR"]);
const TECH_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "META", "AMZN", "NVDA", "TSLA", "MSTR", "COIN"];
const TECH_SYMBOLS_GLOBAL = ["ASML.AS", "SAP.DE", "005930.KS", "6758.T", "SHOP.TO", "ADYEN.AS", "NOKIA.HE", "0700.HK", "9988.HK"];
const ENERGY_SYMBOLS = ["XOM", "CVX", "COP", "OXY", "SLB", "EOG", "MPC", "PSX", "VLO"];
const ENERGY_SYMBOLS_GLOBAL = ["SHEL", "TTE", "BP", "EQNR", "PBR", "ENB", "SU.TO", "CNQ.TO", "REP.MC"];
const FINANCIALS_SYMBOLS = ["JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "AXP"];
const FINANCIALS_SYMBOLS_GLOBAL = ["HSBA.L", "UBSG.SW", "BNP.PA", "DBK.DE", "INGA.AS", "8058.T", "SAN.MC", "BARC.L", "ZURN.SW"];
const INDUSTRIALS_SYMBOLS = ["CAT", "DE", "BA", "GE", "HON", "UNP", "UPS", "LMT", "RTX"];
const INDUSTRIALS_SYMBOLS_GLOBAL = ["SIE.DE", "AIR.PA", "DPW.DE", "VOLV-B.ST", "7203.T", "7267.T", "CP.TO", "6501.T", "SGRO.L"];
const MATERIALS_SYMBOLS = ["LIN", "APD", "SHW", "ECL", "DD", "DOW", "NUE", "FCX", "NEM"];
const MATERIALS_SYMBOLS_GLOBAL = ["BHP.AX", "RIO.AX", "GLEN.L", "ANTO.L", "BAS.DE", "SIKA.SW", "AEM.TO", "NTR.TO", "IVN.AX"];
const DEFENCE_TECH_SYMBOLS_US = ["LMT", "RTX", "NOC", "GD", "LHX", "HII", "TDG", "AVAV", "KTOS"];
const DEFENCE_TECH_SYMBOLS_EU = ["BA.L", "AIR.PA", "RHM.DE", "HO.PA", "LDO.MI", "SAAB-B.ST", "SAF.PA", "HAG.DE", "AM.PA"];
const DEFENCE_TECH_SYMBOLS_TR = ["ASELS.IS", "OTKAR.IS", "SDTTR.IS", "ALTNY.IS", "ONRYT.IS", "PAPIL.IS", "PATEK.IS", "KATMR.IS", "TMSN.IS"];
const DEFENCE_TECH_SYMBOLS_ME = ["CHKP", "CYBR", "NICE", "ESLT", "IAI.TA", "ESLT.TA", "NICE.TA", "MGDL.TA", "FIBI.TA"];
const COMPANY_NAMES: Record<string, string> = {
  AAPL: "Apple",
  MSFT: "Microsoft",
  GOOGL: "Alphabet",
  META: "Meta Platforms",
  AMZN: "Amazon",
  NVDA: "Nvidia",
  TSLA: "Tesla",
  MSTR: "MicroStrategy",
  COIN: "Coinbase",
  "ASML.AS": "ASML Holding",
  "SAP.DE": "SAP",
  "005930.KS": "Samsung Electronics",
  "6758.T": "Sony Group",
  "SHOP.TO": "Shopify",
  "ADYEN.AS": "Adyen",
  "NOKIA.HE": "Nokia",
  "0700.HK": "Tencent",
  "9988.HK": "Alibaba",
  XOM: "Exxon Mobil",
  CVX: "Chevron",
  COP: "ConocoPhillips",
  OXY: "Occidental",
  SLB: "SLB",
  EOG: "EOG Resources",
  MPC: "Marathon Petroleum",
  PSX: "Phillips 66",
  VLO: "Valero",
  SHEL: "Shell",
  TTE: "TotalEnergies",
  BP: "BP",
  EQNR: "Equinor",
  PBR: "Petrobras",
  ENB: "Enbridge",
  "SU.TO": "Suncor Energy",
  "CNQ.TO": "Canadian Natural Resources",
  "REP.MC": "Repsol",
  JPM: "JPMorgan Chase",
  BAC: "Bank of America",
  WFC: "Wells Fargo",
  C: "Citigroup",
  GS: "Goldman Sachs",
  MS: "Morgan Stanley",
  BLK: "BlackRock",
  SCHW: "Charles Schwab",
  AXP: "American Express",
  "HSBA.L": "HSBC",
  "UBSG.SW": "UBS",
  "BNP.PA": "BNP Paribas",
  "DBK.DE": "Deutsche Bank",
  "INGA.AS": "ING Group",
  "8058.T": "Mitsubishi UFJ Financial",
  "SAN.MC": "Banco Santander",
  "BARC.L": "Barclays",
  "ZURN.SW": "Zurich Insurance",
  CAT: "Caterpillar",
  DE: "Deere",
  BA: "Boeing",
  GE: "GE",
  HON: "Honeywell",
  UNP: "Union Pacific",
  UPS: "UPS",
  LMT: "Lockheed Martin",
  RTX: "RTX",
  "SIE.DE": "Siemens",
  "AIR.PA": "Airbus",
  "DPW.DE": "DHL Group",
  "VOLV-B.ST": "Volvo",
  "7203.T": "Toyota",
  "7267.T": "Honda",
  "CP.TO": "Canadian Pacific Kansas City",
  "6501.T": "Hitachi",
  "SGRO.L": "SEGRO",
  LIN: "Linde",
  APD: "Air Products",
  SHW: "Sherwin-Williams",
  ECL: "Ecolab",
  DD: "DuPont",
  DOW: "Dow",
  NUE: "Nucor",
  FCX: "Freeport-McMoRan",
  NEM: "Newmont",
  "BHP.AX": "BHP Group",
  "RIO.AX": "Rio Tinto",
  "GLEN.L": "Glencore",
  "ANTO.L": "Antofagasta",
  "BAS.DE": "BASF",
  "SIKA.SW": "Sika",
  "AEM.TO": "Agnico Eagle Mines",
  "NTR.TO": "Nutrien",
  "IVN.AX": "Ivanhoe",
  NOC: "Northrop Grumman",
  GD: "General Dynamics",
  LHX: "L3Harris",
  HII: "Huntington Ingalls",
  TDG: "TransDigm",
  AVAV: "AeroVironment",
  KTOS: "Kratos Defense",
  "BA.L": "BAE Systems",
  "RHM.DE": "Rheinmetall",
  "HO.PA": "Thales",
  "LDO.MI": "Leonardo",
  "SAAB-B.ST": "Saab",
  "SAF.PA": "Safran",
  "HAG.DE": "Hensoldt",
  "AM.PA": "Dassault Aviation",
  "ASELS.IS": "Aselsan",
  "OTKAR.IS": "Otokar",
  "SDTTR.IS": "SDT Space & Defense",
  "ALTNY.IS": "Altinay Savunma",
  "ONRYT.IS": "Onur Yatirim",
  "PAPIL.IS": "Papilon Savunma",
  "PATEK.IS": "Patek",
  "KATMR.IS": "Katmerciler",
  "TMSN.IS": "Tumosan",
  CHKP: "Check Point Software",
  CYBR: "CyberArk",
  NICE: "NICE",
  ESLT: "Elbit Systems",
  "IAI.TA": "Israel Aerospace Industries",
  "ESLT.TA": "Elbit Systems",
  "NICE.TA": "NICE",
  "MGDL.TA": "Migdal Insurance",
  "FIBI.TA": "First International Bank of Israel",
};
const METRIC_LABEL_CLASS = "text-xs font-semibold uppercase tracking-wide text-black/60";
const METRIC_VALUE_CLASS = "text-base font-semibold text-black tabular-nums";
const METRIC_HELPER_CLASS = "text-xs text-black/50";
const STALE_MS = 60_000;

function Card({
  title,
  right,
  children,
  className,
}: {
  title?: React.ReactNode;
  right?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cx(
        "rounded-3xl border border-black/10 bg-[var(--panel)] p-5 shadow-[0_18px_40px_-30px_rgba(15,25,26,0.5)] backdrop-blur",
        className
      )}
    >
      {(title || right) && (
        <div className="mb-4 flex items-start justify-between gap-3">
          <div className="text-sm font-semibold text-[color:var(--ink)]">{title}</div>
          {right}
        </div>
      )}
      {children}
    </div>
  );
}

function Badge({
  children,
  tone = "neutral",
  className,
}: {
  children: React.ReactNode;
  tone?: "neutral" | "good" | "bad" | "warn" | "info" | "signal";
  className?: string;
}) {
  const tones: Record<string, string> = {
    neutral: "bg-black/5 text-black/70 border-black/10",
    good: "bg-emerald-100/80 text-emerald-800 border-emerald-200",
    bad: "bg-rose-100/80 text-rose-800 border-rose-200",
    warn: "bg-amber-100/80 text-amber-800 border-amber-200",
    info: "bg-sky-100/80 text-sky-800 border-sky-200",
    signal: "bg-amber-200/80 text-amber-900 border-amber-300",
  };
  return (
    <span className={cx("inline-flex items-center gap-1 rounded-full border px-2 py-1 text-xs", tones[tone], className)}>
      {children}
    </span>
  );
}

function InfoTip({ lines }: { lines: string[] }) {
  if (!lines.length) return null;
  return (
    <span className="group relative inline-flex items-center">
      <Info className="h-3.5 w-3.5 text-black/40" />
      <span className="pointer-events-none absolute right-0 top-5 z-10 w-60 rounded-xl border border-black/10 bg-[var(--panel-strong)] px-2 py-1 text-[11px] text-[color:var(--ink)] opacity-0 shadow transition group-hover:opacity-100">
        {lines.map((line) => (
          <div key={line}>{line}</div>
        ))}
      </span>
    </span>
  );
}

function Button({
  children,
  onClick,
  disabled,
  variant = "primary",
  className,
}: {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  variant?: "primary" | "ghost";
  className?: string;
}) {
  const base = "inline-flex items-center justify-center rounded-2xl border px-3 py-2 text-sm font-medium transition disabled:opacity-50";
  const styles =
    variant === "primary"
      ? "bg-[var(--accent)] text-white border-[var(--accent)] hover:brightness-110"
      : "bg-white/70 text-black/70 border-black/10 hover:bg-white";
  return (
    <button onClick={onClick} disabled={disabled} className={cx(base, styles, className)}>
      {children}
    </button>
  );
}

function Input({
  value,
  onChange,
  placeholder,
  className,
}: {
  value: string;
  onChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  placeholder?: string;
  className?: string;
}) {
  return (
    <input
      value={value}
      onChange={onChange}
      placeholder={placeholder}
      className={cx(
        "w-full rounded-2xl border border-black/10 bg-white/80 px-4 py-2 text-sm text-black shadow-sm outline-none focus:ring-2 focus:ring-[var(--accent)]",
        className
      )}
    />
  );
}

function Delta({ value }: { value?: number | null }) {
  return <ChangePill value={value ?? null} className="min-w-[64px] justify-center" />;
}

function FlipNumber({ value, className }: { value: React.ReactNode; className?: string }) {
  const key = typeof value === "string" || typeof value === "number" ? value : undefined;
  return (
    <motion.span
      key={key}
      initial={{ y: 6, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      transition={{ duration: 0.25, ease: "easeOut" }}
      className={className}
    >
      {value}
    </motion.span>
  );
}

function Sparkline({
  points,
  tone,
  stale,
}: {
  points: number[];
  tone?: "up" | "down" | "neutral";
  stale?: boolean;
}) {
  if (!points || points.length < 2) {
    return <div className="h-6 w-16 rounded-full border border-black/10 bg-black/5" />;
  }
  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = max - min || 1;
  const width = 64;
  const height = 24;
  const step = width / (points.length - 1);
  const path = points
    .map((v, i) => {
      const x = i * step;
      const y = height - ((v - min) / range) * height;
      return `${i === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
  const color =
    tone === "up" ? "#22c55e" : tone === "down" ? "#f43f5e" : "var(--muted)";
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} className={cx(stale && "opacity-50")}>
      <path d={path} fill="none" stroke={color} strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}

function InsightIndicator({
  summary,
  tone,
}: {
  summary?: string | null;
  tone?: "up" | "down" | "neutral";
}) {
  if (!summary) {
    return (
      <span className="inline-flex h-6 w-6 items-center justify-center rounded-full border border-black/10 bg-black/5 text-[10px] text-black/40">
        <Sparkles className="h-3.5 w-3.5" />
      </span>
    );
  }
  const color =
    tone === "up" ? "bg-emerald-500/15 text-emerald-700 border-emerald-300/60" :
    tone === "down" ? "bg-rose-500/15 text-rose-700 border-rose-300/60" :
    "bg-amber-500/15 text-amber-700 border-amber-300/60";
  return (
    <span className={cx("group relative inline-flex h-6 w-6 items-center justify-center rounded-full border", color)}>
      <Sparkles className="h-3.5 w-3.5" />
      <span className="pointer-events-none absolute right-0 top-7 z-10 w-56 rounded-xl border border-black/10 bg-[var(--panel-strong)] p-2 text-[11px] text-[color:var(--ink)] opacity-0 shadow transition group-hover:opacity-100">
        {summary}
      </span>
    </span>
  );
}

function MetricRow({
  label,
  value,
  delta,
  showDelta = true,
  className,
  sparkline,
  stale,
  insight,
  insightTone,
  labelClassName,
  valueClassName,
}: {
  label: React.ReactNode;
  value: React.ReactNode;
  delta?: number | null;
  showDelta?: boolean;
  className?: string;
  sparkline?: number[];
  stale?: boolean;
  insight?: string | null;
  insightTone?: "up" | "down" | "neutral";
  labelClassName?: string;
  valueClassName?: string;
}) {
  return (
    <div className={cx("flex items-center justify-between gap-3", className)}>
      <div className="flex flex-col">
        <span className={cx(METRIC_LABEL_CLASS, labelClassName, stale && "text-black/35")}>{label}</span>
        <FlipNumber value={value} className={cx(METRIC_VALUE_CLASS, valueClassName, stale && "text-black/35")} />
      </div>
      <div className="flex items-center gap-2">
        {sparkline ? (
          <Sparkline
            points={sparkline}
            tone={delta == null ? "neutral" : delta >= 0 ? "up" : "down"}
            stale={stale}
          />
        ) : null}
        {showDelta && !sparkline ? <Delta value={delta ?? null} /> : null}
        <InsightIndicator summary={insight ?? undefined} tone={insightTone} />
      </div>
    </div>
  );
}

function ChangePill({ value, className }: { value?: number | null; className?: string }) {
  return (
    <span
      className={cx(
        "inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold tabular-nums",
        value == null
          ? "border-black/10 text-black/40"
          : value >= 0
            ? "border-emerald-300/60 bg-emerald-500/10 text-emerald-700"
            : "border-rose-300/60 bg-rose-500/10 text-rose-700",
        className
      )}
    >
      {value == null ? "n/a" : formatPercent(value)}
    </span>
  );
}

function StatusPill({ ok }: { ok?: boolean }) {
  const tone = ok ? "text-emerald-700 border-emerald-300/60 bg-emerald-500/10"
    : "text-rose-700 border-rose-300/60 bg-rose-500/10";
  return (
    <span className={cx("rounded-full border px-2 py-0.5 text-[10px] font-semibold", tone)}>
      {ok ? "✓" : "✕"}
    </span>
  );
}

type HeatmapPage = {
  label: string;
  tickers: string[];
};

type ServiceHealth = {
  ok?: boolean;
  tsISO?: string;
  service?: string;
  version?: string;
  env?: Record<string, boolean>;
  features?: Record<string, boolean>;
  deps?: string[];
  deps_status?: Record<string, { ok?: boolean; error?: string }>;
  data_missing?: string[];
};

function HeatmapCard({
  sectorName,
  pages,
  liveQuotes,
  isQuoteStale,
  debug,
  className,
}: {
  sectorName: string;
  pages: HeatmapPage[];
  liveQuotes: Record<string, { price?: number; change_pct?: number; updated_iso?: string }> | null;
  isQuoteStale: (symbol: string) => boolean;
  debug?: DebugInfo;
  className?: string;
}) {
  const [pageIndex, setPageIndex] = useState(0);
  const page = pages[pageIndex] ?? pages[0];
  const totalPages = pages.length;
  const label = page?.label ?? "US";
  const tickers = page?.tickers ?? [];
  const avgMove = useMemo(() => {
    const changes = tickers
      .map((symbol) => liveQuotes?.[symbol]?.change_pct)
      .filter((v) => Number.isFinite(v)) as number[];
    if (!changes.length) return 0;
    return changes.reduce((acc, v) => acc + Math.abs(v), 0) / changes.length;
  }, [liveQuotes, tickers]);
  const hot = avgMove >= 1.5;
  const glowClass = hot ? "ring-1 ring-emerald-300/60 shadow-[0_0_32px_rgba(34,197,94,0.18)]" : "";

  return (
    <Card
      title={`Sector Heatmap (${sectorName}) — ${label}`}
      right={
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1 rounded-full border border-black/10 bg-white/70 px-1 py-0.5 text-[11px] text-black/60">
            <button
              type="button"
              onClick={() => setPageIndex((prev) => (prev - 1 + totalPages) % totalPages)}
              className="rounded-full p-1 hover:bg-black/5"
              aria-label="Previous heatmap page"
            >
              <ChevronLeft className="h-3.5 w-3.5" />
            </button>
            <span className="px-1 tabular-nums">
              {pageIndex + 1}/{totalPages}
            </span>
            <button
              type="button"
              onClick={() => setPageIndex((prev) => (prev + 1) % totalPages)}
              className="rounded-full p-1 hover:bg-black/5"
              aria-label="Next heatmap page"
            >
              <ChevronRight className="h-3.5 w-3.5" />
            </button>
          </div>
          <MissingChips debug={debug} keys={["yahoo"]} />
        </div>
      }
      className={cx("h-full", glowClass, className)}
    >
      <div className="grid grid-cols-3 gap-2">
        {tickers.map((symbol) => {
          const quote = liveQuotes?.[symbol];
          const companyName = COMPANY_NAMES[symbol] ?? symbol;
          const hasPrice = Number.isFinite(quote?.price);
          const hasChange = Number.isFinite(quote?.change_pct);
          const hasData = hasPrice || hasChange;
          const change = hasChange ? (quote?.change_pct as number) : 0;
          const intensity = hasData ? Math.min(1, Math.abs(change) / 3) : 0;
          const stale = isQuoteStale(symbol);
          const bg =
            !hasData
              ? "rgba(0,0,0,0.03)"
              : change >= 0
              ? `rgba(34,197,94,${0.12 + intensity * 0.35})`
              : `rgba(244,63,94,${0.12 + intensity * 0.35})`;
          if (!hasData) {
            return (
              <div
                key={symbol}
                className="flex flex-col justify-between rounded-2xl border border-black/10 p-2"
                style={{ backgroundColor: bg }}
                title={companyName}
              >
                <div className="text-[11px] font-semibold text-black/70">{symbol}</div>
                <div className="mt-2 text-[11px] text-black/40">Data unavailable</div>
              </div>
            );
          }
          return (
            <div
              key={symbol}
              className="group relative flex flex-col justify-between rounded-2xl border border-black/10 p-2"
              style={{ backgroundColor: bg }}
              title={companyName}
            >
              <div
                className={cx(
                  "flex items-center justify-between text-[11px] font-semibold text-black/70",
                  stale && "text-black/35"
                )}
              >
                <span>{symbol}</span>
                <span className="tabular-nums">{hasChange ? formatPercent(change, 2) : "—"}</span>
              </div>
              <div className={cx("mt-2 text-sm font-semibold text-black tabular-nums", stale && "text-black/35")}>
                {hasPrice ? formatNumber(quote?.price, 2) : "—"}
              </div>
              <div className="pointer-events-none absolute left-2 top-full z-10 mt-2 w-48 rounded-xl border border-black/10 bg-[var(--panel-strong)] p-2 text-[11px] text-[color:var(--ink)] opacity-0 shadow transition group-hover:opacity-100">
                <div className="font-semibold">{companyName}</div>
                <div className="text-black/60">
                  {symbol} • {hasChange ? formatPercent(change, 2) : "Data unavailable"}
                </div>
              </div>
            </div>
          );
        })}
      </div>
      <div className={cx(METRIC_HELPER_CLASS, "mt-2")}>
        Sector momentum avg: {formatPercent(avgMove, 2)}
      </div>
    </Card>
  );
}

function calcCorrelation(a: number[], b: number[]) {
  const n = Math.min(a.length, b.length);
  if (n < 5) return 0;
  const sliceA = a.slice(-n);
  const sliceB = b.slice(-n);
  const meanA = sliceA.reduce((acc, v) => acc + v, 0) / n;
  const meanB = sliceB.reduce((acc, v) => acc + v, 0) / n;
  let num = 0;
  let denA = 0;
  let denB = 0;
  for (let i = 0; i < n; i += 1) {
    const da = sliceA[i] - meanA;
    const db = sliceB[i] - meanB;
    num += da * db;
    denA += da * da;
    denB += db * db;
  }
  if (denA === 0 || denB === 0) return 0;
  return num / Math.sqrt(denA * denB);
}

function EventSparkline({ data, color }: { data?: number[]; color: string }) {
  if (!data || data.length === 0) return null;
  const max = Math.max(...data.map((v) => Math.abs(v)), 1);
  const points = data
    .map((v, i) => {
      const x = (i / Math.max(1, data.length - 1)) * 100;
      const y = 50 - (v / max) * 40;
      return `${x},${y}`;
    })
    .join(" ");
  return (
    <svg viewBox="0 0 100 60" className="h-6 w-20">
      <polyline fill="none" stroke={color} strokeWidth="3" points={points} />
      <line x1="0" x2="100" y1="50" y2="50" stroke="rgba(0,0,0,0.08)" />
    </svg>
  );
}

function EventStudyChart({ points }: { points: EventPoint[] }) {
  const [cryptoAsset, setCryptoAsset] = useState<"BTC" | "ETH" | "INDEX">("BTC");
  const [nasdaqSource, setNasdaqSource] = useState<"NQ" | "QQQ" | "NDX">("NQ");
  const [primaryWindow, setPrimaryWindow] = useState<"15m" | "30m" | "1h" | "4h" | "24h">("1h");
  const [selected, setSelected] = useState<EventPoint | null>(null);

  if (!points || points.length === 0) {
    return <div className="text-sm text-black/50">Event-study verisi yok.</div>;
  }

  const getReaction = (p: EventPoint, key: string) => (p.reactions ? p.reactions[key] : undefined);
  const getCryptoKey = (p: EventPoint) => {
    if (cryptoAsset === "ETH" && getReaction(p, "ETH")) return "ETH";
    if (cryptoAsset === "INDEX") return getReaction(p, "BTC") ? "BTC" : "ETH";
    return getReaction(p, "BTC") ? "BTC" : "ETH";
  };
  const getNasdaqKey = (p: EventPoint) => {
    if (nasdaqSource === "NQ" && getReaction(p, "NQ")) return "NQ";
    if (nasdaqSource === "QQQ" && getReaction(p, "QQQ")) return "QQQ";
    if (nasdaqSource === "NDX" && getReaction(p, "NDX")) return "NDX";
    return getReaction(p, "NQ") ? "NQ" : getReaction(p, "QQQ") ? "QQQ" : "NDX";
  };
  const reactionZ = (r: any, window: string) =>
    window === "30m" ? r?.around?.["30m"]?.z ?? 0 : r?.post?.[window]?.z ?? 0;
  const reactionRet = (r: any, window: string) =>
    window === "30m" ? r?.around?.["30m"]?.ret ?? 0 : r?.post?.[window]?.ret ?? 0;

  const modeColor = (mode?: string) => {
    if (mode === "BOTH_UP") return "bg-emerald-500";
    if (mode === "BOTH_DOWN") return "bg-rose-500";
    if (mode === "DIVERGENCE") return "bg-purple-500";
    return "bg-orange-400";
  };

  return (
    <div className="relative rounded-2xl border border-black/10 bg-white/60 p-4">
      <div className="mb-3 flex flex-wrap items-center gap-2 text-xs text-black/60">
        <span>Crypto:</span>
        {(["BTC", "ETH", "INDEX"] as const).map((v) => (
          <button
            key={v}
            onClick={() => setCryptoAsset(v)}
            className={cx(
              "rounded-full border px-2 py-0.5",
              cryptoAsset === v ? "border-black/60 bg-black/5 text-black" : "border-black/10"
            )}
          >
            {v}
          </button>
        ))}
        <span className="ml-2">Nasdaq:</span>
        {(["NQ", "QQQ", "NDX"] as const).map((v) => (
          <button
            key={v}
            onClick={() => setNasdaqSource(v)}
            className={cx(
              "rounded-full border px-2 py-0.5",
              nasdaqSource === v ? "border-black/60 bg-black/5 text-black" : "border-black/10"
            )}
          >
            {v}
          </button>
        ))}
        <span className="ml-2">Primary window:</span>
        {(["15m", "30m", "1h", "4h", "24h"] as const).map((v) => (
          <button
            key={v}
            onClick={() => setPrimaryWindow(v)}
            className={cx(
              "rounded-full border px-2 py-0.5",
              primaryWindow === v ? "border-black/60 bg-black/5 text-black" : "border-black/10"
            )}
          >
            {v}
          </button>
        ))}
      </div>
      <div className="mb-3 flex flex-wrap items-center gap-3 text-[11px] text-black/55">
        <span className="font-semibold text-black/70">Legend</span>
        <span className="inline-flex items-center gap-1">
          <span className="h-2 w-2 rounded-full bg-emerald-500" /> BOTH_UP
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="h-2 w-2 rounded-full bg-rose-500" /> BOTH_DOWN
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="h-2 w-2 rounded-full bg-purple-500" /> DIVERGENCE
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="h-2 w-2 rounded-full bg-orange-400" /> LOW_SIGNAL
        </span>
        <span className="ml-2 text-black/50">
          Size scales with max |z| (BTC/NQ): r = 6px + min(10px, max|z| × 2.4)
        </span>
      </div>
      <div className="relative h-36">
        <div className="absolute inset-x-2 top-1/2 h-px bg-black/10" />
        {points.map((p, idx) => {
          const left = ((idx + 1) / (points.length + 1)) * 100;
          const cryptoKey = getCryptoKey(p);
          const nasdaqKey = getNasdaqKey(p);
          const cryptoReaction = cryptoKey ? getReaction(p, cryptoKey) : null;
          const nasdaqReaction = nasdaqKey ? getReaction(p, nasdaqKey) : null;
          const z1 = reactionZ(cryptoReaction, primaryWindow);
          const z2 = reactionZ(nasdaqReaction, primaryWindow);
          const size = 6 + Math.min(10, Math.max(Math.abs(z1), Math.abs(z2)) * 2.4);
          const height = Math.min(80, Math.max(20, 40 + (cryptoReaction?.volume_z ?? 0) * 8));
          const mode = p.combined?.mode ?? "LOW_SIGNAL";
          return (
            <div key={`${p.id}-${idx}`} className="absolute bottom-2" style={{ left: `${left}%` }}>
              <div className="group relative flex flex-col items-center">
                <div className="w-px bg-black/20" style={{ height }} />
                <button
                  type="button"
                  onClick={() => setSelected(p)}
                  className={cx("rounded-full shadow", modeColor(mode))}
                  style={{ width: size, height: size }}
                />
                <div className="pointer-events-none absolute bottom-8 w-72 -translate-x-1/2 rounded-xl border border-black/10 bg-white p-3 text-xs text-black/80 opacity-0 shadow-lg transition group-hover:opacity-100">
                  <div className="mb-1 font-semibold text-black">{p.headline ?? p.title}</div>
                  <div className="mb-2 text-black/60">TSİ: {p.published_at_tsi ?? "—"}</div>
                  <div className="mb-2 grid grid-cols-2 gap-2">
                    <div>
                      <div className="font-semibold text-black">{cryptoKey ?? "BTC"}</div>
                      <div>pre: {fmtNum(cryptoReaction?.pre?.ret ?? 0, 2)}% z {fmtNum(cryptoReaction?.pre?.z ?? 0, 2)}</div>
                      <div>15m: {fmtNum(reactionRet(cryptoReaction, "15m"), 2)}% z {fmtNum(reactionZ(cryptoReaction, "15m"), 2)}</div>
                      <div>
                        ±30m:{" "}
                        {cryptoReaction?.around?.["30m"]?.ret == null ? (
                          <span className="text-black/50">missing (no candles)</span>
                        ) : (
                          <>
                            {fmtNum(cryptoReaction?.around?.["30m"]?.ret ?? 0, 2)}% z{" "}
                            {fmtNum(cryptoReaction?.around?.["30m"]?.z ?? 0, 2)}
                          </>
                        )}
                      </div>
                      <div>
                        -30→0:{" "}
                        {cryptoReaction?.pre_30m_ret == null ? (
                          <span className="text-black/50">missing</span>
                        ) : (
                          <>{fmtNum(cryptoReaction?.pre_30m_ret ?? 0, 2)}%</>
                        )}
                        {"  "}0→+30:{" "}
                        {cryptoReaction?.post_30m_ret == null ? (
                          <span className="text-black/50">missing</span>
                        ) : (
                          <>{fmtNum(cryptoReaction?.post_30m_ret ?? 0, 2)}%</>
                        )}
                      </div>
                      <div>1h: {fmtNum(reactionRet(cryptoReaction, "1h"), 2)}% z {fmtNum(reactionZ(cryptoReaction, "1h"), 2)}</div>
                      <div>4h: {fmtNum(reactionRet(cryptoReaction, "4h"), 2)}% z {fmtNum(reactionZ(cryptoReaction, "4h"), 2)}</div>
                      <div>24h: {fmtNum(reactionRet(cryptoReaction, "24h"), 2)}% z {fmtNum(reactionZ(cryptoReaction, "24h"), 2)}</div>
                      <div className="mt-1 flex items-center gap-2">
                        <EventSparkline data={cryptoReaction?.spark_pre} color="#0ea5e9" />
                        <EventSparkline data={cryptoReaction?.spark_post} color="#10b981" />
                      </div>
                    </div>
                    <div>
                      <div className="font-semibold text-black">{nasdaqKey ?? "NQ"}</div>
                      <div>pre: {fmtNum(nasdaqReaction?.pre?.ret ?? 0, 2)}% z {fmtNum(nasdaqReaction?.pre?.z ?? 0, 2)}</div>
                      <div>15m: {fmtNum(reactionRet(nasdaqReaction, "15m"), 2)}% z {fmtNum(reactionZ(nasdaqReaction, "15m"), 2)}</div>
                      <div>
                        ±30m:{" "}
                        {nasdaqReaction?.around?.["30m"]?.ret == null ? (
                          <span className="text-black/50">missing (no candles)</span>
                        ) : (
                          <>
                            {fmtNum(nasdaqReaction?.around?.["30m"]?.ret ?? 0, 2)}% z{" "}
                            {fmtNum(nasdaqReaction?.around?.["30m"]?.z ?? 0, 2)}
                          </>
                        )}
                      </div>
                      <div>
                        -30→0:{" "}
                        {nasdaqReaction?.pre_30m_ret == null ? (
                          <span className="text-black/50">missing</span>
                        ) : (
                          <>{fmtNum(nasdaqReaction?.pre_30m_ret ?? 0, 2)}%</>
                        )}
                        {"  "}0→+30:{" "}
                        {nasdaqReaction?.post_30m_ret == null ? (
                          <span className="text-black/50">missing</span>
                        ) : (
                          <>{fmtNum(nasdaqReaction?.post_30m_ret ?? 0, 2)}%</>
                        )}
                      </div>
                      <div>1h: {fmtNum(reactionRet(nasdaqReaction, "1h"), 2)}% z {fmtNum(reactionZ(nasdaqReaction, "1h"), 2)}</div>
                      <div>4h: {fmtNum(reactionRet(nasdaqReaction, "4h"), 2)}% z {fmtNum(reactionZ(nasdaqReaction, "4h"), 2)}</div>
                      <div>24h: {fmtNum(reactionRet(nasdaqReaction, "24h"), 2)}% z {fmtNum(reactionZ(nasdaqReaction, "24h"), 2)}</div>
                      <div className="mt-1 flex items-center gap-2">
                        <EventSparkline data={nasdaqReaction?.spark_pre} color="#8b5cf6" />
                        <EventSparkline data={nasdaqReaction?.spark_post} color="#f97316" />
                      </div>
                    </div>
                  </div>
                  <div className="text-black/60">mode: {mode}</div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
      {selected && (
        <div className="absolute inset-0 z-20 flex items-center justify-center bg-black/30 p-6">
          <div className="w-full max-w-3xl rounded-2xl bg-white p-4 shadow-xl">
            <div className="mb-2 flex items-center justify-between">
              <div className="text-sm font-semibold text-black">{selected.headline ?? selected.title}</div>
              <button
                type="button"
                onClick={() => setSelected(null)}
                className="rounded-md border border-black/10 px-2 py-1 text-xs text-black/60"
              >
                Close
              </button>
            </div>
            <OverlayEventChart
              point={selected}
              cryptoKey={getCryptoKey(selected)}
              nasdaqKey={getNasdaqKey(selected)}
            />
          </div>
        </div>
      )}
    </div>
  );
}

function OverlayEventChart({
  point,
  cryptoKey,
  nasdaqKey,
}: {
  point: EventPoint;
  cryptoKey: string | null;
  nasdaqKey: string | null;
}) {
  const crypto = cryptoKey && point.reactions ? point.reactions[cryptoKey] : null;
  const nasdaq = nasdaqKey && point.reactions ? point.reactions[nasdaqKey] : null;
  const series = (r?: typeof crypto) => {
    const pre = r?.spark_pre ?? [];
    const post = r?.spark_post ?? [];
    return [...pre.map((v) => v), 0, ...post.map((v) => v)];
  };
  const s1 = series(crypto);
  const s2 = series(nasdaq);
  const max = Math.max(1, ...s1.map((v) => Math.abs(v)), ...s2.map((v) => Math.abs(v)));
  const points = (data: number[]) =>
    data
      .map((v, i) => {
        const x = (i / Math.max(1, data.length - 1)) * 100;
        const y = 50 - (v / max) * 40;
        return `${x},${y}`;
      })
      .join(" ");
  return (
    <div className="rounded-xl border border-black/10 bg-white/80 p-3">
      <div className="mb-2 text-xs text-black/60">
        TSİ: {point.published_at_tsi ?? "—"} • Scope: {point.scope ?? "—"} • Sectors:{" "}
        {(point.sectors ?? []).join(", ") || "—"}
      </div>
      <svg viewBox="0 0 100 60" className="h-32 w-full">
        <line x1="0" x2="100" y1="50" y2="50" stroke="rgba(0,0,0,0.08)" />
        <line x1="50" x2="50" y1="5" y2="55" stroke="rgba(0,0,0,0.15)" strokeDasharray="2 2" />
        <polyline fill="none" stroke="#0ea5e9" strokeWidth="2.5" points={points(s1)} />
        <polyline fill="none" stroke="#8b5cf6" strokeWidth="2.5" points={points(s2)} />
      </svg>
      <div className="mt-2 flex items-center justify-between text-xs text-black/60">
        <span>{cryptoKey ?? "BTC"} (blue)</span>
        <span>{nasdaqKey ?? "NQ"} (purple)</span>
      </div>
    </div>
  );
}

function DebugRow({ debug }: { debug?: DebugInfo }) {
  if (!debug) return null;
  return (
    <div className="flex flex-wrap gap-2 text-xs text-black/50">
      {debug.data_missing.length > 0 && (
        <Badge tone="warn">data_missing: {debug.data_missing.join(", ")}</Badge>
      )}
      {debug.notes.slice(0, 3).map((n, i) => (
        <Badge key={`${n}-${i}`} className="bg-white/70 text-black/60">
          {n}
        </Badge>
      ))}
    </div>
  );
}

function formatUsdValue(value?: number | null, digits = 0) {
  const formatted = formatNumber(value, digits);
  if (formatted === "Data unavailable") return formatted;
  return `$${formatted}`;
}

function MissingChips({ debug, keys }: { debug?: DebugInfo; keys: string[] }) {
  const missing = debug?.data_missing ?? [];
  const labels = keys.filter((k) => missing.includes(k));
  if (labels.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-1">
      {labels.map((k) => (
        <Badge key={k} tone="warn">
          DATA_MISSING_{k.toUpperCase()}
        </Badge>
      ))}
    </div>
  );
}

function directionMeta(dir: "UP" | "DOWN" | "NEUTRAL") {
  if (dir === "UP") {
    return { label: "Yukari", tone: "good" as const, icon: TrendingUp };
  }
  if (dir === "DOWN") {
    return { label: "Asagi", tone: "bad" as const, icon: TrendingDown };
  }
  return { label: "Notr", tone: "neutral" as const, icon: Activity };
}

function biasTone(value: number) {
  if (value > 0) return "good";
  if (value < 0) return "bad";
  return "neutral";
}

function getAssetClass(symbolOrTicker: string, holding?: any): AssetClass {
  const venue = holding?.venue ?? holding?.asset_class;
  if (venue === "BIST") return "BIST";
  if (venue === "NASDAQ") return "NASDAQ";
  if (venue === "CRYPTO") return "CRYPTO";
  const symbol = symbolOrTicker.toUpperCase();
  if (symbol.endsWith(".IS") || PORTFOLIO_BIST_SYMBOLS.has(symbol)) return "BIST";
  if (PORTFOLIO_NASDAQ_SYMBOLS.has(symbol)) return "NASDAQ";
  if (PORTFOLIO_CRYPTO_SYMBOLS.has(symbol) || symbol.includes("-USD")) return "CRYPTO";
  return "NASDAQ";
}

function assetClassTone(cls: AssetClass) {
  if (cls === "BIST") return "info";
  if (cls === "CRYPTO") return "signal";
  return "neutral";
}

function formatSigned(value: number) {
  if (!Number.isFinite(value)) return "—";
  const s = value >= 0 ? "+" : "";
  return `${s}${value.toFixed(3)}`;
}

function confidenceTone(value: number) {
  if (value >= 75) return "good";
  if (value >= 50) return "info";
  return "warn";
}

function applyIntelDiff(prev: IntelResponse | null, next: IntelResponse): IntelResponse {
  if (!prev) return next;
  const changed = next.changed_blocks ?? [];
  if (!next.block_hashes) {
    return next;
  }
  if (changed.length === 0) {
    return {
      ...prev,
      tsISO: next.tsISO,
      timeframe: next.timeframe,
      newsTimespan: next.newsTimespan,
      etag: next.etag,
      block_hashes: next.block_hashes,
      changed_blocks: next.changed_blocks,
    };
  }
  const map: Record<string, keyof IntelResponse> = {
    market: "market",
    leaders: "leaders",
    top_news: "top_news",
    eventfeed: "event_feed",
    flow: "flow",
    risk: "risk",
    derivatives: "derivatives",
    forecast: "forecast",
    daily_equity_movers: "daily_equity_movers",
    debug: "debug",
  };
  const merged: IntelResponse = { ...prev };
  for (const key of changed) {
    const field = map[key];
    if (field) {
      merged[field] = next[field];
    }
  }
  merged.tsISO = next.tsISO;
  merged.timeframe = next.timeframe;
  merged.newsTimespan = next.newsTimespan;
  merged.etag = next.etag;
  merged.block_hashes = next.block_hashes;
  merged.changed_blocks = next.changed_blocks;
  return merged;
}

function normalizeWatchlist(raw: string) {
  return raw
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean)
    .join(",");
}

export default function Dashboard() {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const [intel, setIntel] = useState<IntelResponse | null>(null);
  const [newsTimespan, setNewsTimespan] = useState("6h");
  const [timeframe, setTimeframe] = useState("1h");
  const [watchInput, setWatchInput] = useState("");
  const [watch, setWatch] = useState("");
  const [derivExchange, setDerivExchange] = useState("binance");
  const [derivData, setDerivData] = useState<DerivativesResponse | null>(null);
  const [derivLoading, setDerivLoading] = useState(false);
  const [forecastTarget, setForecastTarget] = useState("BTC");
  const [forecast, setForecast] = useState<ForecastView | null>(null);
  const [metrics, setMetrics] = useState<ForecastMetrics[]>([]);
  const [eventClusters, setEventClusters] = useState<EventClusterResponse | null>(null);
  const [impactMin, setImpactMin] = useState(20);
  const [tierFilter, setTierFilter] = useState("all");
  const [tagFilter, setTagFilter] = useState("");
  const [dominanceLoading, setDominanceLoading] = useState(false);
  const [portfolio, setPortfolio] = useState<PortfolioResponse | null>(null);
  const [portfolioBase, setPortfolioBase] = useState<"TRY" | "USD">("TRY");
  const [portfolioHorizon, setPortfolioHorizon] = useState<"24h" | "7d" | "30d">("24h");
  const [portfolioLoading, setPortfolioLoading] = useState(false);
  const [portfolioPeriod, setPortfolioPeriod] = useState<"daily" | "weekly" | "monthly">("daily");
  const [portfolioClassFilter, setPortfolioClassFilter] = useState<"ALL" | AssetClass>("ALL");
  const [portfolioDirectOnly, setPortfolioDirectOnly] = useState(false);
  const [portfolioRightTab, setPortfolioRightTab] = useState<"insights" | "optimizer">("insights");
  const [debateInsights, setDebateInsights] = useState<DebateOutput | null>(null);
  const [debateMeta, setDebateMeta] = useState<DebateResponse | null>(null);
  const [debateLoading, setDebateLoading] = useState(false);
  const [debateError, setDebateError] = useState<string | null>(null);
  const [backendHealth, setBackendHealth] = useState<ServiceHealth | null>(null);
  const [analyticsHealth, setAnalyticsHealth] = useState<ServiceHealth | null>(null);
  const [liveMarket, setLiveMarket] = useState<MarketSnapshot | null>(null);
  const [liveRisk, setLiveRisk] = useState<RiskPanel | null>(null);
  const [liveSpot, setLiveSpot] = useState<{ btc?: number; eth?: number } | null>(null);
  const [liveSpotChange, setLiveSpotChange] = useState<{ btc?: number; eth?: number } | null>(null);
  const [liveSpotUpdated, setLiveSpotUpdated] = useState<{ btc?: string; eth?: string } | null>(null);
  const [liveQuotes, setLiveQuotes] = useState<
    Record<string, { price?: number; change_pct?: number; updated_iso?: string }> | null
  >(null);
  const [barsData, setBarsData] = useState<
    Record<string, { updated_iso?: string; points: Array<{ ts: string; close: number }> }>
  >({});
  const [nowTs, setNowTs] = useState(Date.now());
  const [darkMode, setDarkMode] = useState(false);
  const derivETagRef = useRef<string | null>(null);
  const derivLastModRef = useRef<string | null>(null);
  const dominanceRef = useRef<MarketSnapshot["coingecko"]["dominance"] | null>(null);

  const market = intel?.market as MarketSnapshot | undefined;
  const flow = intel?.flow as FlowPanel | undefined;
  const risk = intel?.risk as RiskPanel | undefined;
  const spotMarket = liveMarket ?? market;
  const macroMarket = liveMarket ?? market;
  const riskLive = liveRisk ?? risk;
  const dailyMovers = intel?.daily_equity_movers as DailyEquityMovers | undefined;
  const intelForecast = intel?.forecast as ForecastPanel | undefined;
  const cryptoOutlook = intelForecast?.crypto_outlook;
  const insights = buildRuleInsights(market, intel?.top_news ?? [], flow, risk, intel?.debug?.notes ?? []);
  const totalMcapUsd = market?.coingecko?.total_mcap_usd ?? null;
  const holdingsList = portfolio?.holdings ?? [];
  const fxLive = useMemo(() => {
    const live = liveQuotes?.["USDTRY=X"]?.price;
    return Number.isFinite(live) && (live ?? 0) > 0 ? (live as number) : (portfolio?.fx?.USDTRY ?? 0);
  }, [liveQuotes, portfolio?.fx?.USDTRY]);
  const displayHoldings = useMemo(() => {
    return holdingsList.map((h) => {
      const quote =
        liveQuotes?.[h.yahoo_symbol] ??
        liveQuotes?.[h.symbol] ??
        (PORTFOLIO_CRYPTO_SYMBOLS.has(h.symbol) ? liveQuotes?.[h.symbol] : null);
      const price =
        Number.isFinite(quote?.price) && (quote?.price ?? 0) > 0 ? (quote?.price as number) : (h.price ?? 0);
      const changePct = Number.isFinite(quote?.change_pct) ? (quote?.change_pct as number) : null;
      let value = h.mkt_value_base ?? 0;
      if (price > 0) {
        if (portfolioBase === "TRY" && h.currency === "USD") {
          value = fxLive ? price * h.qty * fxLive : 0;
        } else if (portfolioBase === "USD" && h.currency === "TRY") {
          value = fxLive ? (price * h.qty) / fxLive : 0;
        } else {
          value = price * h.qty;
        }
      }
      return {
        ...h,
        display_price: price,
        display_value: value,
        change_pct: changePct,
      };
    });
  }, [holdingsList, liveQuotes, fxLive, portfolioBase]);
  const portfolioTotal = displayHoldings.reduce((acc, h) => acc + (h.display_value ?? 0), 0);
  const holdingsSymbolsSet = useMemo(() => new Set(displayHoldings.map((h) => h.symbol)), [displayHoldings]);
  const symbolToHolding = useMemo(() => {
    const map: Record<string, (typeof displayHoldings)[number]> = {};
    displayHoldings.forEach((h) => {
      map[h.symbol] = h;
    });
    return map;
  }, [displayHoldings]);
  const holdingsByClass = useMemo(() => {
    const grouped: Record<AssetClass, (typeof displayHoldings)[number][]> = {
      BIST: [],
      NASDAQ: [],
      CRYPTO: [],
    };
    displayHoldings.forEach((h) => {
      const cls = getAssetClass(h.symbol, h);
      grouped[cls].push(h);
    });
    return grouped;
  }, [displayHoldings]);
  const holdingTotalsByClass = useMemo(() => {
    const totals: Record<AssetClass, number> = { BIST: 0, NASDAQ: 0, CRYPTO: 0 };
    (Object.keys(holdingsByClass) as AssetClass[]).forEach((cls) => {
      totals[cls] = holdingsByClass[cls].reduce((acc, h) => acc + (h.display_value ?? 0), 0);
    });
    return totals;
  }, [holdingsByClass]);
  const holdingChangeByClass = useMemo(() => {
    const totals: Record<AssetClass, number> = { BIST: 0, NASDAQ: 0, CRYPTO: 0 };
    const counts: Record<AssetClass, number> = { BIST: 0, NASDAQ: 0, CRYPTO: 0 };
    displayHoldings.forEach((h) => {
      const cls = getAssetClass(h.symbol, h);
      const value = h.display_value ?? 0;
      const pct = h.change_pct;
      if (!Number.isFinite(value) || value <= 0) return;
      if (!Number.isFinite(pct as number)) return;
      const change = (pct as number) / 100;
      if (change <= -0.9999) return;
      const delta = value * (change / (1 + change));
      totals[cls] += delta;
      counts[cls] += 1;
    });
    return { totals, counts };
  }, [displayHoldings]);
  const portfolioDelta = useMemo(() => {
    const totals = holdingChangeByClass.totals;
    return (totals.BIST ?? 0) + (totals.NASDAQ ?? 0) + (totals.CRYPTO ?? 0);
  }, [holdingChangeByClass]);
  const formatMoneyByClass = (cls: AssetClass, value: number) => {
    const decimals = cls === "BIST" ? 0 : 2;
    return formatNumber(value, decimals);
  };
  const formatQtyByClass = (cls: AssetClass, value: number) => {
    const decimals = cls === "BIST" ? 0 : 2;
    return formatNumber(value, decimals);
  };
  const portfolioHasValues = displayHoldings.some((h) => (h.display_value ?? 0) > 0);
  const dominanceCap = (pct?: number | null) => {
    if (pct == null || totalMcapUsd == null || !Number.isFinite(totalMcapUsd)) return null;
    const cap = (totalMcapUsd * pct) / 100.0;
    return Number.isFinite(cap) && cap > 0 ? cap : null;
  };
  const rawNewsItems = portfolio?.newsImpact?.items ?? [];
  const visibleNewsItems = useMemo(() => {
    return rawNewsItems
      .map((item) => {
        const matchedSymbols = (item.matchedSymbols ?? []).filter((s) => holdingsSymbolsSet.has(s));
        if (matchedSymbols.length === 0) return null;
        if (
          portfolioClassFilter !== "ALL" &&
          !matchedSymbols.some((s) => getAssetClass(s, symbolToHolding[s]) === portfolioClassFilter)
        ) {
          return null;
        }
        if (portfolioDirectOnly) {
          if (item.low_signal) return null;
          if (Math.abs(item.impactScore ?? 0) < 0.05) return null;
        }
        return { ...item, visibleSymbols: matchedSymbols };
      })
      .filter(Boolean) as Array<(typeof rawNewsItems)[number] & { visibleSymbols: string[] }>;
  }, [rawNewsItems, holdingsSymbolsSet, portfolioClassFilter, portfolioDirectOnly, symbolToHolding]);
  const netImpactByClass = useMemo(() => {
    const totals: Record<AssetClass, number> = { BIST: 0, NASDAQ: 0, CRYPTO: 0 };
    visibleNewsItems.forEach((item) => {
      const impact = item.impactScore ?? 0;
      const symbols = item.visibleSymbols || [];
      if (!symbols.length) return;
      const share = impact / symbols.length;
      symbols.forEach((s) => {
        const cls = getAssetClass(s, symbolToHolding[s]);
        totals[cls] += share;
      });
    });
    return totals;
  }, [visibleNewsItems, symbolToHolding]);
  const perSymbolImpact = useMemo(() => {
    const totals: Record<string, number> = {};
    visibleNewsItems.forEach((item) => {
      const impact = item.impactScore ?? 0;
      const symbols = item.visibleSymbols || [];
      if (!symbols.length) return;
      const share = impact / symbols.length;
      symbols.forEach((s) => {
        totals[s] = (totals[s] ?? 0) + share;
      });
    });
    return totals;
  }, [visibleNewsItems]);
  const topImpactedSymbols = useMemo(() => {
    return Object.entries(perSymbolImpact)
      .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
      .slice(0, 3)
      .map(([symbol, value]) => ({
        symbol,
        value,
        cls: getAssetClass(symbol, symbolToHolding[symbol]),
      }));
  }, [perSymbolImpact, symbolToHolding]);

  useEffect(() => {
    if (market?.coingecko?.dominance) {
      dominanceRef.current = market.coingecko.dominance;
    }
  }, [market?.coingecko?.dominance]);

  const resampleSeries = (values: number[], factor: number) => {
    if (factor <= 1) return values;
    const out: number[] = [];
    for (let i = 0; i < values.length; i += factor) {
      const idx = Math.min(i + factor - 1, values.length - 1);
      out.push(values[idx]);
    }
    return out;
  };
  const getSparkline = (asset: string) => {
    const points = barsData?.[asset]?.points?.map((p) => p.close) ?? [];
    const factor = timeframe === "1h" ? 4 : timeframe === "3h" ? 12 : timeframe === "6h" ? 24 : 1;
    return resampleSeries(points, factor);
  };
  const getBarUpdated = (asset: string) => barsData?.[asset]?.updated_iso ?? barsData?.[asset]?.points?.slice(-1)?.[0]?.ts;
  const calcChangeFromBars = (asset: string, hours: number) => {
    const points = barsData?.[asset]?.points ?? [];
    if (points.length < 2) return null;
    const last = points[points.length - 1];
    const lastTs = new Date(last.ts).getTime();
    if (!Number.isFinite(lastTs)) return null;
    const targetTs = lastTs - hours * 60 * 60 * 1000;
    let base: (typeof points)[number] | null = null;
    for (let i = points.length - 1; i >= 0; i -= 1) {
      const ts = new Date(points[i].ts).getTime();
      if (!Number.isFinite(ts)) continue;
      if (ts <= targetTs) {
        base = points[i];
        break;
      }
    }
    if (!base) return null;
    if (!Number.isFinite(base.close) || !Number.isFinite(last.close) || base.close === 0) return null;
    return ((last.close - base.close) / base.close) * 100;
  };
  const isStale = (iso?: string | null) => {
    if (!iso) return true;
    const ts = new Date(iso).getTime();
    if (!Number.isFinite(ts)) return true;
    return nowTs - ts > STALE_MS;
  };
  const isQuoteStale = (symbol: string) => isStale(liveQuotes?.[symbol]?.updated_iso ?? getBarUpdated(symbol));

  async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
    const res = await fetch(url, init);
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data?.error ?? "API error");
    }
    return data as T;
  }

  async function refreshIntel() {
    const qs = new URLSearchParams({
      timeframe,
      newsTimespan,
      watch,
    });
    const data = await fetchJSON<IntelResponse>(`${API_PROXY}/insights?${qs.toString()}`);
    setIntel((prev) => applyIntelDiff(prev, data));
  }

  async function refreshForecast() {
    const qs = new URLSearchParams({ tf: timeframe, target: forecastTarget });
    const data = await fetchJSON<any>(`${API_PROXY}/forecasts/latest?${qs.toString()}`);
    if (data?.forecast === null) {
      setForecast(null);
      return;
    }
    setForecast(data as ForecastView);
  }

  async function refreshMetrics() {
    const data = await fetchJSON<{ metrics: ForecastMetrics[] }>(`${API_PROXY}/forecasts/metrics`);
    setMetrics(data?.metrics ?? []);
  }

  async function refreshDominance() {
    setDominanceLoading(true);
    try {
      await refreshIntel();
    } catch (e: any) {
      // keep last snapshot on failure
    } finally {
      setDominanceLoading(false);
    }
  }

  async function refreshEvents() {
    const data = await fetchJSON<EventClusterResponse>(`${API_PROXY}/events/latest?hours=24`);
    setEventClusters(data);
  }

  async function refreshHealth() {
    try {
      const data = await fetchJSON<ServiceHealth>(`${API_PROXY}/health`);
      setBackendHealth(data);
    } catch (e) {
      setBackendHealth(null);
    }
    try {
      const data = await fetchJSON<ServiceHealth>(`${API_PROXY}/health/analytics`);
      setAnalyticsHealth(data);
    } catch (e) {
      setAnalyticsHealth(null);
    }
  }

  async function refreshBars() {
    const limit = timeframe === "15m" ? "96" : "192";
    const qs = new URLSearchParams({
      assets: BAR_ASSETS.join(","),
      limit,
    });
    try {
      const data = await fetchJSON<any>(`${API_PROXY}/bars?${qs.toString()}`);
      setBarsData(data?.assets ?? {});
    } catch (e: any) {
      setBarsData((prev) => prev ?? {});
    }
  }

  async function refreshPortfolio() {
    setPortfolioLoading(true);
    try {
      const qs = new URLSearchParams({ base: portfolioBase, horizon: portfolioHorizon });
      const data = await fetchJSON<PortfolioResponse>(`${API_PROXY}/portfolio?${qs.toString()}`);
      setPortfolio(data);
    } catch (e) {
      setPortfolio(null);
    } finally {
      setPortfolioLoading(false);
    }
  }

  const debateWindow =
    portfolioPeriod === "weekly" ? "7d" : portfolioPeriod === "monthly" ? "30d" : portfolioHorizon;

  const selectDebateOutput = (data: DebateResponse | null): DebateOutput | null => {
    if (!data) return null;
    const winner = data.winner;
    if (winner === "referee" && data.consensus) return data.consensus;
    if (winner === "openrouter" && data.raw?.openrouter) return data.raw.openrouter;
    if (winner === "openai" && data.raw?.openai) return data.raw.openai;
    if (winner === "tie" && data.raw?.openrouter) return data.raw.openrouter;
    if (winner === "single") return data.raw?.openrouter ?? data.raw?.openai ?? null;
    return (data.consensus as DebateOutput) ?? data.raw?.openrouter ?? data.raw?.openai ?? null;
  };

  const getDebugValue = (notes: string[] | undefined, key: string) => {
    if (!notes?.length) return null;
    const prefix = `${key}=`;
    const hit = notes.find((n) => n.startsWith(prefix));
    return hit ? hit.slice(prefix.length) : null;
  };

  async function fetchDebateCache() {
    setDebateLoading(true);
    setDebateError(null);
    try {
      const qs = new URLSearchParams({
        window: debateWindow,
        horizon: portfolioPeriod,
        base: portfolioBase,
      });
      const data = await fetchJSON<DebateResponse>(`${API_PROXY}/portfolio/debate?${qs.toString()}`);
      setDebateMeta(data);
      const picked = selectDebateOutput(data);
      setDebateInsights(picked);
      if (!picked && data?.dataStatus !== "ok") {
        setDebateError(data?.reason ? `OpenRouter içgörüleri devre dışı: ${data.reason}` : "OpenRouter içgörüleri devre dışı / veri eksik");
      }
    } catch (e) {
      setDebateMeta(null);
      setDebateInsights(null);
      setDebateError("OpenRouter içgörüleri devre dışı / veri eksik");
    } finally {
      setDebateLoading(false);
    }
  }

  async function requestDebate() {
    setDebateLoading(true);
    setDebateError(null);
    try {
      const payload = {
        window: debateWindow,
        horizon: portfolioPeriod,
        base: portfolioBase,
        force: true,
      };
      const data = await fetchJSON<DebateResponse>(`${API_PROXY}/portfolio/debate`, {
        method: "POST",
        body: JSON.stringify(payload),
        headers: { "Content-Type": "application/json" },
      });
      setDebateMeta(data);
      setDebateInsights(selectDebateOutput(data));
      if (data?.dataStatus !== "ok") {
        setDebateError(data?.reason ? `OpenRouter içgörüleri devre dışı: ${data.reason}` : "OpenRouter içgörüleri devre dışı / veri eksik");
      }
    } catch (e) {
      setDebateMeta(null);
      setDebateInsights(null);
      setDebateError("OpenRouter içgörüleri devre dışı / veri eksik");
    } finally {
      setDebateLoading(false);
    }
  }

  async function refreshAll() {
    setLoading(true);
    setErr(null);
    try {
      await Promise.all([
        refreshIntel(),
        refreshForecast(),
        refreshMetrics(),
        refreshEvents(),
        refreshBars(),
        refreshHealth(),
        refreshPortfolio(),
      ]);
    } catch (e: any) {
      setErr(e?.message ?? "Veri cekme hatasi");
    } finally {
      setLoading(false);
    }
  }

  async function refreshDerivatives() {
    setDerivLoading(true);
    try {
      const headers: Record<string, string> = {};
      if (derivETagRef.current) {
        headers["If-None-Match"] = derivETagRef.current;
      }
      if (derivLastModRef.current) {
        headers["If-Modified-Since"] = derivLastModRef.current;
      }
      const url = `${API_PROXY}/derivatives?exchange=${derivExchange}&symbol=BTCUSDT`;
      const res = await fetch(url, { headers });
      if (res.status === 304) {
        return;
      }
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data?.error ?? "Derivatives API error");
      }
      const etag = res.headers.get("ETag");
      const lastMod = res.headers.get("Last-Modified");
      if (etag) derivETagRef.current = etag;
      if (lastMod) derivLastModRef.current = lastMod;
      const data = (await res.json()) as DerivativesResponse;
      setDerivData(data);
    } catch (e: any) {
      // keep last good derivatives data
    } finally {
      setDerivLoading(false);
    }
  }

  useEffect(() => {
    refreshAll();
    const id = window.setInterval(refreshAll, 60_000);
    return () => window.clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const stored = window.localStorage.getItem("mq-dark-mode");
    const enabled = stored === "1";
    setDarkMode(enabled);
    document.documentElement.classList.toggle("dark", enabled);
  }, []);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", darkMode);
    window.localStorage.setItem("mq-dark-mode", darkMode ? "1" : "0");
  }, [darkMode]);

  useEffect(() => {
    const id = window.setInterval(() => setNowTs(Date.now()), 30_000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    const normalized = normalizeWatchlist(watchInput);
    const id = window.setTimeout(() => {
      setWatch((prev) => (prev === normalized ? prev : normalized));
    }, 450);
    return () => window.clearTimeout(id);
  }, [watchInput]);

  useEffect(() => {
    refreshIntel().catch(() => null);
  }, [newsTimespan, timeframe, watch]);

  useEffect(() => {
    refreshBars().catch(() => null);
  }, [timeframe]);

  useEffect(() => {
    refreshForecast().catch(() => null);
  }, [timeframe, forecastTarget]);

  useEffect(() => {
    refreshPortfolio().catch(() => null);
  }, [portfolioBase, portfolioHorizon]);

  useEffect(() => {
    fetchDebateCache().catch(() => null);
  }, [portfolioBase, portfolioHorizon, portfolioPeriod]);

  useEffect(() => {
    let es: EventSource | null = null;
    let retryId: number | undefined;

    const connect = () => {
      const qs = new URLSearchParams({
        timeframe,
        newsTimespan,
        watch,
      });
      es = new EventSource(`${BACKEND_BASE}/api/v1/stream?${qs.toString()}`);
      es.onmessage = (evt) => {
        try {
          const data = JSON.parse(evt.data);
          if (data?.market && !data?.error) {
            const nextMarket = data.market as MarketSnapshot;
            const dominance = dominanceRef.current ?? nextMarket?.coingecko?.dominance ?? null;
            if (dominance && nextMarket?.coingecko) {
              setLiveMarket({
                ...nextMarket,
                coingecko: {
                  ...nextMarket.coingecko,
                  dominance,
                },
              });
            } else {
              setLiveMarket(nextMarket);
            }
          }
          if (data?.risk && !data?.error) {
            setLiveRisk(data.risk);
          }
          if (data?.quotes) {
            setLiveQuotes(data.quotes);
          }
        } catch (err) {
          // ignore malformed payloads
        }
      };
      es.onerror = () => {
        es?.close();
        es = null;
        retryId = window.setTimeout(connect, 4000);
      };
    };

    connect();
    return () => {
      if (retryId) window.clearTimeout(retryId);
      es?.close();
    };
  }, [timeframe, newsTimespan, watch]);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let retryId: number | undefined;

    const connect = () => {
      ws = new WebSocket("wss://stream.binance.com:9443/stream?streams=btcusdt@miniTicker/ethusdt@miniTicker");
      ws.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          const stream = payload?.stream || "";
          const data = payload?.data || {};
          const price = Number(data.c);
          const open = Number(data.o);
          const changePct = Number.isFinite(price) && Number.isFinite(open) && open !== 0
            ? ((price - open) / open) * 100
            : null;
          if (!Number.isFinite(price)) return;
          if (stream.includes("btcusdt")) {
            setLiveSpot((prev) => ({ ...(prev ?? {}), btc: price }));
            if (changePct != null) {
              setLiveSpotChange((prev) => ({ ...(prev ?? {}), btc: changePct }));
            }
            setLiveSpotUpdated((prev) => ({ ...(prev ?? {}), btc: new Date().toISOString() }));
          } else if (stream.includes("ethusdt")) {
            setLiveSpot((prev) => ({ ...(prev ?? {}), eth: price }));
            if (changePct != null) {
              setLiveSpotChange((prev) => ({ ...(prev ?? {}), eth: changePct }));
            }
            setLiveSpotUpdated((prev) => ({ ...(prev ?? {}), eth: new Date().toISOString() }));
          }
        } catch (err) {
          // ignore malformed payloads
        }
      };
      ws.onerror = () => {
        ws?.close();
      };
      ws.onclose = () => {
        retryId = window.setTimeout(connect, 4000);
      };
    };

    connect();
    return () => {
      if (retryId) window.clearTimeout(retryId);
      ws?.close();
    };
  }, []);

  useEffect(() => {
    derivETagRef.current = null;
    derivLastModRef.current = null;
    let intervalId: number | undefined;
    const timerId = window.setTimeout(() => {
      refreshDerivatives();
      intervalId = window.setInterval(refreshDerivatives, 60_000);
    }, 15_000);
    return () => {
      window.clearTimeout(timerId);
      if (intervalId) window.clearInterval(intervalId);
    };
  }, [derivExchange]);

  const clusterItems = useMemo(() => eventClusters?.clusters ?? [], [eventClusters]);
  const clusterByAsset = useMemo(() => {
    const out: Record<string, EventClusterView> = {};
    for (const c of clusterItems) {
      const targets = c.targets ?? [];
      for (const t of targets) {
        const prev = out[t.asset];
        if (!prev || (c.impact_score ?? 0) > (prev.impact_score ?? 0) || c.ts_utc > prev.ts_utc) {
          out[t.asset] = c;
        }
      }
    }
    return out;
  }, [clusterItems]);
  const filteredClusters = useMemo(() => {
    const tag = tagFilter.trim().toLowerCase();
    return clusterItems.filter((c) => {
      if (impactMin > 0 && (c.impact_score ?? 0) < impactMin) return false;
      if (tierFilter !== "all" && c.source_tier !== tierFilter) return false;
      if (tag && !(c.tags || []).some((t) => t.toLowerCase().includes(tag))) return false;
      return true;
    });
  }, [clusterItems, impactMin, tierFilter, tagFilter]);


  const moversSummary = useMemo(() => {
    const items = dailyMovers?.items ?? [];
    if (items.length === 0) {
      return { confidence: 0 };
    }
    const avg = items.reduce((acc, it) => acc + (it.confidence ?? 0), 0) / items.length;
    return { confidence: Math.round(avg) };
  }, [dailyMovers]);

  const spotConfidence = cryptoOutlook?.confidence ?? forecast?.confidence;
  const frontendBasePresent = Boolean(process.env.NEXT_PUBLIC_API_BASE);
  const spotBtcChange =
    liveSpotChange?.btc ??
    (liveQuotes?.BTC?.change_pct ?? null) ??
    (spotMarket?.coingecko?.btc_price_usd && spotMarket.coingecko.btc_price_usd > 0
      ? spotMarket.coingecko.btc_chg_24h
      : null) ??
    (spotMarket?.yahoo?.btc && spotMarket.yahoo.btc > 0 ? spotMarket.yahoo.btc_chg_24h : null) ??
    calcChangeFromBars("BTC", 24);
  const spotEthChange =
    liveSpotChange?.eth ??
    (liveQuotes?.ETH?.change_pct ?? null) ??
    (spotMarket?.coingecko?.eth_price_usd && spotMarket.coingecko.eth_price_usd > 0
      ? spotMarket.coingecko.eth_chg_24h
      : null) ??
    (spotMarket?.yahoo?.eth && spotMarket.yahoo.eth > 0 ? spotMarket.yahoo.eth_chg_24h : null) ??
    calcChangeFromBars("ETH", 24);
  const btcSpark = getSparkline("BTC");
  const dxySpark = getSparkline("DXY");
  const dxyBtcCorr = useMemo(() => calcCorrelation(btcSpark, dxySpark), [btcSpark, dxySpark]);
  const spotHot =
    Math.abs(spotBtcChange ?? 0) >= 1.2 ||
    Math.abs(spotEthChange ?? 0) >= 1.2 ||
    (spotConfidence ?? 0) >= 0.65 ||
    Boolean(clusterByAsset["BTC"]);
  const glowClass = (on: boolean) =>
    on ? "ring-1 ring-emerald-300/60 shadow-[0_0_32px_rgba(34,197,94,0.18)]" : "";

  return (
    <div className="mx-auto max-w-7xl space-y-6 p-4 md:p-8">
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6 }}
        className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between"
      >
        <div>
          <div className="text-3xl font-semibold text-black">MacroQuant Intel</div>
          <div className="text-sm text-black/60">
            Son guncelleme: <span className="font-medium text-black">{toTSIString(intel?.tsISO)}</span> (TSI)
          </div>
          <div className="mt-2">
            <DebugRow debug={intel?.debug} />
          </div>
        </div>

        <div className="flex flex-col gap-3 md:items-end">
          <div className="flex flex-wrap items-center gap-2">
            <div className="flex items-center gap-2 rounded-2xl border border-black/10 bg-white/80 px-3 py-2 text-sm text-black/60">
              <Search className="h-4 w-4" />
              <Input
                value={watchInput}
                onChange={(e) => setWatchInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key !== "Enter") return;
                  const normalized = normalizeWatchlist(watchInput);
                  setWatch(normalized);
                  setWatchInput(normalized);
                }}
                placeholder="watchlist (BTC, ETH, AAPL)"
                className="w-64 border-none bg-transparent px-0 py-0 focus:ring-0"
              />
            </div>
            <select
              value={newsTimespan}
              onChange={(e) => setNewsTimespan(e.target.value)}
              className="rounded-2xl border border-black/10 bg-white/80 px-3 py-2 text-sm"
            >
              <option value="1h">News 1h</option>
              <option value="6h">News 6h</option>
              <option value="24h">News 24h</option>
            </select>
            <select
              value={timeframe}
              onChange={(e) => setTimeframe(e.target.value)}
              className="rounded-2xl border border-black/10 bg-white/80 px-3 py-2 text-sm"
            >
              <option value="15m">Timeframe 15m</option>
              <option value="1h">Timeframe 1h</option>
              <option value="3h">Timeframe 3h</option>
              <option value="6h">Timeframe 6h</option>
            </select>
            <Button onClick={refreshAll} disabled={loading} className="gap-2">
              <RefreshCw className={cx("h-4 w-4", loading && "animate-spin")} />
              Yenile
            </Button>
            <Button
              variant="ghost"
              onClick={() => setDarkMode((prev) => !prev)}
              className="gap-2"
            >
              {darkMode ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
              {darkMode ? "Light" : "Dark"}
            </Button>
          </div>
          <div className="flex items-center gap-2 text-xs text-black/50">
            <Activity className="h-3.5 w-3.5" />
            Go API: {BACKEND_BASE}
          </div>
        </div>
      </motion.div>

      {err ? (
        <Card
          title={
            <span className="inline-flex items-center gap-2 text-rose-700">
              <ShieldAlert className="h-5 w-5" /> Hata
            </span>
          }
          className="border-rose-200 bg-rose-50"
        >
          <div className="text-sm text-rose-700">{err}</div>
        </Card>
      ) : null}

      <div className="space-y-4">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
          <div className="flex flex-col gap-4">
            <Card
              title="SPOTs"
              right={<MissingChips debug={intel?.debug} keys={["coingecko"]} />}
              className={cx("h-full", glowClass(spotHot))}
            >
              <div className="space-y-3">
                <MetricRow
                  className="w-full"
                  label="BTC"
                  value={formatUsdValue(liveSpot?.btc ?? spotMarket?.coingecko?.btc_price_usd, 0)}
                  delta={spotBtcChange ?? null}
                  stale={isStale(liveSpotUpdated?.btc ?? spotMarket?.tsISO)}
                  valueClassName="text-lg"
                />
                <MetricRow
                  className="w-full"
                  label="ETH"
                  value={formatUsdValue(liveSpot?.eth ?? spotMarket?.coingecko?.eth_price_usd, 0)}
                  delta={spotEthChange ?? null}
                  stale={isStale(liveSpotUpdated?.eth ?? spotMarket?.tsISO)}
                  valueClassName="text-lg"
                />
                <div className="space-y-2 border-t border-black/10 pt-3">
                  <MetricRow
                    className="w-full"
                    label={
                      <span className="inline-flex items-center gap-2">
                        ALT (ex BTC)
                        {spotMarket?.coingecko?.altcoin_total_value_ex_btc_source ? (
                          <span className="text-[10px] text-black/45">
                            src {spotMarket?.coingecko?.altcoin_total_value_ex_btc_source}
                          </span>
                        ) : null}
                      </span>
                    }
                    value={formatCurrencyCompact(spotMarket?.coingecko?.altcoin_total_value_ex_btc_usd, 1)}
                    showDelta={false}
                  />
                  <MetricRow
                    className="w-full"
                    label="Total MCap"
                    value={formatCurrencyCompact(spotMarket?.coingecko?.total_mcap_usd, 1)}
                    showDelta={false}
                  />
                  <div className="flex items-center justify-between gap-3">
                    <div className="flex flex-col">
                      <span className={METRIC_LABEL_CLASS}>DXY ↔ BTC correlation (24h)</span>
                      <span className={METRIC_VALUE_CLASS}>{formatNumber(dxyBtcCorr, 2)}</span>
                    </div>
                    <span className="group relative inline-flex">
                      <Badge tone={dxyBtcCorr <= -0.3 ? "good" : dxyBtcCorr >= 0.3 ? "bad" : "neutral"}>
                        {dxyBtcCorr <= -0.3 ? "Inverse" : dxyBtcCorr >= 0.3 ? "Aligned" : "Mixed"}
                      </Badge>
                      <span className="pointer-events-none absolute right-0 top-full z-10 mt-2 w-56 rounded-xl border border-black/10 bg-[var(--panel-strong)] p-2 text-[11px] text-[color:var(--ink)] opacity-0 shadow transition group-hover:opacity-100">
                        {dxyBtcCorr <= -0.3
                          ? "Inverse: 24h correlation <= -0.30 (DXY up, BTC tends to move down)."
                          : dxyBtcCorr >= 0.3
                          ? "Aligned: 24h correlation >= 0.30 (DXY and BTC move together)."
                          : "Mixed: correlation between -0.30 and 0.30 (no clear relationship)."}
                      </span>
                    </span>
                  </div>
                </div>
              </div>
            </Card>
          </div>

          <Card
            title="Dominance + DXY"
            right={
              <div className="flex items-center gap-2">
                <Button
                  variant="ghost"
                  onClick={refreshDominance}
                  disabled={dominanceLoading}
                  className="h-7 px-2 text-xs"
                >
                  <RefreshCw className={cx("h-3.5 w-3.5", dominanceLoading && "animate-spin")} />
                </Button>
                <InfoTip
                  lines={[
                    "Dominance: piyasa degeri payi (BTC/USDT/USDC).",
                    "DXY: dolar endeksi (24h değişim).",
                  ]}
                />
                <MissingChips debug={intel?.debug} keys={["coingecko", "yahoo"]} />
              </div>
            }
            className="h-full"
          >
            <div className="space-y-3">
              <MetricRow
                label="BTC.D"
                value={
                  <span className="flex flex-col">
                    <span>{formatPercent(market?.coingecko?.dominance?.btc, 2)}</span>
                    {dominanceCap(market?.coingecko?.dominance?.btc) ? (
                      <span className="text-xs text-black/50">
                        {formatCurrencyCompact(dominanceCap(market?.coingecko?.dominance?.btc) ?? undefined, 1)}
                      </span>
                    ) : null}
                  </span>
                }
                showDelta={false}
              />
              <MetricRow
                label="USDT.D"
                value={
                  <span className="flex flex-col">
                    <span>{formatPercent(market?.coingecko?.dominance?.usdt, 2)}</span>
                    {dominanceCap(market?.coingecko?.dominance?.usdt) ? (
                      <span className="text-xs text-black/50">
                        {formatCurrencyCompact(dominanceCap(market?.coingecko?.dominance?.usdt) ?? undefined, 1)}
                      </span>
                    ) : null}
                  </span>
                }
                showDelta={false}
              />
              <MetricRow
                label="USDC.D"
                value={
                  <span className="flex flex-col">
                    <span>{formatPercent(market?.coingecko?.dominance?.usdc, 2)}</span>
                    {dominanceCap(market?.coingecko?.dominance?.usdc) ? (
                      <span className="text-xs text-black/50">
                        {formatCurrencyCompact(dominanceCap(market?.coingecko?.dominance?.usdc) ?? undefined, 1)}
                      </span>
                    ) : null}
                  </span>
                }
                showDelta={false}
              />
              <MetricRow
                label="DXY"
                value={formatNumber(market?.yahoo?.dxy, 2)}
                delta={market?.yahoo?.dxy_chg_24h ?? null}
                stale={isStale(getBarUpdated("DXY"))}
              />
            </div>
          </Card>

          <Card title="Stock Markets" right={<MissingChips debug={intel?.debug} keys={["yahoo"]} />} className="h-full">
            <div className="space-y-3">
              <MetricRow
                label="Nasdaq"
                value={formatNumber(macroMarket?.yahoo?.nasdaq, 2)}
                delta={macroMarket?.yahoo?.nasdaq_chg_24h ?? null}
                stale={isStale(getBarUpdated("NASDAQ"))}
              />
              <MetricRow
                label="FTSE 100"
                value={formatNumber(macroMarket?.yahoo?.ftse, 2)}
                delta={macroMarket?.yahoo?.ftse_chg_24h ?? null}
                stale={isStale(getBarUpdated("FTSE"))}
              />
              <MetricRow
                label="Euro Stoxx 50"
                value={formatNumber(macroMarket?.yahoo?.eurostoxx, 2)}
                delta={macroMarket?.yahoo?.eurostoxx_chg_24h ?? null}
                stale={isStale(getBarUpdated("EUROSTOXX"))}
              />
              <MetricRow
                label="BIST 100"
                value={formatNumber(macroMarket?.yahoo?.bist, 2)}
                delta={macroMarket?.yahoo?.bist_chg_24h ?? null}
                stale={isStale(getBarUpdated("BIST"))}
              />
            </div>
          </Card>

          <Card title="Commodities" right={<MissingChips debug={intel?.debug} keys={["yahoo"]} />} className="h-full">
            <div className="space-y-3">
              <MetricRow
                label="Silver"
                value={formatNumber(macroMarket?.yahoo?.silver, 2)}
                delta={macroMarket?.yahoo?.silver_chg_24h ?? null}
                stale={isStale(getBarUpdated("SILVER"))}
              />
              <MetricRow
                label="Gold"
                value={formatNumber(macroMarket?.yahoo?.gold, 2)}
                delta={macroMarket?.yahoo?.gold_chg_24h ?? null}
                stale={isStale(getBarUpdated("GOLD"))}
              />
              <MetricRow
                label="Copper"
                value={formatNumber(macroMarket?.yahoo?.copper, 2)}
                delta={macroMarket?.yahoo?.copper_chg_24h ?? null}
                stale={isStale(getBarUpdated("COPPER"))}
              />
            </div>
          </Card>
        </div>

        {PORTFOLIO_ENABLED ? (
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
            <Card
              title="Portfolio"
              right={
                <div className="flex items-center gap-2 text-xs">
                  {(["TRY", "USD"] as const).map((v) => (
                    <button
                      key={`base-${v}`}
                      onClick={() => setPortfolioBase(v)}
                      className={cx(
                        "rounded-full border px-2 py-0.5",
                        portfolioBase === v ? "border-black/60 bg-black/5 text-black" : "border-black/10"
                      )}
                    >
                      {v}
                    </button>
                  ))}
                </div>
              }
          >
            <div className="space-y-3 text-sm">
              <div className="flex items-center justify-between">
                <span className={METRIC_LABEL_CLASS}>Toplam Deger</span>
                <span className="text-lg font-semibold text-black tabular-nums">
                  {portfolioBase === "TRY" ? "₺" : "$"}
                  {portfolioTotal > 0 ? formatNumber(portfolioTotal, 0) : "—"}
                </span>
              </div>
              <div className="flex items-center justify-between text-[11px] text-black/60">
                <span>Anlik degisim</span>
                <span
                  className={cx(
                    "tabular-nums font-semibold",
                    portfolioDelta > 0 ? "text-emerald-600" : portfolioDelta < 0 ? "text-rose-600" : "text-black/50"
                  )}
                >
                  {portfolioDelta !== 0
                    ? `${portfolioDelta > 0 ? "+" : "-"}${portfolioBase === "TRY" ? "₺" : "$"}${formatNumber(
                        Math.abs(portfolioDelta),
                        0
                      )}`
                    : "—"}
                </span>
              </div>
              <div className="flex flex-wrap items-center gap-2 text-[11px] text-black/55">
                {(Object.keys(holdingChangeByClass.totals) as AssetClass[]).map((cls, idx) => {
                  const delta = holdingChangeByClass.totals[cls];
                  const count = holdingChangeByClass.counts[cls];
                  const sign = delta > 0 ? "+" : delta < 0 ? "-" : "";
                  const tone =
                    delta > 0 ? "text-emerald-600" : delta < 0 ? "text-rose-600" : "text-black/50";
                  return (
                    <span key={`delta-${cls}`} className="inline-flex items-center gap-1">
                      <span className="uppercase">{cls}</span>
                      <span className={tone}>
                        {count > 0
                          ? `${sign}${portfolioBase === "TRY" ? "₺" : "$"}${formatMoneyByClass(cls, Math.abs(delta))}`
                          : "—"}
                      </span>
                      {idx < 2 ? <span className="text-black/30">•</span> : null}
                    </span>
                  );
                })}
              </div>
              {holdingsList.length > 0 && !portfolioHasValues ? (
                <div className="rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
                  Fiyat/FX verisi eksik olabilir (degerler 0 gorunebilir).
                </div>
              ) : null}
              <div className="grid gap-3">
                {(Object.keys(holdingsByClass) as AssetClass[]).map((cls) => {
                  const items = holdingsByClass[cls];
                  if (!items.length) return null;
                  const subtotal = holdingTotalsByClass[cls];
                  const delta = holdingChangeByClass.totals[cls];
                  const deltaCount = holdingChangeByClass.counts[cls];
                  const deltaSign = delta > 0 ? "+" : delta < 0 ? "-" : "";
                  return (
                    <div key={`group-${cls}`} className="rounded-xl border border-black/10 bg-white/70 p-3">
                      <div className="flex items-center justify-between text-[11px] text-black/60">
                        <div className="flex items-center gap-2">
                          <Badge tone={assetClassTone(cls)}>{cls}</Badge>
                        </div>
                        <span>
                          {items.length} pozisyon ·{" "}
                          {subtotal > 0 ? `${portfolioBase === "TRY" ? "₺" : "$"}${formatMoneyByClass(cls, subtotal)}` : "—"}
                          {deltaCount > 0 ? (
                            <span
                              className={cx(
                                "ml-2",
                                delta > 0 ? "text-emerald-600" : delta < 0 ? "text-rose-600" : "text-black/50"
                              )}
                            >
                              {deltaSign}
                              {portfolioBase === "TRY" ? "₺" : "$"}
                              {formatMoneyByClass(cls, Math.abs(delta))}
                            </span>
                          ) : null}
                        </span>
                      </div>
                      <div className="mt-2 grid gap-2">
                        {items.map((h) => {
                          const clsBadge = getAssetClass(h.symbol, h);
                          const hasPrice = Number.isFinite(h.display_price ?? 0) && (h.display_price ?? 0) > 0;
                          const hasValue = Number.isFinite(h.display_value ?? 0) && (h.display_value ?? 0) > 0;
                          const valueLabel = !hasPrice
                            ? "veri eksik"
                            : !hasValue
                              ? "FX/fiyat eksik olabilir"
                              : null;
                          return (
                            <div key={`holding-${h.symbol}`} className="flex items-center justify-between gap-3 text-xs">
                              <div className="flex flex-col">
                                <div className="flex items-center gap-2">
                                  <span className="font-semibold text-black">{h.symbol}</span>
                                </div>
                                <span className="text-[11px] text-black/55">
                                  {hasPrice
                                    ? `${formatQtyByClass(clsBadge, h.qty)} × ${formatNumber(h.display_price, 2)} ${
                                        h.currency
                                      }`
                                    : "—"}
                                  {valueLabel ? ` · ${valueLabel}` : ""}
                                </span>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className="tabular-nums text-black/70">
                                  {hasValue
                                    ? `${portfolioBase === "TRY" ? "₺" : "$"}${formatMoneyByClass(clsBadge, h.display_value)}`
                                    : "—"}
                                </span>
                                <ChangePill value={h.change_pct ?? null} />
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  );
                })}
                {holdingsList.length === 0 && !portfolioLoading ? (
                  <div className="text-xs text-black/50">Portfoy verisi yok.</div>
                ) : null}
              </div>
              <div className="border-t border-black/10 pt-2 text-xs text-black/55">
                <div className="flex items-center justify-between">
                  <span>Risk (HHI)</span>
                  <span className="tabular-nums">{formatNumber(portfolio?.risk?.hhi ?? 0, 3)}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span>Max Weight</span>
                  <span className="tabular-nums">{formatPercent((portfolio?.risk?.max_weight ?? 0) * 100, 1)}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span>Vol 30g</span>
                  <span className="tabular-nums">{formatPercent((portfolio?.risk?.vol_30d ?? 0) * 100, 2)}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span>VaR 95 (1g)</span>
                  <span className="tabular-nums">{formatPercent((portfolio?.risk?.var_95_1d ?? 0) * 100, 2)}</span>
                </div>
              </div>
            </div>
          </Card>

            <Card
              title="Haber → Portfoy Etkisi"
              right={
                <div className="flex items-center gap-2 text-xs">
                  {(["24h", "7d", "30d"] as const).map((v) => (
                    <button
                      key={`hz-${v}`}
                      onClick={() => setPortfolioHorizon(v)}
                      className={cx(
                        "rounded-full border px-2 py-0.5",
                        portfolioHorizon === v ? "border-black/60 bg-black/5 text-black" : "border-black/10"
                      )}
                    >
                      {v}
                    </button>
                  ))}
                </div>
              }
          >
            <div className="space-y-3 text-xs">
              <div className="flex flex-wrap items-center justify-between gap-2 text-black/60">
                <span>Eslesen haber</span>
                <span>
                  {portfolio?.newsImpact?.coverage?.matched ?? visibleNewsItems.length}/
                  {portfolio?.newsImpact?.coverage?.total ?? rawNewsItems.length}
                </span>
              </div>
              <div className="flex flex-wrap items-center gap-2 text-[11px]">
                {(["ALL", "BIST", "NASDAQ", "CRYPTO"] as const).map((v) => (
                  <button
                    key={`pf-filter-${v}`}
                    onClick={() => setPortfolioClassFilter(v)}
                    className={cx(
                      "rounded-full border px-2 py-0.5",
                      portfolioClassFilter === v ? "border-black/60 bg-black/5 text-black" : "border-black/10"
                    )}
                  >
                    {v === "ALL" ? "All" : v}
                  </button>
                ))}
                <button
                  onClick={() => setPortfolioDirectOnly((prev) => !prev)}
                  className={cx(
                    "rounded-full border px-2 py-0.5",
                    portfolioDirectOnly ? "border-black/60 bg-black/5 text-black" : "border-black/10 text-black/60"
                  )}
                >
                  Direct only
                </button>
              </div>
              <div className="rounded-xl border border-black/10 bg-white/70 px-3 py-2 text-[11px] text-black/60">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span>Impact Summary</span>
                  <span className="flex items-center gap-2">
                    <span>BIST {formatSigned(netImpactByClass.BIST)}</span>
                    <span>NASDAQ {formatSigned(netImpactByClass.NASDAQ)}</span>
                    <span>CRYPTO {formatSigned(netImpactByClass.CRYPTO)}</span>
                  </span>
                </div>
              </div>
              {topImpactedSymbols.length ? (
                <div className="rounded-xl border border-black/10 bg-white/70 px-3 py-2 text-[11px] text-black/60">
                  <div className="mb-1 text-[11px] uppercase tracking-wide text-black/40">
                    En cok etkilenenler
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {topImpactedSymbols.map((t) => (
                      <div key={`top-${t.symbol}`} className="flex items-center gap-2">
                        <Badge tone={assetClassTone(t.cls)} className="px-2 py-0.5 text-[10px] font-medium" title={t.cls}>
                          {t.symbol}
                        </Badge>
                        <span className={t.value >= 0 ? "text-emerald-700" : "text-rose-700"}>
                          {formatSigned(t.value)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}
              <div className="space-y-2">
                {visibleNewsItems.slice(0, 6).map((n, idx) => (
                  <div key={`pnews-${idx}`} className="rounded-xl border border-black/10 bg-white/70 p-2">
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-[11px] font-semibold text-black line-clamp-2">{n.title}</div>
                      <Badge tone={n.direction === "positive" ? "good" : n.direction === "negative" ? "bad" : "neutral"}>
                        {formatNumber(n.impactScore, 3)}
                      </Badge>
                    </div>
                    <div className="mt-1 flex flex-wrap gap-1 text-[10px] text-black/55">
                      {(n.visibleSymbols ?? []).map((s) => {
                        const cls = getAssetClass(s, symbolToHolding[s]);
                        return (
                          <Badge
                            key={`${n.title}-${s}`}
                            tone={assetClassTone(cls)}
                            className="px-2 py-0.5 text-[10px] font-medium"
                            title={cls}
                          >
                            {s}
                          </Badge>
                        );
                      })}
                      {n.low_signal ? (
                        <Badge tone="warn" className="px-2 py-0.5 text-[10px] font-medium">
                          low-signal
                        </Badge>
                      ) : null}
                    </div>
                  </div>
                ))}
                {!visibleNewsItems.length ? (
                  <div className="text-xs text-black/50">Eslesen haber yok.</div>
                ) : null}
              </div>
            </div>
          </Card>

          <Card
            title="Optimizasyon Onerileri"
            right={
              <div className="flex items-center gap-2 text-xs">
                {(["insights", "optimizer"] as const).map((v) => (
                  <button
                    key={`tab-${v}`}
                    onClick={() => setPortfolioRightTab(v)}
                    className={cx(
                      "rounded-full border px-2 py-0.5",
                      portfolioRightTab === v ? "border-black/60 bg-black/5 text-black" : "border-black/10 text-black/60"
                    )}
                  >
                    {v}
                  </button>
                ))}
              </div>
            }
          >
            <div className="space-y-3 text-xs">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  {(["daily", "weekly", "monthly"] as const).map((v) => (
                    <button
                      key={`per-${v}`}
                      onClick={() => setPortfolioPeriod(v)}
                      className={cx(
                        "rounded-full border px-2 py-0.5",
                        portfolioPeriod === v ? "border-black/60 bg-black/5 text-black" : "border-black/10"
                      )}
                    >
                      {v}
                    </button>
                  ))}
                </div>
                <span className="text-[11px] text-black/50">{debateWindow}</span>
              </div>
              {portfolioRightTab === "insights" ? (
                <div className="space-y-3 text-xs">
                  <div className="flex items-center justify-between gap-2">
                    <button
                      onClick={() => requestDebate().catch(() => null)}
                      className={cx(
                        "rounded-full border px-3 py-1 text-[11px]",
                        debateLoading ? "border-black/10 text-black/40" : "border-black/50 bg-black/5 text-black"
                      )}
                      disabled={debateLoading}
                    >
                      Debate
                    </button>
                    {debateMeta?.cache?.cooldown_remaining_seconds ? (
                      <span className="text-[10px] text-black/50">
                        {debateMeta.cache.cooldown_remaining_seconds}s cooldown
                      </span>
                    ) : null}
                  </div>
                  {debateMeta?.providers ? (
                    <div className="text-[10px] text-black/45">
                      openrouter: {debateMeta.providers.openrouter ?? "—"} · openai: {debateMeta.providers.openai ?? "—"}
                    </div>
                  ) : null}
                  {debateLoading ? <div className="text-xs text-black/50">Yukleniyor...</div> : null}
                  {!debateLoading && debateInsights ? (
                    <>
                      <div className="space-y-3">
                        <div className="text-[11px] uppercase tracking-wide text-black/40">Debate</div>
                        <div className="grid gap-2">
                          {(["openrouter", "openai"] as const).map((key) => {
                            const plan = debateMeta?.raw?.[key];
                            const model =
                              key === "openrouter"
                                ? getDebugValue(debateMeta?.debug_notes, "openrouter_model")
                                : getDebugValue(debateMeta?.debug_notes, "openai_model");
                            const title = model || key;
                            const error =
                              key === "openrouter"
                                ? getDebugValue(debateMeta?.debug_notes, "openrouter_error")
                                : getDebugValue(debateMeta?.debug_notes, "openai_error");
                            const metaError =
                              key === "openrouter"
                                ? debateMeta?.provider_meta?.openrouter?.error_message
                                : debateMeta?.provider_meta?.openai?.error_message;
                            return (
                              <div key={`debate-${key}`} className="rounded-xl border border-black/10 bg-white/70 px-3 py-2">
                                <div className="flex items-center justify-between">
                                  <span className="text-[11px] font-semibold text-black">{title}</span>
                                  <span className="text-[10px] text-black/50">{debateMeta?.providers?.[key] ?? "—"}</span>
                                </div>
                                {plan?.executiveSummary?.length ? (
                                  <ul className="mt-1 list-disc space-y-0.5 pl-4 text-[11px] text-black/60">
                                    {plan.executiveSummary.slice(0, 3).map((line, idx) => (
                                      <li key={`debate-${key}-s-${idx}`}>{line}</li>
                                    ))}
                                  </ul>
                                ) : (
                                  <div className="mt-1 text-[10px] text-black/40">
                                    no response{metaError ? ` · ${metaError}` : error ? ` · ${error}` : ""}
                                  </div>
                                )}
                              </div>
                            );
                          })}
                        </div>
                      </div>

                      <div className="space-y-2">
                        <div className="text-[11px] uppercase tracking-wide text-black/40">Hakem</div>
                        <div className="rounded-xl border border-black/10 bg-white/70 px-3 py-2">
                          <div className="flex items-center justify-between">
                            <span className="text-[11px] font-semibold text-black">Referee</span>
                            <span className="text-[10px] text-black/50">{debateMeta?.referee?.status ?? "—"}</span>
                          </div>
                          {debateMeta?.referee?.result ? (
                            <div className="mt-1 space-y-1 text-[11px] text-black/60">
                              <div className="flex items-center justify-between">
                                <span>winner</span>
                                <span className="font-semibold text-black">{debateMeta.referee.result.winner ?? "—"}</span>
                              </div>
                              <div className="flex items-center justify-between">
                                <span>confidence</span>
                                <span>{debateMeta.referee.result.confidence ?? "—"}</span>
                              </div>
                              {Array.isArray(debateMeta.referee.result.why) ? (
                                <ul className="list-disc space-y-0.5 pl-4 text-[11px] text-black/55">
                                  {debateMeta.referee.result.why.slice(0, 3).map((w: any, idx: number) => (
                                    <li key={`ref-why-${idx}`}>{w?.text ?? ""}</li>
                                  ))}
                                </ul>
                              ) : null}
                              {debateMeta.referee.result.contrarian_idea?.text ? (
                                <div className="text-[10px] text-black/50">
                                  contrarian: {debateMeta.referee.result.contrarian_idea.text}
                                </div>
                              ) : null}
                            </div>
                          ) : (
                            <div className="mt-1 text-[10px] text-black/40">
                              {debateMeta?.referee?.error ? `error: ${debateMeta.referee.error}` : "—"}
                            </div>
                          )}
                        </div>
                      </div>

                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <span className="text-[11px] uppercase tracking-wide text-black/40">Conclusion</span>
                          <Badge tone="info">{debateMeta?.winner ?? "—"}</Badge>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="text-[11px] uppercase tracking-wide text-black/40">Mode</span>
                          <Badge tone="info">{debateInsights.portfolioMode ?? "—"}</Badge>
                        </div>
                        {debateInsights.executiveSummary?.length ? (
                          <ul className="list-disc space-y-1 pl-4 text-[11px] text-black/65">
                            {debateInsights.executiveSummary.slice(0, 5).map((line, idx) => (
                              <li key={`conclusion-s-${idx}`}>{line}</li>
                            ))}
                          </ul>
                        ) : null}
                        {(debateInsights.actions?.length || debateInsights.trimSignals?.length) ? (
                          <div className="space-y-2">
                            <div className="text-[11px] uppercase tracking-wide text-black/40">Signals</div>
                            {(debateInsights.actions?.length
                              ? debateInsights.actions.slice(0, 5).map((a) => ({
                                  symbol: a.symbol,
                                  action: a.action,
                                  reason: a.reason,
                                  deltaWeight: undefined,
                                }))
                              : debateInsights.trimSignals?.slice(0, 5).map((t) => ({
                                  symbol: t.symbol,
                                  action: t.action ?? "trim",
                                  reason: t.evidence_ids?.join(" · "),
                                  deltaWeight: t.deltaWeight,
                                })) ?? []
                            ).map((a, idx) => {
                              const cls = a.symbol ? getAssetClass(a.symbol, symbolToHolding[a.symbol]) : "NASDAQ";
                              const reason = Array.isArray(a.reason) ? a.reason.join(" · ") : a.reason;
                              return (
                                <div key={`conclusion-act-${idx}`} className="rounded-xl border border-black/10 bg-white/70 px-3 py-2">
                                  <div className="flex items-center justify-between gap-2">
                                    <div className="flex items-center gap-2">
                                      {a.symbol ? <Badge tone={assetClassTone(cls)}>{`${cls}: ${a.symbol}`}</Badge> : null}
                                      <span className="font-semibold text-black">{a.action ?? "—"}</span>
                                    </div>
                                    <span className="text-[10px] text-black/50">
                                      {reason ?? ""}
                                      {typeof a.deltaWeight === "number" ? ` · ${formatPercent(a.deltaWeight * 100, 2)}` : ""}
                                    </span>
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        ) : null}
                        <div className="text-[11px] text-black/50">{debateInsights.note ?? "Sinyal; yatirim tavsiyesi degildir."}</div>
                      </div>
                    </>
                  ) : null}
                  {!debateLoading && !debateInsights ? (
                    <div className="text-xs text-black/50">{debateError ?? "OpenRouter içgörüleri devre dışı / veri eksik"}</div>
                  ) : null}
                </div>
              ) : (
                <div className="space-y-2 text-xs">
                  <div className="text-[11px] text-black/50">Yatirim tavsiyesi degildir.</div>
                  {(portfolio?.recommendations ?? [])
                    .filter((r) => r.period === portfolioPeriod)
                    .flatMap((r) => r.actions)
                    .slice(0, 6)
                    .map((a, idx) => {
                      const cls = getAssetClass(a.symbol, symbolToHolding[a.symbol]);
                      return (
                        <div
                          key={`rec-${idx}`}
                          className="flex items-center justify-between rounded-xl border border-black/10 bg-white/70 px-3 py-2"
                        >
                          <div className="flex flex-col">
                            <span className="font-semibold text-black">
                              {a.symbol} · {a.action}
                            </span>
                            <span className="text-[10px] text-black/55">{a.reason.join(" | ")}</span>
                            <div className="mt-1">
                              <Badge tone={assetClassTone(cls)}>{cls}</Badge>
                            </div>
                          </div>
                          <Badge tone={a.action === "increase" ? "good" : "bad"}>
                            {a.action === "increase" ? "+" : "-"}
                            {formatNumber(a.deltaWeight * 100, 2)}%
                          </Badge>
                        </div>
                      );
                    })}
                  {!portfolio?.recommendations?.length ? (
                    <div className="text-xs text-black/50">Oneri yok (veri eksik olabilir).</div>
                  ) : null}
                </div>
              )}
            </div>
          </Card>
          </div>
        ) : null}

        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          <HeatmapCard
            sectorName="Tech"
            pages={[
              { label: "US", tickers: TECH_SYMBOLS },
              { label: "Global (Non-US)", tickers: TECH_SYMBOLS_GLOBAL },
            ]}
            liveQuotes={liveQuotes}
            isQuoteStale={isQuoteStale}
            debug={intel?.debug}
          />
          <HeatmapCard
            sectorName="Energy"
            pages={[
              { label: "US", tickers: ENERGY_SYMBOLS },
              { label: "Global (Non-US)", tickers: ENERGY_SYMBOLS_GLOBAL },
            ]}
            liveQuotes={liveQuotes}
            isQuoteStale={isQuoteStale}
            debug={intel?.debug}
          />
          <HeatmapCard
            sectorName="Financials"
            pages={[
              { label: "US", tickers: FINANCIALS_SYMBOLS },
              { label: "Global (Non-US)", tickers: FINANCIALS_SYMBOLS_GLOBAL },
            ]}
            liveQuotes={liveQuotes}
            isQuoteStale={isQuoteStale}
            debug={intel?.debug}
          />
          <HeatmapCard
            sectorName="Industrials"
            pages={[
              { label: "US", tickers: INDUSTRIALS_SYMBOLS },
              { label: "Global (Non-US)", tickers: INDUSTRIALS_SYMBOLS_GLOBAL },
            ]}
            liveQuotes={liveQuotes}
            isQuoteStale={isQuoteStale}
            debug={intel?.debug}
          />
          <HeatmapCard
            sectorName="Materials"
            pages={[
              { label: "US", tickers: MATERIALS_SYMBOLS },
              { label: "Global (Non-US)", tickers: MATERIALS_SYMBOLS_GLOBAL },
            ]}
            liveQuotes={liveQuotes}
            isQuoteStale={isQuoteStale}
            debug={intel?.debug}
          />
          <HeatmapCard
            sectorName="Defence Tech"
            pages={[
              { label: "US", tickers: DEFENCE_TECH_SYMBOLS_US },
              { label: "Europe", tickers: DEFENCE_TECH_SYMBOLS_EU },
              { label: "Turkey", tickers: DEFENCE_TECH_SYMBOLS_TR },
              { label: "Middle East", tickers: DEFENCE_TECH_SYMBOLS_ME },
            ]}
            liveQuotes={liveQuotes}
            isQuoteStale={isQuoteStale}
            debug={intel?.debug}
          />
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <Card title="Flow Panel">
          <div className="flex items-center justify-between gap-3">
            <div className="flex flex-col">
              <span className={METRIC_LABEL_CLASS}>FlowScore</span>
              <span className="text-lg font-semibold text-[var(--accent)] tabular-nums">{flow?.flow_score ?? 0}</span>
            </div>
            <span className={METRIC_HELPER_CLASS}>0-100</span>
          </div>
          <div className="mt-4">
            <div className="text-xs font-semibold uppercase tracking-wide text-black/50">Evidence</div>
            <ul className="mt-2 list-disc space-y-1 pl-5 text-sm text-black/70">
              {(flow?.evidence ?? []).slice(0, 6).map((e, idx) => (
                <li key={idx}>{e}</li>
              ))}
            </ul>
          </div>
          <div className="mt-4">
            <div className="text-xs font-semibold uppercase tracking-wide text-black/50">Watch Metrics</div>
            <div className="mt-2 flex flex-wrap gap-2">
              {(flow?.watch_metrics ?? []).map((m) => (
                <Badge key={m}>{m}</Badge>
              ))}
            </div>
          </div>
        </Card>

        <RiskPanelV2
          rsi={riskLive?.rsi ?? null}
          flags={riskLive?.flags ?? []}
          derivatives={derivData}
          exchange={derivExchange}
          loading={derivLoading}
          fundingZ={riskLive?.funding_z ?? null}
          oiDelta={riskLive?.oi_delta ?? null}
          fearGreed={riskLive?.fear_greed ?? null}
        />
      </div>

      <Card title="Event Study" right={<Badge tone="info">hover markers</Badge>}>
        <EventStudyChart points={flow?.event_study ?? []} />
      </Card>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <Card
          title="Forecast Tracker"
          right={
            <div className="flex items-center gap-2 text-xs text-black/50">
              <span>TF: {timeframe}</span>
              <Badge tone={forecast?.direction === "UP" ? "good" : forecast?.direction === "DOWN" ? "bad" : "neutral"}>
                {forecast?.direction ?? "NEUTRAL"}
              </Badge>
            </div>
          }
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <span className="text-sm text-black/50">Target</span>
              <select
                value={forecastTarget}
                onChange={(e) => setForecastTarget(e.target.value)}
                className="rounded-2xl border border-black/10 bg-white/80 px-3 py-1 text-sm"
              >
                {["BTC", "ETH", "ALTS", "STABLES"].map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-2 text-xs text-black/50">
              <span>Guncelleme: {toTSIString(forecast?.ts_utc)}</span>
              <Badge tone={forecast && forecast.confidence >= 0.65 ? "signal" : "info"}>
                {forecast ? `${Math.round((forecast.confidence || 0) * 100)}%` : "—"}
              </Badge>
            </div>
          </div>
          <div className="mt-3 text-sm text-black/70">
            {forecast?.rationale_text ?? "Forecast verisi yok."}
          </div>
          <div className="mt-3 text-xs text-black/50">
            Son gecerlilik: {toTSIString(forecast?.expires_at_utc)}
          </div>
          {forecast?.drivers?.evidence?.length ? (
            <div className="mt-3 space-y-1 text-xs text-black/60">
              {forecast.drivers.evidence.slice(0, 5).map((line) => (
                <div key={line}>• {line}</div>
              ))}
            </div>
          ) : null}
          {forecast?.drivers?.rank_reason?.length ? (
            <div className="mt-3 flex flex-wrap gap-2 text-[11px] text-black/60">
              {forecast.drivers.rank_reason.map((tag) => (
                <span key={tag} className="rounded-full border px-2 py-0.5">
                  {tag}
                </span>
              ))}
            </div>
          ) : null}
          {forecast?.drivers?.feature_contrib?.length ? (
            <div className="mt-3 space-y-1 text-xs text-black/60">
              <div className="text-[11px] uppercase tracking-wide text-black/40">Market features</div>
              {forecast.drivers.feature_contrib.slice(0, 5).map((f) => (
                <div key={f.name} className="flex items-center justify-between gap-2">
                  <span>{f.name}</span>
                  <span>{f.contribution.toFixed(3)}</span>
                </div>
              ))}
            </div>
          ) : null}
          {forecast?.drivers?.news_contrib?.length ? (
            <div className="mt-3 space-y-1 text-xs text-black/60">
              <div className="text-[11px] uppercase tracking-wide text-black/40">News contributions</div>
              {forecast.drivers.news_contrib.slice(0, 3).map((n, idx) => (
                <div key={n.cluster_id ?? idx} className="flex items-center justify-between gap-2">
                  <span className="truncate">{n.headline ?? n.cluster_id}</span>
                  <span>{n.contribution.toFixed(3)}</span>
                </div>
              ))}
            </div>
          ) : null}
          {forecast?.drivers?.clusters?.length ? (
            <div className="mt-3 space-y-2">
              {forecast.drivers.clusters.slice(0, 2).map((c) => (
                <div key={c.cluster_id} className="rounded-xl border border-black/10 bg-white/70 p-3 text-xs">
                  <div className="font-semibold text-black">{c.headline}</div>
                  <div className="mt-1 flex flex-wrap items-center gap-2 text-black/60">
                    <Badge tone="info">impact {c.impact.toFixed(2)}</Badge>
                    <Badge tone="neutral">{c.source_tier}</Badge>
                    <span>cred {Math.round(c.credibility * 100)}%</span>
                  </div>
                </div>
              ))}
            </div>
          ) : null}
        </Card>

        <Card title="Consistency Metrics" right={<Badge tone="neutral">{metrics.length} tf</Badge>}>
          <div className="space-y-2">
            {TF_ORDER.map((tf) => {
              const m = metrics.find((row) => row.tf === tf);
              return (
                <div key={tf} className="rounded-2xl border border-black/10 bg-white/70 p-3 text-xs">
                  <div className="flex items-center justify-between">
                    <div className="font-semibold text-black">{tf}</div>
                    <div className="text-black/50">
                      hit 24h {m ? Math.round(m.hit_rate_24h * 100) : 0}% • 7d {m ? Math.round(m.hit_rate_7d * 100) : 0}%
                    </div>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-3 text-black/60">
                    <span>brier 24h {m ? m.brier_24h.toFixed(3) : "0.000"}</span>
                    <span>brier 7d {m ? m.brier_7d.toFixed(3) : "0.000"}</span>
                    <span>flip 7d {m ? m.flip_rate_7d.toFixed(2) : "0.00"}</span>
                    <span>coverage 24h {m ? Math.round(m.coverage_24h * 100) : 0}%</span>
                  </div>
                </div>
              );
            })}
          </div>
        </Card>
      </div>

      <Card
        title="Haber Akışı (Cluster)"
        right={
          <div className="flex flex-wrap items-center gap-2 text-xs text-black/50">
            <Badge>{filteredClusters.length} cluster</Badge>
            <span>Son tarama: {toTSIString(eventClusters?.last_scan_ts)}</span>
          </div>
        }
      >
        <div className="mb-4 flex flex-wrap items-center gap-2 text-xs text-black/60">
          <span>Min impact</span>
          <select
            value={impactMin}
            onChange={(e) => setImpactMin(Number(e.target.value))}
            className="rounded-2xl border border-black/10 bg-white/80 px-3 py-1 text-xs"
          >
            {[0, 20, 40, 60, 80].map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
          <span>Source tier</span>
          <select
            value={tierFilter}
            onChange={(e) => setTierFilter(e.target.value)}
            className="rounded-2xl border border-black/10 bg-white/80 px-3 py-1 text-xs"
          >
            {["all", "primary", "tier1", "tier2", "social"].map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
          <span>Tag</span>
          <Input
            value={tagFilter}
            onChange={(e) => setTagFilter(e.target.value)}
            placeholder="etiket ara"
            className="w-40 text-xs"
          />
        </div>
        <div className="space-y-4">
          {filteredClusters.length === 0 ? (
            <div className="text-sm text-black/50">Veri yok.</div>
          ) : (
            filteredClusters.map((evt) => {
              return (
                <div key={evt.cluster_id} className="rounded-xl border border-black/10 bg-white/70 p-4 shadow-sm">
                  <div className="text-sm font-semibold text-black">{evt.headline}</div>
                  <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-black/60">
                    <Badge tone="info">impact {evt.impact_score.toFixed(2)}</Badge>
                    <Badge tone="neutral">{evt.source_tier}</Badge>
                    <span>cred {Math.round(evt.credibility_score * 100)}%</span>
                    <span>sev {Math.round(evt.severity_score * 100)}%</span>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-black/50">
                    {(evt.tags || []).slice(0, 4).map((tag) => (
                      <span key={`${evt.cluster_id}-${tag}`} className="rounded-full border px-2 py-0.5">
                        {tag}
                      </span>
                    ))}
                  </div>
                  <div className="mt-3 flex flex-wrap items-center justify-between gap-3 text-xs text-black/50">
                    <div className="inline-flex items-center gap-2">
                      <Calendar className="h-3.5 w-3.5" />
                      <span>{toTSIString(evt.ts_utc)}</span>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {(evt.targets || []).slice(0, 3).map((t) => (
                        <span key={`${evt.cluster_id}-${t.asset}`} className="rounded-full border px-2 py-0.5 text-[11px]">
                          {t.asset} {t.relevance.toFixed(2)}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
              );
            })
          )}
        </div>
      </Card>

      <Card
        title="Gunluk Olasi Hareket (US)"
        right={
          <div className="flex flex-wrap items-center gap-2 text-xs text-black/50">
            <span>{dailyMovers?.asof ?? "—"}</span>
            <Badge tone={confidenceTone(moversSummary.confidence)}>{moversSummary.confidence}</Badge>
          </div>
        }
      >
        {dailyMovers?.items?.length ? (
          <div className="space-y-3">
            {dailyMovers.items.map((item) => {
              const meta = directionMeta(item.expected_direction);
              const Icon = meta.icon;
              const expectedMove = Number.isFinite(item.expected_move_band_pct)
                ? `±${item.expected_move_band_pct.toFixed(1)}%`
                : "Data unavailable";
              const why = item.why || "Kisa vadede oynaklik artabilir.";
              return (
                <div key={item.ticker} className="rounded-2xl border border-black/10 bg-white p-3">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div className="flex items-center gap-2">
                      <Badge tone="neutral" className="px-3 py-1 text-xs font-semibold">
                        {item.ticker}
                      </Badge>
                      <span className="text-xs text-black/60">{item.company_name ?? item.ticker}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge tone={meta.tone} className="gap-1">
                        <Icon className="h-3.5 w-3.5" />
                        {meta.label}
                      </Badge>
                      <Badge tone="info">{item.move_score}</Badge>
                    </div>
                  </div>
                  <div className="mt-2 text-sm text-black/70">
                    <span className="font-semibold text-black">{expectedMove}</span>
                    <span className="ml-2 text-black/60">{why}</span>
                  </div>
                  {item.catalysts?.length ? (
                    <div className="mt-2 flex flex-wrap gap-2">
                      {item.catalysts.slice(0, 2).map((c) => (
                        <Badge key={`${item.ticker}-${c}`} tone="info">
                          {c}
                        </Badge>
                      ))}
                    </div>
                  ) : null}
                  <div className="mt-2 text-[11px] text-black/45">
                    evidence: {item.evidence?.length ?? 0} • pricing: {item.pricing_status}
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="space-y-2">
            <div className="text-sm text-black/50">Bugun guclu katalizor yok veya veri yetersiz.</div>
            {dailyMovers?.debug?.reason_if_empty ? (
              <Badge tone="warn">{dailyMovers.debug.reason_if_empty}</Badge>
            ) : null}
          </div>
        )}
      </Card>

      <Card title="Ongoruler" right={<Badge tone="info">{insights.length}</Badge>}>
        <div className="space-y-3">
          {cryptoOutlook ? (
            <div className="rounded-2xl border border-black/10 bg-white p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="text-sm font-semibold text-black">BTC/ETH Bias</div>
                <Badge tone="info">{cryptoOutlook.confidence}</Badge>
              </div>
              <div className="mt-2 grid gap-3 text-sm text-black/70 sm:grid-cols-2">
                <div className="flex items-center justify-between rounded-xl border border-black/10 bg-white/80 px-3 py-2">
                  <span>BTC Bias</span>
                  <Badge tone={biasTone(cryptoOutlook.btc_bias)}>
                    {cryptoOutlook.btc_bias > 0 ? "+" : ""}
                    {cryptoOutlook.btc_bias}
                  </Badge>
                </div>
                <div className="flex items-center justify-between rounded-xl border border-black/10 bg-white/80 px-3 py-2">
                  <span>ETH Bias</span>
                  <Badge tone={biasTone(cryptoOutlook.eth_bias)}>
                    {cryptoOutlook.eth_bias > 0 ? "+" : ""}
                    {cryptoOutlook.eth_bias}
                  </Badge>
                </div>
              </div>
              <ul className="mt-2 list-disc space-y-1 pl-4 text-xs text-black/60">
                {cryptoOutlook.drivers.slice(0, 3).map((d, idx) => (
                  <li key={`crypto-driver-${idx}`}>{d}</li>
                ))}
              </ul>
              <div className="mt-2 flex flex-wrap gap-2">
                {cryptoOutlook.watch_metrics.slice(0, 3).map((m) => (
                  <Badge key={`crypto-metric-${m}`} tone="neutral">
                    {m}
                  </Badge>
                ))}
              </div>
            </div>
          ) : null}
          {insights.length === 0 ? (
            <div className="text-sm text-black/50">Veri yok.</div>
          ) : (
            insights.map((insight, idx) => (
              <div key={`${insight.title}-${idx}`} className="rounded-2xl border border-black/10 bg-white p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="text-sm font-semibold text-black">{insight.title}</div>
                  <Badge tone={insight.severity === "high" ? "warn" : insight.severity === "med" ? "info" : "neutral"}>
                    {insight.severity}
                  </Badge>
                </div>
                <ul className="mt-2 list-disc space-y-1 pl-4 text-xs text-black/60">
                  {insight.bullets.map((b, i) => (
                    <li key={`${insight.title}-b-${i}`}>{b}</li>
                  ))}
                </ul>
                {insight.tags.length > 0 ? (
                  <div className="mt-2 flex flex-wrap gap-2">
                    {insight.tags.map((t) => (
                      <Badge key={`${insight.title}-${t}`} tone="info">
                        {t}
                      </Badge>
                    ))}
                  </div>
                ) : null}
              </div>
            ))
          )}
        </div>
      </Card>

      <Card title="Data Status" className="border-black/10">
        <div className="grid gap-3 text-xs text-black/65 md:grid-cols-2">
          <div>
            <div className="mb-1 font-semibold text-black">Backend-Go</div>
            {backendHealth ? (
              <div className="space-y-1">
                {ENV_KEYS.map((key) => (
                  <div key={`be-${key}`} className="flex items-center justify-between gap-2">
                    <span className="truncate">{key}</span>
                    <StatusPill ok={backendHealth.env?.[key]} />
                  </div>
                ))}
                {backendHealth.features ? (
                  <div className="pt-2 text-[11px] text-black/50">
                    {Object.entries(backendHealth.features).map(([key, value]) => (
                      <div key={`be-feature-${key}`} className="flex items-center justify-between gap-2">
                        <span className="truncate">{key}</span>
                        <StatusPill ok={value} />
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>
            ) : (
              <div className="text-rose-700">Backend health unreachable</div>
            )}
          </div>
          <div>
            <div className="mb-1 font-semibold text-black">Analytics-Py</div>
            {analyticsHealth ? (
              <div className="space-y-1">
                {ENV_KEYS.map((key) => (
                  <div key={`py-${key}`} className="flex items-center justify-between gap-2">
                    <span className="truncate">{key}</span>
                    <StatusPill ok={analyticsHealth.env?.[key]} />
                  </div>
                ))}
                {analyticsHealth.features ? (
                  <div className="pt-2 text-[11px] text-black/50">
                    {Object.entries(analyticsHealth.features).map(([key, value]) => (
                      <div key={`py-feature-${key}`} className="flex items-center justify-between gap-2">
                        <span className="truncate">{key}</span>
                        <StatusPill ok={value} />
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>
            ) : (
              <div className="text-rose-700">Analytics health unreachable</div>
            )}
          </div>
        </div>
        <div className="mt-3 flex items-center justify-between text-[11px] text-black/55">
          <span>Frontend NEXT_PUBLIC_API_BASE</span>
          <StatusPill ok={frontendBasePresent} />
        </div>
      </Card>
    </div>
  );
}
