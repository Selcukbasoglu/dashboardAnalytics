"use client";

import React from "react";
import { Activity, Info } from "lucide-react";

import type { DerivativesResponse } from "@/app/api/types";
import { cx, fmtNum } from "@/lib/utils";

type RiskPanelV2Props = {
  rsi: number | null;
  flags: string[];
  derivatives: DerivativesResponse | null;
  exchange: string;
  loading?: boolean;
  fundingZ?: number | null;
  oiDelta?: number | null;
  fearGreed?: number | null;
};

function StatusChip({ status }: { status: "ENABLED" | "DISABLED" | "DEGRADED" }) {
  const tone =
    status === "ENABLED"
      ? "bg-emerald-100 text-emerald-800 border-emerald-200"
      : status === "DEGRADED"
        ? "bg-amber-100 text-amber-800 border-amber-200"
        : "bg-rose-100 text-rose-800 border-rose-200";
  return <span className={cx("rounded-full border px-2 py-1 text-xs font-semibold", tone)}>{status}</span>;
}

function FlagChip({ count }: { count: number }) {
  const tone = count > 0 ? "bg-rose-100 text-rose-800 border-rose-200" : "bg-emerald-100 text-emerald-800 border-emerald-200";
  return (
    <span className={cx("rounded-full border px-2 py-1 text-xs font-semibold", tone)}>
      {count > 0 ? `${count} flags` : "No flags"}
    </span>
  );
}

function MetricCard({
  label,
  value,
  helper,
  tooltip,
  loading,
  error,
}: {
  label: string;
  value: string;
  helper: string;
  tooltip: string;
  loading?: boolean;
  error?: string | null;
}) {
  const tooltipLines = [helper, tooltip].filter(Boolean);
  return (
    <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
      <div className="flex items-center justify-between">
        <div className="text-xs font-semibold uppercase tracking-wide text-black/60">{label}</div>
        <span className="group relative inline-flex items-center">
          <Info className="h-3.5 w-3.5 text-black/40" />
          {tooltipLines.length ? (
            <span className="pointer-events-none absolute right-0 top-5 z-10 w-56 rounded-xl border border-black/10 bg-white px-2 py-1 text-[11px] text-black/70 opacity-0 shadow transition group-hover:opacity-100">
              {tooltipLines.map((line) => (
                <div key={line}>{line}</div>
              ))}
            </span>
          ) : null}
        </span>
      </div>
      <div className={cx("mt-2 text-lg font-semibold text-black tabular-nums", loading && "animate-pulse text-black/30")}>
        {value}
      </div>
      {error ? <div className="mt-2 text-xs text-rose-600">{error}</div> : null}
    </div>
  );
}

export default function RiskPanelV2({
  rsi,
  flags,
  derivatives,
  exchange,
  loading,
  fundingZ: fundingZOverride,
  oiDelta: oiDeltaOverride,
  fearGreed,
}: RiskPanelV2Props) {
  const health = derivatives?.health;
  const status: "ENABLED" | "DISABLED" | "DEGRADED" =
    health?.degraded_mode ? "DEGRADED" : health?.error ? "DISABLED" : "ENABLED";
  const lastUpdated = derivatives?.ts ?? "—";
  const fundingZ = fundingZOverride ?? derivatives?.computed?.funding_z ?? null;
  const oiDelta = oiDeltaOverride ?? derivatives?.computed?.oi_delta_pct ?? null;
  const fundingError = health?.error ? `Data unavailable (${health.error})` : null;
  const oiError = health?.error ? `Data unavailable (${health.error})` : null;
  const rsiValue = rsi == null ? "—" : fmtNum(rsi, 1);

  return (
    <div className="rounded-3xl border border-black/10 bg-[var(--panel)] p-5 shadow-[0_18px_40px_-30px_rgba(15,25,26,0.5)] backdrop-blur">
      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div className="text-sm font-semibold text-[color:var(--ink)]">Risk Panel</div>
        <div className="flex flex-wrap items-center gap-2 text-xs text-black/60">
          <span className="rounded-full border border-black/10 bg-white/80 px-3 py-1 text-xs uppercase">
            {exchange}
          </span>
          <div className="inline-flex items-center gap-2 rounded-full border border-black/10 bg-white/70 px-3 py-1">
            <Activity className="h-3.5 w-3.5" />
            Son güncelleme: {lastUpdated}
          </div>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-center gap-3 rounded-2xl border border-black/10 bg-white/70 px-4 py-3 text-xs text-black/60">
        <FlagChip count={flags.length} />
        <StatusChip status={status} />
        <span>Kaynak: {exchange.toUpperCase()}</span>
        {health?.degraded_mode ? (
          <span>Degraded (last good: {health.last_good_age_s}s)</span>
        ) : (
          <span>{health?.cache_hit ? "Cache hit" : "Live fetch"}</span>
        )}
      </div>
      {status !== "ENABLED" ? (
        <div className="mt-2 text-xs text-black/60">
          Veri gelmiyor: {health?.error || "derivatives not available"} • Deneyin: borsayı değiştirin veya birkaç dakika sonra yeniden deneyin.
        </div>
      ) : null}

      <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-4">
        <MetricCard
          label="Fear & Greed"
          value={fearGreed == null ? "—" : fmtNum(fearGreed, 0)}
          helper="0-100 • düşük = fear, yüksek = greed"
          tooltip="Fear & Greed: RSI + funding z-score + OI delta bileşimi. Kaynak: Binance."
          loading={loading}
          error={fearGreed == null ? "Data unavailable (fear/greed missing)" : null}
        />
        <MetricCard
          label="RSI"
          value={rsiValue}
          helper="14-periyot • 30 altı oversold, 70 üstü overbought"
          tooltip="RSI: 14-periyot, momentum seviyesi."
          loading={loading}
          error={!rsiValue || rsiValue === "—" ? "Data unavailable (RSI missing)" : null}
        />
        <MetricCard
          label="Funding Z"
          value={fundingZ == null ? "—" : fmtNum(fundingZ, 2)}
          helper="7d dağılım • + => long crowded, - => short crowded"
          tooltip="Funding Z: son 7 gün funding rate serisi üzerinden mean/std ile z-score. Kaynak: Binance."
          loading={loading}
          error={fundingZ == null ? fundingError ?? "Data unavailable (funding missing)" : null}
        />
        <MetricCard
          label="OI Δ"
          value={oiDelta == null ? "—" : `${fmtNum(oiDelta, 2)}%`}
          helper="24h • trend teyidi / short build-up"
          tooltip="OI Δ: 24h önceki open interest ile yüzde değişim. Kaynak: Binance."
          loading={loading}
          error={oiDelta == null ? oiError ?? "Data unavailable (oi missing)" : null}
        />
      </div>

    </div>
  );
}
