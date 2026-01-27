export function cx(...cls: Array<string | false | null | undefined>) {
  return cls.filter(Boolean).join(" ");
}

export function fmtNum(n?: number | null, digits = 2) {
  return formatNumber(n, digits);
}

export function fmtUsd(n?: number | null, digits = 0) {
  if (n === undefined || n === null || Number.isNaN(n)) return "Data unavailable";
  return "$" + n.toLocaleString("en-US", { maximumFractionDigits: digits });
}

export function pct(n?: number | null, digits = 2) {
  return formatPercent(n, digits);
}

export function formatNumber(n?: number | null, digits = 2) {
  if (n === undefined || n === null || Number.isNaN(n)) return "Data unavailable";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(n);
}

export function formatCurrencyCompact(n?: number | null, digits = 1) {
  if (n === undefined || n === null || Number.isNaN(n)) return "Data unavailable";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: digits,
  }).format(n);
}

export function formatPercent(n?: number | null, digits = 2) {
  if (n === undefined || n === null || Number.isNaN(n)) return "Data unavailable";
  const sign = n > 0 ? "+" : "";
  return `${sign}${formatNumber(n, digits)}%`;
}

export function toTSIString(iso?: string) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleString("tr-TR", {
    timeZone: "Europe/Istanbul",
    year: "numeric",
    month: "long",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function pctDiff(a?: number, b?: number) {
  if (a == null || b == null || b === 0) return null;
  return ((a - b) / b) * 100;
}
