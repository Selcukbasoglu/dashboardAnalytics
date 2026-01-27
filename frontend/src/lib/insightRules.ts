import type { FlowPanel, MarketSnapshot, NewsItem, RiskPanel } from "@/app/api/types";

type InsightCard = {
  title: string;
  severity: "low" | "med" | "high";
  bullets: string[];
  tags: string[];
};

const INSIGHTS_V2_ENABLED =
  (process.env.NEXT_PUBLIC_INSIGHTS_V2_ENABLED ?? "true").toLowerCase() in
  { "1": true, "true": true, "yes": true, "on": true };

function pct(n?: number | null) {
  if (n === undefined || n === null || Number.isNaN(n)) return "—";
  const s = n >= 0 ? "+" : "";
  return `${s}${n.toFixed(2)}%`;
}

function severityFromChange(n?: number | null): "low" | "med" | "high" {
  if (n === undefined || n === null || Number.isNaN(n)) return "low";
  const v = Math.abs(n);
  if (v >= 1.5) return "high";
  if (v >= 0.7) return "med";
  return "low";
}

function countByCategory(news: NewsItem[], category: string) {
  return news.filter((n) => n.category === category).length;
}

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

function recencyWeight(ts?: string | null) {
  if (!ts) return 0.2;
  const t = new Date(ts).getTime();
  if (!Number.isFinite(t)) return 0.2;
  const hours = (Date.now() - t) / 36e5;
  if (hours <= 1) return 1.0;
  if (hours <= 6) return 0.7;
  if (hours <= 24) return 0.4;
  return 0.2;
}

function topN(map: Record<string, number>, n: number) {
  return Object.entries(map)
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([k, v]) => ({ key: k, value: v }));
}

export function buildRuleInsights(
  market?: MarketSnapshot,
  news: NewsItem[] = [],
  flow?: FlowPanel,
  risk?: RiskPanel,
  debugNotes: string[] = []
): InsightCard[] {
  const out: InsightCard[] = [];
  const qqq = market?.yahoo?.qqq_chg_24h;
  const dxy = market?.yahoo?.dxy_chg_24h;
  const oil = market?.yahoo?.oil_chg_24h;

  const techCount = countByCategory(news, "teknoloji");
  const energyCount = countByCategory(news, "enerji");
  const cryptoCount = countByCategory(news, "kripto");

  if (qqq !== undefined && qqq !== null) {
    out.push({
      title: "Teknoloji hisseleri momentum",
      severity: severityFromChange(qqq),
      bullets: [
        `QQQ 24s degisim: ${pct(qqq)}`,
        `Teknoloji haber yogunlugu: ${techCount}`,
        qqq <= -0.5
          ? "Kisa vadede risk-on zayif, teknoloji hisselerinde baski beklenir."
          : qqq >= 0.5
            ? "Momentum destekli, teknoloji hisselerinde yukari egilim gozlenebilir."
            : "Degisim sinirli, secici hareketler beklenir.",
      ],
      tags: ["QQQ", "tech"],
    });
  }

  if (oil !== undefined && oil !== null) {
    out.push({
      title: "Enerji fiyatlari etkisi",
      severity: severityFromChange(oil),
      bullets: [
        `Oil 24s degisim: ${pct(oil)}`,
        `Enerji haber yogunlugu: ${energyCount}`,
        oil >= 2
          ? "Enerji maliyeti yukseliyor, enerji hisselerinde kisa vadede destek gorulebilir."
          : oil <= -2
            ? "Enerji maliyeti geriliyor, enerji hisselerinde karisik seyir olasiligi artar."
            : "Fiyat degisimi sinirli, haber akisi belirleyici olabilir.",
      ],
      tags: ["oil", "energy"],
    });
  }

  if (dxy !== undefined && dxy !== null) {
    out.push({
      title: "Dolar ve risk algisi",
      severity: severityFromChange(dxy),
      bullets: [
        `DXY 24s degisim: ${pct(dxy)}`,
        dxy >= 0.5
          ? "Dolar guclenmesi riskli varliklar icin baski yaratabilir."
          : dxy <= -0.5
            ? "Dolar zayifligi riskli varliklara destek saglayabilir."
            : "Dolar tarafinda belirgin bir sinyal yok.",
      ],
      tags: ["dxy", "macro"],
    });
  }

  if (flow?.flow_score !== undefined) {
    out.push({
      title: "Kripto akisi ve duyarlilik",
      severity: flow.flow_score <= 40 ? "med" : flow.flow_score >= 60 ? "med" : "low",
      bullets: [
        `FlowScore: ${flow.flow_score}`,
        `Kripto haber yogunlugu: ${cryptoCount}`,
        flow.flow_score <= 40
          ? "Akis zayif, kisa vadede temkinli durus uygun olabilir."
          : flow.flow_score >= 60
            ? "Akis guclu, kisa vadede momentum artisi izlenebilir."
            : "Akis dengeli, haber basliklari belirleyici olabilir.",
      ],
      tags: ["flow", "crypto"],
    });
  }

  if (risk?.flags?.length) {
    out.push({
      title: "Risk bayraklari",
      severity: "high",
      bullets: [`Aktif bayraklar: ${risk.flags.join(", ")}`],
      tags: ["risk"],
    });
  }

  if (!INSIGHTS_V2_ENABLED) {
    return out;
  }

  const eventTypeCounts: Record<string, number> = {};
  const impactCounts: Record<string, number> = {};
  const sectorCounts: Record<string, number> = {};
  const sectorDirCounts: Record<string, Record<string, number>> = {};
  const personCounts: Record<string, number> = {};
  const personMeta: Record<string, Record<string, number>> = {};
  let lowSignalCount = 0;

  for (const item of news) {
    const rel = clamp((item.relevance_score ?? item.score ?? 0) / 100, 0, 1);
    const qual = clamp((item.quality_score ?? 0) / 100, 0, 1);
    const wBase = rel * qual;
    let w = wBase * recencyWeight(item.publishedAtISO);
    const impactChannels = item.impact_channel ?? [];
    if (item.event_type === "OTHER" && (!impactChannels || impactChannels.length === 0)) {
      lowSignalCount += 1;
      w *= 0.25;
    }
    if (!w) continue;
    if (item.event_type) {
      eventTypeCounts[item.event_type] = (eventTypeCounts[item.event_type] ?? 0) + w;
    }
    for (const ch of impactChannels || []) {
      impactCounts[ch] = (impactCounts[ch] ?? 0) + w;
    }
    for (const si of item.sector_impacts ?? []) {
      if (!si?.sector) continue;
      sectorCounts[si.sector] = (sectorCounts[si.sector] ?? 0) + w;
      const dir = (si.direction || "NEUTRAL").toUpperCase();
      sectorDirCounts[si.sector] = sectorDirCounts[si.sector] || {};
      sectorDirCounts[si.sector][dir] = (sectorDirCounts[si.sector][dir] ?? 0) + w;
    }
    const pe = item.person_event;
    const personKey = pe?.actor_name || pe?.actor_group || null;
    if (personKey) {
      personCounts[personKey] = (personCounts[personKey] ?? 0) + w;
      const meta = (personMeta[personKey] = personMeta[personKey] || {});
      if (pe?.statement_type) {
        meta[`type:${pe.statement_type}`] = (meta[`type:${pe.statement_type}`] ?? 0) + w;
      }
      if (pe?.stance) {
        meta[`stance:${pe.stance}`] = (meta[`stance:${pe.stance}`] ?? 0) + w;
      }
    } else if (item.tags?.includes("PERSONAL_MATCH")) {
      const key = "PERSONAL_MATCH";
      personCounts[key] = (personCounts[key] ?? 0) + w;
    }
  }

  const topEventTypes = topN(eventTypeCounts, 2);
  const topChannels = topN(impactCounts, 2);
  out.push({
    title: "Katalizor haritasi",
    severity: topEventTypes.length ? "med" : "low",
    bullets: [
      topEventTypes.length
        ? `Baskin event_type: ${topEventTypes.map((t) => t.key).join(", ")}`
        : "Event type etiketleri zayif veya eksik.",
      topChannels.length
        ? `Baskin etki kanali: ${topChannels.map((t) => t.key).join(", ")}`
        : "Etki kanali etiketleri eksik/az (veri eksik).",
      `Toplam ${news.length} haber, low-signal ${lowSignalCount} adet`,
    ],
    tags: ["catalyst", "labels"],
  });

  const riskBias = {
    risk_off: 0,
    risk_on: 0,
    liquidity: 0,
    supply: 0,
  };
  for (const [k, v] of Object.entries(impactCounts)) {
    if (k === "regülasyon_baskısı" || k === "risk_primi") riskBias.risk_off += v;
    if (k === "büyüme") riskBias.risk_on += v;
    if (k === "likidite") riskBias.liquidity += v;
    if (k === "arz_zinciri") riskBias.supply += v;
  }
  const biasPairs = Object.entries(riskBias).sort((a, b) => b[1] - a[1]);
  const topBias = biasPairs[0]?.[0];
  const biasBullets = [];
  if (topBias === "risk_off") {
    biasBullets.push("Haber akisi risk primi / regulasyon baskisi kanalinda yogunlasiyor.");
  } else if (topBias === "risk_on") {
    biasBullets.push("Büyüme kanalinda yogunluk var; risk-on temasi one cikiyor.");
  } else if (topBias === "liquidity") {
    biasBullets.push("Likidite/akis temasi one cikiyor.");
  } else if (topBias === "supply") {
    biasBullets.push("Arz zinciri etkileri one cikiyor.");
  } else {
    biasBullets.push("Etki kanallari daginik; net bir rejim sinyali yok.");
  }
  if (dxy !== undefined && dxy !== null) {
    if (topBias === "risk_off" && dxy >= 0.5) biasBullets.push("DXY guclenmesiyle eslik ediyor.");
    if (topBias === "risk_on" && dxy <= -0.5) biasBullets.push("DXY zayiflamasiyla eslik ediyor.");
  }
  out.push({
    title: "Etki kanali → risk rejimi",
    severity: topBias === "risk_off" || topBias === "risk_on" ? "med" : "low",
    bullets: biasBullets,
    tags: ["impact", "regime"],
  });

  const topSectors = topN(sectorCounts, 3);
  if (topSectors.length) {
    const sectorLines = topSectors.map((s) => {
      const dirMap = sectorDirCounts[s.key] || {};
      const dir = Object.entries(dirMap).sort((a, b) => b[1] - a[1])[0]?.[0] || "NEUTRAL";
      return `${s.key}: ${dir}`;
    });
    out.push({
      title: "Sektor etkileri",
      severity: "med",
      bullets: sectorLines,
      tags: ["sector"],
    });
  } else {
    out.push({
      title: "Sektor etkileri",
      severity: "low",
      bullets: ["Sektor etkisi etiketleri su an cogunlukla null (veri eksik)."],
      tags: ["sector"],
    });
  }

  const topPersons = topN(personCounts, 3);
  if (topPersons.length) {
    const topKey = topPersons[0].key;
    const meta = personMeta[topKey] || {};
    const topType = topN(
      Object.fromEntries(Object.entries(meta).filter(([k]) => k.startsWith("type:")).map(([k, v]) => [k, v])),
      1
    )[0]?.key?.replace("type:", "");
    const topStance = topN(
      Object.fromEntries(Object.entries(meta).filter(([k]) => k.startsWith("stance:")).map(([k, v]) => [k, v])),
      1
    )[0]?.key?.replace("stance:", "");
    out.push({
      title: "Lider / kisi katalizorleri",
      severity: "med",
      bullets: [
        `En cok gecen: ${topKey}`,
        topType ? `Statement tipi: ${topType}` : "Statement tipi: belirsiz",
        topStance ? `Stance: ${topStance}` : "Stance: belirsiz",
      ],
      tags: ["person"],
    });
  } else {
    const reason =
      debugNotes.find((n) => n.includes("gdelt_rate_limited")) ||
      debugNotes.find((n) => n.includes("event_time_budget_exceeded")) ||
      debugNotes.find((n) => n.includes("personal_source=none")) ||
      null;
    out.push({
      title: "Lider / kisi katalizorleri",
      severity: "low",
      bullets: [reason ? `PERSON akisi zayif: ${reason}` : "PERSON olaylari su an beslenmiyor."],
      tags: ["person"],
    });
  }

  return out;
}
