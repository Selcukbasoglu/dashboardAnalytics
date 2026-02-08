import type { NewsItem } from "@/app/api/types";

type GdeltRaw = {
  title: string;
  url: string;
  domain?: string | null;
  seendate?: string | null;
};

const TAGS = [
  {
    tag: "CEO",
    w: 3,
    re: [/\bCEO\b/i, /earnings/i, /guidance/i, /chief executive/i, /results/i, /CFO/i],
  },
  {
    tag: "Devlet",
    w: 3,
    re: [/\bpresident\b/i, /prime minister/i, /sanctions?/i, /white house/i, /kremlin/i, /\bfed\b/i, /\bboj\b/i, /\becb\b/i],
  },
  {
    tag: "Savaş",
    w: 4,
    re: [/\bwar\b/i, /ceasefire/i, /missile/i, /attack/i, /conflict/i, /ukraine/i, /gaza/i, /iran/i, /yemen/i, /red sea/i],
  },
  {
    tag: "Enerji",
    w: 4,
    re: [/oil/i, /brent/i, /wti/i, /\bgas\b/i, /lng/i, /opec/i, /pipeline/i, /energy crisis/i, /shipping/i],
  },
  
{
  tag: "Yaklaşan",
  w: 5,
  re: [/will speak/i, /to speak/i, /scheduled/i, /set to/i, /to address/i, /press conference/i, /remarks/i, /statement/i],
    },

  
];

function tagOne(title: string) {
  const tags: string[] = [];
  let score = 0;

  for (const t of TAGS) {
    if (t.re.some((r) => r.test(title))) {
      tags.push(t.tag);
      score += t.w;
    }
  }
  if (/bitcoin|ethereum|crypto|stablecoin|etf/i.test(title)) score += 1;
  if (/breaking|exclusive|urgent/i.test(title)) score += 1;

  return { tags, score };
}

function parseGdeltSeenDate(seen?: string): string | null {
  if (!seen) return null;

  // GDELT sıkça YYYYMMDDHHMMSS (14 haneli) verir
  if (/^\d{14}$/.test(seen)) {
    const y = seen.slice(0, 4);
    const mo = seen.slice(4, 6);
    const d = seen.slice(6, 8);
    const hh = seen.slice(8, 10);
    const mm = seen.slice(10, 12);
    const ss = seen.slice(12, 14);
    const iso = `${y}-${mo}-${d}T${hh}:${mm}:${ss}Z`;
    const dt = new Date(iso);
    return Number.isNaN(dt.getTime()) ? null : dt.toISOString();
  }

  // Diğer olası formatlar
  const dt = new Date(seen);
  return Number.isNaN(dt.getTime()) ? null : dt.toISOString();
}

function hasCJK(text: string) {
  // CJK Unified Ideographs + Hiragana/Katakana + Hangul
  return /[\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF]/.test(text);
}

function isMostlyLatin(text: string) {
  // Çok kaba bir heuristik: harflerin önemli kısmı Latin ise true
  const letters = (text.match(/[A-Za-zĞÜŞİÖÇğüşıöç]/g) ?? []).length;
  const total = (text.match(/[^\s]/g) ?? []).length;
  if (total === 0) return false;
  return letters / total >= 0.20; // eşik: 35%
}

export function normalizeNews(raw: GdeltRaw[]): NewsItem[] {
  return raw
    .filter((n) => {
      const t = n.title ?? "";
      if (!t) return false;

      // CJK (Çince/Japonca/Korece) gibi başlıkları ele
      if (hasCJK(t)) return false;

      // Çok sıkı eşik yüzünden her şey eleniyordu → eşiği gevşet
      if (!isMostlyLatin(t)) return false;

      return true;
    })
    .map((n) => {
      const { tags, score } = tagOne(n.title);
      return {
        title: n.title,
        url: n.url,
        source: n.domain ?? "gdelt",
        publishedAtISO: parseGdeltSeenDate(n.seendate ?? undefined),
        tags,
        score,
        tier_score: score,
      };
    })
    .sort((a, b) => b.score - a.score);
}
