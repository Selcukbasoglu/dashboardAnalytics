from __future__ import annotations

import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Tuple

from app.engine.labels import PERSONAL_GROUPS, canonical_person_name, person_id

PERSON_GROUP_PRIORITY = [
    "CENTRAL_BANK_HEADS",
    "REGULATORS",
    "ENERGY_MINISTERS",
    "DEFENSE_SECURITY",
    "EU_OFFICIALS",
    "REGIONAL_POWER_LEADERS",
]

STANCE_RULES = {
    "CENTRAL_BANK_HEADS": {
        "HAWKISH": [
            "rate hike",
            "higher for longer",
            "inflation persistent",
            "tightening",
            "restrictive",
            "not ready to cut",
            "hold rates",
        ],
        "DOVISH": [
            "rate cut",
            "disinflation",
            "easing",
            "lower rates",
            "pause",
            "soft landing",
            "cuts soon",
        ],
    },
    "REGULATORS": {
        "RISK_ESCALATE": [
            "lawsuit",
            "enforcement",
            "ban",
            "fine",
            "investigation",
            "approval denied",
            "cease and desist",
        ],
        "RISK_DEESCALATE": [
            "approval",
            "clarity",
            "framework",
            "license granted",
            "settlement",
        ],
    },
    "ENERGY_MINISTERS": {
        "RISK_ESCALATE": [
            "cut output",
            "supply disruption",
            "sanctions",
            "attack on shipping",
            "output cut",
            "production cut",
        ],
        "RISK_DEESCALATE": [
            "increase output",
            "restore supply",
            "ceasefire",
            "shipping normal",
            "output increase",
            "production increase",
        ],
    },
    "DEFENSE_SECURITY": {
        "RISK_ESCALATE": [
            "escalation",
            "attack",
            "missile",
            "drone strike",
            "mobilization",
        ],
        "RISK_DEESCALATE": [
            "ceasefire",
            "de-escalation",
            "peace talks",
            "truce",
        ],
    },
}

STATEMENT_TYPE_RULES = {
    "TWEET": [r"\btweet\b", r"\bposted on x\b", r"\bx.com\b", r"\btwitter\b"],
    "PRESS": [r"\bpress conference\b", r"\bpress briefing\b", r"\bpress\b", r"\bstatement\b", r"\bannounces?\b"],
    "INTERVIEW": [r"\binterview\b", r"\bq&a\b", r"\bpodcast\b"],
    "SPEECH": [r"\bspeech\b", r"\baddress\b", r"\bremarks\b", r"\btestimony\b", r"\bkeynote\b"],
}

AMBIGUITY_WORDS = [
    "may",
    "could",
    "might",
    "reportedly",
    "sources",
    "rumor",
    "rumour",
    "likely",
    "possible",
]

NUMERIC_SIGNAL_RE = re.compile(
    r"\b\d+(?:\.\d+)?\s?(?:%|bp|bps|bpd|mbpd|million|billion|trillion)\b",
    flags=re.IGNORECASE,
)

PERSON_NAME_TO_GROUP = {name: group for group, names in PERSONAL_GROUPS.items() for name in names}


def _get(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _combined_text(item: Any) -> str:
    title = _safe_text(_get(item, "title"))
    desc = _safe_text(_get(item, "description") or _get(item, "snippet") or _get(item, "summary"))
    content = _safe_text(_get(item, "content_text"))
    return " ".join([t for t in [title, desc, content] if t]).strip()


def _select_group(tags: list[str], actor_name: str | None) -> str | None:
    tag_set = set(tags or [])
    for group in PERSON_GROUP_PRIORITY:
        if group in tag_set:
            return group
    if actor_name and actor_name in PERSON_NAME_TO_GROUP:
        return PERSON_NAME_TO_GROUP[actor_name]
    for t in tag_set:
        if t in PERSONAL_GROUPS:
            return t
    return None


def _find_actor_name(entities: list[str], title: str, actor_group: str | None) -> str | None:
    if entities:
        if actor_group:
            for ent in entities:
                if ent in PERSONAL_GROUPS.get(actor_group, []):
                    return ent
        for ent in entities:
            if ent in PERSON_NAME_TO_GROUP:
                return ent

    title_lower = title.lower()
    best = None
    best_pos = None
    for name in PERSON_NAME_TO_GROUP.keys():
        pos = title_lower.find(name.lower())
        if pos == -1:
            continue
        if best is None or pos < best_pos:
            best = name
            best_pos = pos
    return best


def _statement_type(text: str, url: str | None) -> str:
    text_lower = text.lower()
    if url and re.search(r"(twitter\.com|x\.com)", url, flags=re.IGNORECASE):
        return "TWEET"
    for stmt_type, patterns in STATEMENT_TYPE_RULES.items():
        if any(re.search(p, text_lower, flags=re.IGNORECASE) for p in patterns):
            return stmt_type
    return "OTHER"


def _count_hits(text_lower: str, patterns: list[str]) -> int:
    return sum(1 for p in patterns if re.search(p, text_lower, flags=re.IGNORECASE))


def detect_stance(actor_group: str | None, text: str) -> Tuple[str, Dict[str, int], bool]:
    if not actor_group:
        return "UNKNOWN", {}, False
    rules = STANCE_RULES.get(actor_group, {})
    text_lower = text.lower()
    counts: Dict[str, int] = {}
    for stance, patterns in rules.items():
        counts[stance] = _count_hits(text_lower, patterns)
    active = [k for k, v in counts.items() if v > 0]
    if len(active) > 1:
        return "UNKNOWN", counts, True
    if len(active) == 1:
        return active[0], counts, False
    if actor_group in STANCE_RULES:
        return "NEUTRAL", counts, False
    return "UNKNOWN", counts, False


def map_impact(actor_group: str | None, stance: str) -> Tuple[list[str], list[str], str]:
    if actor_group == "CENTRAL_BANK_HEADS":
        channels = ["faiz", "enflasyon", "likidite"]
        if stance == "HAWKISH":
            return channels, ["risk_off", "rates_up"], "negatif: sikilastirma tonu risk primini artirir"
        if stance == "DOVISH":
            return channels, ["risk_on", "rates_down"], "pozitif: gevseme sinyali likidite algisini artirir"
        return channels, [], "karisik: net yon sinyali yok"
    if actor_group == "REGULATORS":
        channels = ["regülasyon_baskısı"]
        if stance == "RISK_ESCALATE":
            return channels, ["crypto_reg_risk", "risk_off"], "negatif: denetim baskisi risk algisini artirir"
        if stance == "RISK_DEESCALATE":
            return channels, ["risk_on", "crypto_beta_up"], "pozitif: duzenleme netligi risk primini azaltir"
        return channels, [], "karisik: reg ton belirsiz"
    if actor_group == "ENERGY_MINISTERS":
        channels = ["enerji_maliyeti", "enflasyon", "risk_primi"]
        if stance == "RISK_ESCALATE":
            return channels, ["energy_up", "risk_off"], "negatif: arz riski maliyetleri yukari iter"
        if stance == "RISK_DEESCALATE":
            return channels, ["energy_down", "risk_on"], "pozitif: arz rahatlamasi maliyetleri dusurur"
        return channels, [], "karisik: arz/uretim mesaji net degil"
    if actor_group == "DEFENSE_SECURITY":
        channels = ["risk_primi"]
        if stance == "RISK_ESCALATE":
            return channels, ["risk_off", "defense_up"], "negatif: jeopolitik risk primini artirir"
        if stance == "RISK_DEESCALATE":
            return channels, ["risk_on"], "pozitif: tansiyon dususu risk primini azaltir"
        return channels, [], "karisik: savunma mesajinda net sinyal yok"
    return ["risk_primi"], [], "karisik: etki kanali belirsiz"


def extract_person_event(item: Any) -> dict | None:
    tags = list(_get(item, "tags") or [])
    is_person = "PERSONAL" in tags or any(t in PERSONAL_GROUPS for t in tags)
    if not is_person:
        return None

    title = _safe_text(_get(item, "title"))
    entities = list(_get(item, "entities") or [])
    actor_group = _select_group(tags, None)
    actor_name = _find_actor_name(entities, title, actor_group)
    if actor_name:
        actor_name = canonical_person_name(actor_name)
    actor_group = _select_group(tags, actor_name)

    text = _combined_text(item)
    statement_type = _statement_type(text, _get(item, "url"))
    stance, _, _ = detect_stance(actor_group, text)
    impact_channel, asset_class_bias, expected = map_impact(actor_group, stance)

    return {
        "actor_name": actor_name,
        "actor_id": person_id(actor_name) if actor_name else None,
        "actor_group": actor_group,
        "actor_group_id": actor_group.lower() if actor_group else None,
        "statement_type": statement_type,
        "stance": stance,
        "impact_channel": impact_channel,
        "asset_class_bias": asset_class_bias,
        "expected_direction_short_term": expected,
        "impact_potential": 0,
        "confidence": 0,
    }


def _parse_any_date(value: Any) -> datetime | None:
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except Exception:
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None


def _recency_boost(published_at: Any) -> int:
    dt = _parse_any_date(published_at)
    if not dt:
        return 0
    age = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
    hours = age.total_seconds() / 3600.0
    if hours <= 6:
        return 10
    if hours <= 24:
        return 5
    return 0


def score_person_impact(person_event: dict, item: Any) -> Tuple[int, int]:
    actor_group = person_event.get("actor_group")
    stance = person_event.get("stance", "UNKNOWN")
    text = _combined_text(item)
    text_lower = text.lower()

    group_weight = {
        "CENTRAL_BANK_HEADS": 60,
        "REGULATORS": 50,
        "ENERGY_MINISTERS": 50,
        "DEFENSE_SECURITY": 45,
        "EU_OFFICIALS": 40,
        "REGIONAL_POWER_LEADERS": 45,
    }.get(actor_group, 35)

    if stance in ("HAWKISH", "DOVISH", "RISK_ESCALATE", "RISK_DEESCALATE"):
        stance_strength = 18
    elif stance == "NEUTRAL":
        stance_strength = 8
    else:
        stance_strength = 0

    numeric_signal = 10 if NUMERIC_SIGNAL_RE.search(text_lower) else 0
    recency = _recency_boost(_get(item, "publishedAtISO"))

    ambiguity_hits = sum(1 for w in AMBIGUITY_WORDS if re.search(rf"\b{re.escape(w)}\b", text_lower))
    ambiguity_penalty = min(15, ambiguity_hits * 5)

    impact = group_weight + stance_strength + numeric_signal + recency - ambiguity_penalty
    impact = max(0, min(100, int(round(impact))))

    stance_detected, counts, conflict = detect_stance(actor_group, text)
    keyword_hits = sum(v for v in counts.values())
    quality_score = int(_get(item, "quality_score") or 0)
    quality_bonus = min(20, int(round(quality_score / 5)))

    confidence = 30
    if person_event.get("actor_name"):
        confidence += 10
    confidence += min(30, keyword_hits * 6)
    confidence += quality_bonus
    if conflict:
        confidence -= 15
    if stance_detected in ("UNKNOWN", "NEUTRAL") and keyword_hits == 0:
        confidence -= 10
    confidence = max(0, min(100, int(round(confidence))))

    return impact, confidence
