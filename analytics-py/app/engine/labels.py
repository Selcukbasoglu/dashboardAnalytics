from __future__ import annotations

# Centralized labels for person-group detection to avoid circular imports.
import hashlib
import re
import unicodedata

PERSONAL_GROUPS = {
    "CENTRAL_BANK_HEADS": [
        "Jerome Powell",
        "Christine Lagarde",
        "Andrew Bailey",
        "Kazuo Ueda",
        "Pan Gongsheng",
        "Sanjay Malhotra",
        "Fatih Karahan",
        "Tiff Macklem",
        "Michele Bullock",
        "Rhee Chang-yong",
        "Gabriel Galipolo",
        "Gabriel Galípolo",
        "Victoria Rodriguez Ceja",
        "Victoria Rodríguez Ceja",
        "Ayman M. Al-Sayari",
        "Martin Schlegel",
        "Erik Thedeen",
        "Erik Thedéen",
        "Ida Wolden Bache",
        "Christian Kettel Thomsen",
        "Asgeir Jonsson",
        "Ásgeir Jónsson",
    ],
    "EU_OFFICIALS": [
        "Ursula von der Leyen",
        "Antonio Costa",
        "António Costa",
        "Kaja Kallas",
        "Roberta Metsola",
        "Maros Sefcovic",
        "Maroš Šefčovič",
    ],
    "REGIONAL_POWER_LEADERS": [
        "Donald Trump",
        "Luiz Inacio Lula da Silva",
        "Luiz Inácio Lula da Silva",
        "Mark Carney",
        "Claudia Sheinbaum",
        "Javier Milei",
        "Friedrich Merz",
        "Keir Starmer",
        "Emmanuel Macron",
        "Giorgia Meloni",
        "Vladimir Putin",
        "Xi Jinping",
        "Narendra Modi",
        "Sanae Takaichi",
        "Lee Jae Myung",
        "Prabowo Subianto",
        "Salman bin Abdulaziz Al Saud",
        "Mohammed bin Salman",
        "Recep Tayyip Erdogan",
        "Recep Tayyip Erdoğan",
        "Ali Khamenei",
        "Mohamed bin Zayed Al Nahyan",
    ],
    "REGULATORS": [
        "Gary Gensler",
        "Rostin Behnam",
        "Hester Peirce",
        "Mark Uyeda",
        "Jaime Lizarraga",
    ],
    "ENERGY_MINISTERS": [
        "Haitham Al Ghais",
        "Prince Abdulaziz bin Salman",
        "Suhail al-Mazrouei",
        "Alexander Novak",
    ],
    "DEFENSE_SECURITY": [
        "Lloyd Austin",
        "Mark Rutte",
        "Yulia Svyrydenko",
    ],
}

PERSONAL_TITLE_ALIASES = [
    "Fed Chair",
    "Federal Reserve Chair",
    "ECB President",
    "BoE Governor",
    "BoJ Governor",
    "PBoC Governor",
    "RBI Governor",
    "CBRT Governor",
    "SAMA Governor",
    "European Commission President",
    "European Council President",
    "EU High Representative",
    "President of the United States",
    "Prime Minister",
    "Chancellor of Germany",
]

PERSONAL_KEYWORDS = [
    "policy rate",
    "sanctions",
    "tariffs",
    "oil supply",
    "ceasefire",
]

# Canonical aliases for common diacritic variations
PERSON_ALIASES = {
    "Gabriel Galipolo": ["Gabriel Galípolo"],
    "Antonio Costa": ["António Costa"],
    "Maros Sefcovic": ["Maroš Šefčovič"],
    "Erik Thedeen": ["Erik Thedéen"],
    "Asgeir Jonsson": ["Ásgeir Jónsson"],
    "Victoria Rodriguez Ceja": ["Victoria Rodríguez Ceja"],
    "Recep Tayyip Erdogan": ["Recep Tayyip Erdoğan"],
}


def normalize_person_name(name: str) -> str:
    if not name:
        return ""
    cleaned = " ".join(name.strip().split())
    normalized = unicodedata.normalize("NFKD", cleaned)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return normalized


def person_id(name: str) -> str:
    base = normalize_person_name(name)
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:12]
    return digest


def canonical_person_name(name: str) -> str:
    if not name:
        return name
    normalized = normalize_person_name(name)
    for canon, aliases in PERSON_ALIASES.items():
        if normalize_person_name(canon) == normalized:
            return canon
        for alias in aliases:
            if normalize_person_name(alias) == normalized:
                return canon
    return name
