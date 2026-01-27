SECTOR_ROTATION_MAX = 5
SECTOR_CORE_ALWAYS = ["SEMICONDUCTORS", "OIL_GAS_UPSTREAM"]
SECTOR_DYNAMIC_COUNT = 3
PREFERRED_SOURCELANG = ["english", "turkish"]

BACKSTOP_MAXRECORDS = 80
BACKSTOP_DOMAIN_TIER = ["T1", "T2"]
BACKSTOP_GIANTS_PER_SECTOR = 8
BACKSTOP_MIN_HITS = 3

SECTOR_GIANTS_REGISTRY = {
    "SEMICONDUCTORS": [
        "TSMC",
        "Samsung Electronics",
        "SK hynix",
        "ASML",
        "NVIDIA",
        "Broadcom",
        "AMD",
        "Micron",
    ],
    "OIL_GAS_UPSTREAM": [
        "Saudi Aramco",
        "Exxon Mobil",
        "Chevron",
        "Shell",
        "TotalEnergies",
        "PetroChina",
        "Sinopec",
        "CNOOC",
    ],
    "LNG_NATGAS": [
        "Cheniere",
        "Shell",
        "TotalEnergies",
        "QatarEnergy",
        "PetroChina",
        "CNOOC",
        "Sinopec",
        "Enbridge",
    ],
    "POWER_UTILITIES": [
        "NextEra Energy",
        "Duke Energy",
        "Southern Company",
        "Iberdrola",
        "Enel",
        "RWE",
        "National Grid",
        "EDF",
    ],
    "BANKS_RATES": [
        "JPMorgan",
        "Bank of America",
        "HSBC",
        "BNP Paribas",
        "Santander",
        "ICBC",
        "China Construction Bank",
        "Mitsubishi UFJ",
    ],
    "SHIPPING_LOGISTICS": [
        "Maersk",
        "COSCO",
        "Hapag-Lloyd",
        "CMA CGM",
        "MSC",
        "DP World",
        "UPS",
        "FedEx",
    ],
    "DEFENSE_AEROSPACE": [
        "Lockheed Martin",
        "RTX",
        "Northrop Grumman",
        "BAE Systems",
        "Airbus",
        "Rheinmetall",
        "Thales",
        "Leonardo",
    ],
}

SECTOR_TRIGGER_KEYWORDS = [
    "earnings",
    "guidance",
    "outlook",
    "capex",
    "investment",
    "deal",
    "acquisition",
    "regulation",
    "sanction",
    "tariff",
    "production",
    "supply",
    "demand",
    "pricing",
    "forecast",
    "results",
]

SECTOR_RULES = {
    "SEMICONDUCTORS": {
        "required": ["semiconductor", "semiconductors", "chip", "chipmaker", "gpu", "foundry", "fab", "wafer"],
        "boost": ["ai", "datacenter", "lithography", "memory", "hbm"],
        "exclude": [],
    },
    "OIL_GAS_UPSTREAM": {
        "required": ["oil", "crude", "gas", "upstream", "exploration", "production"],
        "boost": ["opec", "drilling", "rig", "brent", "wti"],
        "exclude": ["renewable", "solar", "wind"],
    },
    "LNG_NATGAS": {
        "required": ["lng", "natural gas", "natgas"],
        "boost": ["gas export", "pipeline", "liquefaction", "regasification"],
        "exclude": [],
    },
    "POWER_UTILITIES": {
        "required": ["utility", "utilities", "power grid", "electricity", "power generation"],
        "boost": ["renewable", "wind", "solar", "nuclear", "grid"],
        "exclude": [],
    },
    "BANKS_RATES": {
        "required": ["bank", "banks", "lender", "lending", "loan", "deposit"],
        "boost": ["rates", "interest rate", "credit", "mortgage"],
        "exclude": [],
    },
    "SHIPPING_LOGISTICS": {
        "required": ["shipping", "logistics", "freight", "container", "port", "cargo"],
        "boost": ["supply chain", "vessel", "fleet", "air freight"],
        "exclude": [],
    },
    "DEFENSE_AEROSPACE": {
        "required": ["defense", "aerospace", "missile", "aircraft", "fighter", "drone"],
        "boost": ["military", "contract", "pentagon", "arms"],
        "exclude": [],
    },
}

SECTOR_ORDER = list(SECTOR_GIANTS_REGISTRY.keys())
