import unittest

from app.engine.sector_impact import attach_scope_and_sectors


def _item(title: str, event_type: str | None = None, entities=None):
    return {
        "title": title,
        "description": "",
        "event_type": event_type,
        "entities": entities or [],
        "tags": [],
        "publishedAtISO": "2025-01-01T00:00:00Z",
    }


class ScopeSectorTests(unittest.TestCase):
    def test_opec_output_cut(self):
        item = _item("OPEC oil output cut amid supply disruption", "ENERGY_SUPPLY_OPEC")
        attach_scope_and_sectors(item)
        self.assertEqual(item["news_scope"], "GEOPOLITICS")
        sectors = {s["sector"]: s for s in item["sector_impacts"]}
        self.assertIn("OIL_GAS_UPSTREAM", sectors)
        self.assertEqual(sectors["OIL_GAS_UPSTREAM"]["direction"], "UP")

    def test_cpi_macro(self):
        item = _item("US CPI hot as yields jump after payroll surprise", "MACRO_RATES_INFLATION")
        attach_scope_and_sectors(item)
        self.assertIn(item["news_scope"], {"MACRO", "SYSTEMIC"})
        sectors = {s["sector"]: s for s in item["sector_impacts"]}
        self.assertIn("BANKS_RATES", sectors)

    def test_export_controls_chips(self):
        item = _item("Chip export controls expand for advanced semiconductors", "SANCTIONS_GEOPOLITICS")
        attach_scope_and_sectors(item)
        self.assertIn(item["news_scope"], {"GEOPOLITICS", "SYSTEMIC"})
        sectors = {s["sector"]: s for s in item["sector_impacts"]}
        self.assertIn("SEMICONDUCTORS", sectors)
        self.assertEqual(sectors["SEMICONDUCTORS"]["direction"], "DOWN")

    def test_ceasefire_shipping(self):
        item = _item("Ceasefire reached, defense tensions ease as shipping lanes reopen", "SANCTIONS_GEOPOLITICS")
        attach_scope_and_sectors(item)
        sectors = {s["sector"]: s for s in item["sector_impacts"]}
        self.assertIn("DEFENSE_AEROSPACE", sectors)
        self.assertIn("SHIPPING_LOGISTICS", sectors)
        self.assertEqual(sectors["DEFENSE_AEROSPACE"]["direction"], "DOWN")
        self.assertEqual(sectors["SHIPPING_LOGISTICS"]["direction"], "UP")

    def test_sec_sues_crypto_exchange(self):
        item = _item("SEC sues crypto exchange for unregistered products", "REGULATION_LEGAL")
        attach_scope_and_sectors(item)
        self.assertEqual(item["news_scope"], "SECTOR")
        sectors = {s["sector"]: s for s in item["sector_impacts"]}
        self.assertIn("CRYPTO", sectors)

    def test_etf_approval_crypto(self):
        item = _item("Bitcoin ETF approval opens access for institutions", "CRYPTO_MARKET_STRUCTURE")
        attach_scope_and_sectors(item)
        sectors = {s["sector"]: s for s in item["sector_impacts"]}
        self.assertIn("CRYPTO", sectors)
        self.assertEqual(sectors["CRYPTO"]["direction"], "UP")

    def test_gas_pipeline_disruption(self):
        item = _item("Natural gas pipeline disruption hits LNG flows", "SANCTIONS_GEOPOLITICS")
        attach_scope_and_sectors(item)
        sectors = {s["sector"]: s for s in item["sector_impacts"]}
        self.assertIn("LNG_NATGAS", sectors)
        self.assertEqual(sectors["LNG_NATGAS"]["direction"], "UP")

    def test_utility_grid_outage(self):
        item = _item("Utility grid outage knocks out power across region", "SECURITY_INCIDENT")
        attach_scope_and_sectors(item)
        sectors = {s["sector"]: s for s in item["sector_impacts"]}
        self.assertIn("POWER_UTILITIES", sectors)
        self.assertIn(sectors["POWER_UTILITIES"]["direction"], {"DOWN", "MIXED"})

    def test_single_company_earnings(self):
        item = _item("Company earnings miss sends shares lower", "EARNINGS_GUIDANCE", entities=["Example Co"])
        attach_scope_and_sectors(item)
        self.assertEqual(item["news_scope"], "COMPANY")
        self.assertLessEqual(item["max_sector_impact"], 30)

    def test_global_risk_off_systemic(self):
        item = _item("Global risk-off wave as volatility spikes across markets", None)
        attach_scope_and_sectors(item)
        self.assertEqual(item["news_scope"], "SYSTEMIC")
        self.assertGreaterEqual(item["scope_score"], 50)


if __name__ == "__main__":
    unittest.main()
