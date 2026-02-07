import unittest
from unittest.mock import patch

from app.engine.news_engine import collect_local_news
from app.providers.tr_local_scrape import filter_tr_local_scraped_items, search_tr_local_scrape


SAMPLE_HTML = """
<html>
  <head>
    <script type=\"application/ld+json\">
      {
        \"@context\": \"https://schema.org\",
        \"@type\": \"NewsArticle\",
        \"headline\": \"Borsa Istanbul'da banka hisseleri one cikti\",
        \"url\": \"https://www.paraanaliz.com/2026/borsa/borsa-istanbul-banka-hisseleri-g-100001/\",
        \"datePublished\": \"2026-02-07T10:10:00Z\",
        \"description\": \"BIST 100 gunu yukselisle tamamladi.\"
      }
    </script>
  </head>
  <body>
    <a href=\"https://www.dunya.com/finans/borsa/new-york-borsasi-yukselisle-kapandi-haberi-814225\" title=\"New York borsasi yukselisle kapandi\">x</a>
    <a href=\"https://www.ekonomim.com/finans/haberler/borsa/astor-enerji-icin-pozitif-haber-haberi-743818\" title=\"ASTOR Enerji icin pozitif haber\">x</a>
    <a href=\"https://www.ekonomim.com/finans/borsa/hisseler/astor-astor-enerji-as\" title=\"ASTOR\">ASTOR</a>
    <a href=\"/finans/borsa\">Borsa</a>
  </body>
</html>
"""


class TrLocalScrapeTests(unittest.TestCase):
    @patch("app.providers.tr_local_scrape.get_text", return_value=SAMPLE_HTML)
    def test_search_extracts_article_links_and_skips_stock_pages(self, _mock_get_text):
        items = search_tr_local_scrape("BIST", max_items=20, timeout=0.1)
        urls = {it.get("url") for it in items}
        self.assertIn(
            "https://www.paraanaliz.com/2026/borsa/borsa-istanbul-banka-hisseleri-g-100001/",
            urls,
        )
        self.assertIn(
            "https://www.ekonomim.com/finans/haberler/borsa/astor-enerji-icin-pozitif-haber-haberi-743818",
            urls,
        )
        self.assertNotIn(
            "https://www.ekonomim.com/finans/borsa/hisseler/astor-astor-enerji-as",
            urls,
        )

    def test_filter_supports_strict_ticker_query(self):
        raw = [
            {
                "title": "ASTOR Enerji yeni yatirim planini acikladi",
                "url": "https://www.paraanaliz.com/2026/borsa/astor-enerji-yatirim-g-100002/",
                "published": "2026-02-07T09:00:00Z",
                "summary": "",
            },
            {
                "title": "Borsa Istanbul gunu yatay tamamladi",
                "url": "https://www.dunya.com/finans/borsa/borsa-gunu-dususle-tamamladi-haberi-814196",
                "published": "2026-02-07T09:00:00Z",
                "summary": "",
            },
        ]
        out = filter_tr_local_scraped_items(raw, "ASTOR BIST", max_items=10, strict=True, timespan="24h")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["url"], "https://www.paraanaliz.com/2026/borsa/astor-enerji-yatirim-g-100002/")

    @patch("app.engine.news_engine._load_bist_alias_terms", return_value=[])
    @patch("app.engine.news_engine.search_rss_local", return_value=[])
    @patch(
        "app.engine.news_engine.search_tr_local_scrape",
        return_value=[
            {
                "title": "Borsa Istanbul'da banka hisseleri one cikti",
                "url": "https://www.paraanaliz.com/2026/borsa/borsa-istanbul-banka-hisseleri-g-100001/",
                "summary": "",
                "published": "2026-02-07T10:10:00Z",
            },
            {
                "title": "ASTOR Enerji icin pozitif haber",
                "url": "https://www.ekonomim.com/finans/haberler/borsa/astor-enerji-icin-pozitif-haber-haberi-743818",
                "summary": "",
                "published": "2026-02-07T10:00:00Z",
            },
        ],
    )
    @patch(
        "app.engine.news_engine.filter_tr_local_scraped_items",
        return_value=[
            {
                "title": "ASTOR Enerji icin pozitif haber",
                "url": "https://www.ekonomim.com/finans/haberler/borsa/astor-enerji-icin-pozitif-haber-haberi-743818",
                "summary": "",
                "published": "2026-02-07T10:00:00Z",
            }
        ],
    )
    def test_collect_local_news_uses_scrape_fallback(
        self,
        _mock_filter,
        _mock_scrape,
        _mock_rss,
        _mock_aliases,
    ):
        items, notes, counts = collect_local_news(["ASTOR"], "24h", maxrecords=20, timeout=0.2)
        self.assertTrue(items)
        self.assertIn("scrape_local_base", notes)
        self.assertIn("scrape_local_term", notes)
        self.assertGreater(counts.get("tr", 0), 0)
        self.assertGreater(counts.get("tr_scrape", 0), 0)


if __name__ == "__main__":
    unittest.main()
