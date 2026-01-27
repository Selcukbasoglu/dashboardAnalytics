import unittest
from datetime import datetime, timezone

import pandas as pd

from app.engine.event_study import compute_event_study
from app.models import NewsItem


class EventStudyV2Tests(unittest.TestCase):
    def _build_candles(self, start: datetime, periods: int):
        idx = pd.date_range(start=start, periods=periods, freq="15min", tz="UTC")
        close = pd.Series([100 + i for i in range(periods)], index=idx)
        volume = pd.Series([1000 + (i % 10) for i in range(periods)], index=idx)
        df = pd.DataFrame({"Close": close, "Volume": volume})
        return df

    def test_event_windows_and_tsi(self):
        start = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        df = self._build_candles(start, 200)
        event_time = df.index[10].to_pydatetime()
        news = [
            NewsItem(
                title="Test headline",
                url="http://example.com",
                publishedAtISO=event_time.isoformat().replace("+00:00", "Z"),
                score=10,
            )
        ]
        points = compute_event_study(news, {"BTC": df, "NQ": df}, timeframe="15m")
        self.assertEqual(len(points), 1)
        point = points[0]
        self.assertIsNotNone(point.published_at_tsi)
        self.assertIn("03:00", point.published_at_tsi)

        btc = point.reactions.get("BTC")
        self.assertIsNotNone(btc)
        ret_15m = btc.post.get("15m").ret
        expected_15m = (111 - 110) / 110 * 100.0
        self.assertAlmostEqual(ret_15m, expected_15m, places=4)

        pre_ret = btc.pre.ret
        expected_pre = (109 - 106) / 106 * 100.0
        self.assertAlmostEqual(pre_ret, expected_pre, places=4)

        around = btc.around.get("30m")
        expected_around = (112 - 108) / 108 * 100.0
        self.assertAlmostEqual(around.ret, expected_around, places=4)
        self.assertIsNotNone(around.z)
        self.assertAlmostEqual(btc.pre_30m_ret, (110 - 108) / 108 * 100.0, places=4)
        self.assertAlmostEqual(btc.post_30m_ret, (112 - 110) / 110 * 100.0, places=4)


class EventStudyMissingTests(unittest.TestCase):
    def test_missing_around_window(self):
        start = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        idx = pd.date_range(start=start, periods=10, freq="15min", tz="UTC")
        close = pd.Series([100 + i for i in range(10)], index=idx)
        volume = pd.Series([1000 + (i % 5) for i in range(10)], index=idx)
        df = pd.DataFrame({"Close": close, "Volume": volume})
        event_time = idx[8].to_pydatetime()
        news = [
            NewsItem(
                title="Missing around",
                url="http://example.com",
                publishedAtISO=event_time.isoformat().replace("+00:00", "Z"),
                score=10,
            )
        ]
        points = compute_event_study(news, {"BTC": df}, timeframe="15m")
        self.assertEqual(len(points), 1)
        btc = points[0].reactions.get("BTC")
        self.assertIsNotNone(btc)
        around = btc.around.get("30m")
        self.assertIsNone(around.ret)
        self.assertIn("reactions.BTC.around.30m", btc.missing_fields)


class EventStudyAlignmentTests(unittest.TestCase):
    def test_alignment_floor_15m(self):
        start = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        idx = pd.date_range(start=start, periods=20, freq="15min", tz="UTC")
        close = pd.Series([100 + i for i in range(20)], index=idx)
        volume = pd.Series([1000 + i for i in range(20)], index=idx)
        df = pd.DataFrame({"Close": close, "Volume": volume})
        # t0 at 12:07 should align to 12:00 (index 0)
        event_time = datetime(2025, 1, 1, 12, 7, tzinfo=timezone.utc)
        news = [
            NewsItem(
                title="Alignment test",
                url="http://example.com",
                publishedAtISO=event_time.isoformat().replace("+00:00", "Z"),
                score=10,
            )
        ]
        points = compute_event_study(news, {"BTC": df}, timeframe="15m")
        self.assertEqual(len(points), 1)
        btc = points[0].reactions.get("BTC")
        # pre window uses i-4..i-1 which should be missing (since i=0), so pre_ret should be 0.0
        self.assertEqual(btc.pre.ret, 0.0)


if __name__ == "__main__":
    unittest.main()
