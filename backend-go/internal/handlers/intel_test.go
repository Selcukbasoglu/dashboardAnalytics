package handlers

import "testing"

func TestAppendUniqueString_DedupesCaseInsensitive(t *testing.T) {
	items := []string{"analytics"}
	items = appendUniqueString(items, "Analytics")
	if len(items) != 1 {
		t.Fatalf("expected deduped length 1, got %d", len(items))
	}

	items = appendUniqueString(items, " python_unreachable ")
	if len(items) != 2 {
		t.Fatalf("expected length 2 after append, got %d", len(items))
	}
	if items[1] != "python_unreachable" {
		t.Fatalf("expected trimmed value, got %q", items[1])
	}
}

func TestEmptyIntelFallbackRemainsSingleAnalytics(t *testing.T) {
	resp := emptyIntel("1h", "6h")
	resp.Debug.DataMissing = appendUniqueString(resp.Debug.DataMissing, "analytics")
	count := 0
	for _, v := range resp.Debug.DataMissing {
		if v == "analytics" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected analytics to appear once, got %d", count)
	}
}
