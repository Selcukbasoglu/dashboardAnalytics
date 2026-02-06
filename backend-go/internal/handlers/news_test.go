package handlers

import (
	"testing"

	"macroquant-intel/backend-go/internal/models"
)

func TestApplyNewsFilter(t *testing.T) {
	items := []models.NewsItem{
		{Title: "Fed signals caution", Tags: []string{"Macro", "Rates"}},
		{Title: "Chipmaker earnings beat", Tags: []string{"Earnings", "Tech"}},
	}
	got := applyNewsFilter(items, "rates")
	if len(got) != 1 {
		t.Fatalf("expected 1 item, got %d", len(got))
	}
	if got[0].Title != "Fed signals caution" {
		t.Fatalf("unexpected item: %s", got[0].Title)
	}
}

func TestApplyNewsSearch(t *testing.T) {
	items := []models.NewsItem{
		{Title: "Fed sees inflation easing", ShortSummary: "CPI slowed in latest print", Tags: []string{"Macro"}},
		{Title: "Oil output cut", ShortSummary: "OPEC announces supply action", Tags: []string{"Energy"}},
	}

	got := applyNewsSearch(items, "fed cpi")
	if len(got) != 1 {
		t.Fatalf("expected 1 item for token match, got %d", len(got))
	}
	if got[0].Title != "Fed sees inflation easing" {
		t.Fatalf("unexpected matched title: %s", got[0].Title)
	}

	got = applyNewsSearch(items, "supply action")
	if len(got) != 1 {
		t.Fatalf("expected 1 item for phrase match, got %d", len(got))
	}
	if got[0].Title != "Oil output cut" {
		t.Fatalf("unexpected matched title: %s", got[0].Title)
	}
}
