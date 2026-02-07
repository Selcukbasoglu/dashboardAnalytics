package services

import (
	"testing"
	"time"

	"macroquant-intel/backend-go/internal/config"
	"macroquant-intel/backend-go/internal/models"
)

func TestHasCacheableIntel(t *testing.T) {
	if hasCacheableIntel(models.IntelResponse{}) {
		t.Fatal("expected empty response to be non-cacheable")
	}
	if !hasCacheableIntel(models.IntelResponse{TsISO: "2026-02-07T17:18:00Z"}) {
		t.Fatal("expected response with ts to be cacheable")
	}
}

func TestIntelRequestTimeoutPrefersIntelTimeout(t *testing.T) {
	svc := &IntelService{cfg: config.Config{RequestTimeout: 12 * time.Second, IntelTimeout: 25 * time.Second}}
	if got := svc.intelRequestTimeout(); got != 25*time.Second {
		t.Fatalf("expected intel timeout 25s, got %v", got)
	}

	svc = &IntelService{cfg: config.Config{RequestTimeout: 12 * time.Second, IntelTimeout: 0}}
	if got := svc.intelRequestTimeout(); got != 12*time.Second {
		t.Fatalf("expected fallback request timeout 12s, got %v", got)
	}
}
