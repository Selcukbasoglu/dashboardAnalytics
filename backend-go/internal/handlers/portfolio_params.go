package handlers

func normalizeBase(v string) string {
	if v == "TRY" || v == "USD" {
		return v
	}
	return ""
}

func normalizeWindow(v string) string {
	switch v {
	case "24h", "7d", "30d":
		return v
	default:
		return ""
	}
}

func normalizePeriod(v string) string {
	switch v {
	case "daily", "weekly", "monthly":
		return v
	default:
		return ""
	}
}
