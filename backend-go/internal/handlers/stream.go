package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (a *API) StreamIntel(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusBadRequest)
		return
	}

	q := r.URL.Query()
	timeframe := q.Get("timeframe")
	if timeframe == "" {
		timeframe = "1h"
	}
	newsTimespan := q.Get("newsTimespan")
	if newsTimespan == "" {
		newsTimespan = "6h"
	}
	watch := parseWatchlist(q.Get("watch"), a.cfg.MaxWatchlist)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch, unsubscribe := a.intel.Subscribe(r.Context(), timeframe, newsTimespan, watch, 5*time.Second)
	defer unsubscribe()

	send := func(meta map[string]any) {
		ctx, cancel := context.WithTimeout(r.Context(), a.cfg.RequestTimeout)
		defer cancel()
		quotes, qerr := a.quotes.Fetch(ctx, []string{
			"BTC",
			"ETH",
			"BTC-USD",
			"NEAR-USD",
			"USDTRY=X",
			"ASTOR.IS",
			"SOKM.IS",
			"TUPRS.IS",
			"ENJSA.IS",
			"SIL",
			"AMD",
			"PLTR",
			"HL",
			"AAPL",
			"MSFT",
			"AMZN",
			"GOOGL",
			"META",
			"NVDA",
			"TSLA",
			"MSTR",
			"COIN",
			"ASML.AS",
			"SAP.DE",
			"005930.KS",
			"6758.T",
			"SHOP.TO",
			"ADYEN.AS",
			"NOKIA.HE",
			"0700.HK",
			"9988.HK",
			"XOM",
			"CVX",
			"COP",
			"OXY",
			"SLB",
			"EOG",
			"MPC",
			"PSX",
			"VLO",
			"SHEL",
			"TTE",
			"BP",
			"EQNR",
			"PBR",
			"ENB",
			"SU.TO",
			"CNQ.TO",
			"REP.MC",
			"JPM",
			"BAC",
			"WFC",
			"C",
			"GS",
			"MS",
			"BLK",
			"SCHW",
			"AXP",
			"HSBA.L",
			"UBSG.SW",
			"BNP.PA",
			"DBK.DE",
			"INGA.AS",
			"8058.T",
			"SAN.MC",
			"BARC.L",
			"ZURN.SW",
			"CAT",
			"DE",
			"BA",
			"GE",
			"HON",
			"UNP",
			"UPS",
			"LMT",
			"RTX",
			"SIE.DE",
			"AIR.PA",
			"DPW.DE",
			"VOLV-B.ST",
			"7203.T",
			"7267.T",
			"CP.TO",
			"6501.T",
			"SGRO.L",
			"LIN",
			"APD",
			"SHW",
			"ECL",
			"DD",
			"DOW",
			"NUE",
			"FCX",
			"NEM",
			"BHP.AX",
			"RIO.AX",
			"GLEN.L",
			"ANTO.L",
			"BAS.DE",
			"SIKA.SW",
			"AEM.TO",
			"NTR.TO",
			"IVN.AX",
			"NOC",
			"GD",
			"LHX",
			"HII",
			"TDG",
			"AVAV",
			"KTOS",
			"BA.L",
			"RHM.DE",
			"HO.PA",
			"LDO.MI",
			"SAAB-B.ST",
			"SAF.PA",
			"HAG.DE",
			"AM.PA",
			"ASELS.IS",
			"OTKAR.IS",
			"SDTTR.IS",
			"ALTNY.IS",
			"ONRYT.IS",
			"PAPIL.IS",
			"PATEK.IS",
			"KATMR.IS",
			"TMSN.IS",
			"CHKP",
			"CYBR",
			"NICE",
			"ESLT",
			"IAI.TA",
			"ESLT.TA",
			"NICE.TA",
			"MGDL.TA",
			"FIBI.TA",
		})
		payload := map[string]any{
			"tsISO":  meta["tsISO"],
			"market": meta["market"],
			"risk":   meta["risk"],
		}
		for k, v := range meta {
			if k == "tsISO" || k == "market" || k == "risk" {
				continue
			}
			payload[k] = v
		}
		if len(quotes) > 0 {
			payload["quotes"] = quotes
		}
		if qerr != nil {
			payload["quotes_error"] = qerr.Error()
		}
		data, _ := json.Marshal(payload)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case snap, ok := <-ch:
			if !ok {
				return
			}
			meta := map[string]any{
				"tsISO":        snap.Resp.TsISO,
				"market":       snap.Resp.Market,
				"risk":         snap.Resp.Risk,
				"intel_source": snap.Meta.Source,
				"intel_stale":  snap.Meta.Stale,
			}
			if snap.Meta.Err != "" {
				meta["intel_error"] = snap.Meta.Err
			}
			send(meta)
		}
	}
}
