# Gemini Summary Eval Set (v1)

Bu klasor, `generate_portfolio_summary` kalitesini sistematik olcmek icin hazirlandi.

## 1) Neyi olcuyoruz?

Bu eval set, tek bir "dogru metin" beklemez. Onun yerine kalite sinyallerini puanlar:

- `header_coverage`: zorunlu 6 baslik var mi?
- `evidence_density`: `[KANIT:T1/L1]` gibi kanit referanslari yeterli mi?
- `portfolio_grounding`: ozet portfoy sembollerine degiyor mu?
- `assumption_hygiene`: model fikirleri "varsayim" diye etiketlenmis mi?
- `actionability`: izleme/yonetim dili var mi?
- `non_template`: anlamsiz sablon cikti var mi?
- `length_quality`: cok kisa/cok uzun mu?

Bu metrikler `profile` ile calisir:

- `soft`: bug yakalamaya odakli, daha toleransli esikler (varsayilan)
- `strict`: ayni case'lerin daha sikilastirilmis esikleri

## 2) Eval set yapisi

Dosya: `dataset_v1.json`

Her case su yapida:

- `id`: benzersiz test kimligi
- `tags`: filtreleme/analiz etiketleri
- `input_payload`: gemini summary fonksiyonuna giden veri
- `expect`:
  - `min_scores`: metrik esikleri
  - `must_contain`: zorunlu metinler
  - `must_regex`: zorunlu regex desenleri
  - `forbid`: gecmemesi gereken metinler
- `expect_profiles`:
  - `soft`: bug-finding odakli esikler
  - `strict`: kaliteyi yukari tasimak icin sikilastirilmis esikler

## 3) Nasil calistirilir?

`analytics-py` klasorunden:

```bash
PYTHONPATH=. ./.venv/bin/python -m app.evals.gemini_summary.runner --mode rule_based
```

Hizli smoke run:

```bash
PYTHONPATH=. ./.venv/bin/python -m app.evals.gemini_summary.runner --mode rule_based --limit 3
```

Strict profil (2. faz):

```bash
PYTHONPATH=. ./.venv/bin/python -m app.evals.gemini_summary.runner --mode rule_based --profile strict
```

Canli model (GEMINI_API_KEY gerekli):

```bash
PYTHONPATH=. ./.venv/bin/python -m app.evals.gemini_summary.runner --mode live --timeout 10
```

## 4) LangSmith ile nasil kullanilir?

Bu runner, LangSmith'e manuel import icin JSONL uretir:

```bash
PYTHONPATH=. ./.venv/bin/python -m app.evals.gemini_summary.runner \
  --mode live \
  --export-langsmith-jsonl analytics-py/eval_reports/langsmith_gemini_eval_v1.jsonl \
  --langsmith-dataset gemini-summary-evals-v1
```

Sonra bu JSONL dosyasini LangSmith UI'da dataset/run import akisi ile yukleyebilirsin.

## 5) Case ekleme rehberi (pratik)

1. Yeni bir vaka sec: gercek bug veya edge-case.
2. `dataset_v1.json` icine yeni case ekle.
3. Baslangicta `expect_profiles.soft` esiklerini kullan (`overall >= 0.60` gibi).
4. Runner'i calistir, skorlari gor.
5. 1-2 hafta sonra `--profile strict` ile kalite esigini sikilastir.

## 6) Teknik not

Bu set "deterministic + ucuz" bir kalite kapisidir.

- Unit test: fonksiyonel kontrat
- Eval set: metin kalitesi
- Canli eval + LangSmith: gercek model davranisi

Ucunu birlikte kullanmak en saglam yol.
