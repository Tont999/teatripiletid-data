# teatripiletid-data

Automaatne kraaper Eesti Draamateatri ja Tallinna Linnateatri kava ja piletiseisu jaoks.

GitHub Actions käivitab igapäevaselt (07:00 Eesti aja järgi, 04:00 UTC) skripti, mis kraabib mõlema teatri kodulehe, eraldab etenduste nimekirja koos kuupäevade ja piletilinkidega ning salvestab tulemuse struktureeritud JSON-ina `data/` kausta. Commit lükkub automaatselt samasse repossse.

Seda repot tarbib (ainult loeb) Anthropic'u Cowork agent, mis koostab kasutajale igapäevase raporti ja uuendab Cowork püsiva lehe.

## Repo struktuur

```
.github/workflows/scrape.yml   # igapäevane GitHub Actions töö
scraper/
  scrape.py                    # peaskript
  requirements.txt             # Python sõltuvused
data/
  state.json                   # viimane strukturaalne väljavõte (tarbija loeb seda)
  raw/                         # töötlemata HTML (debug)
  log.txt                      # kraapimise logi
```

## Käsitsi käivitamine

GitHub repo all **Actions** → **Scrape teatripiletid** → **Run workflow**.

## state.json skeem

```json
{
  "generated_at": "2026-04-22T04:03:11Z",
  "scraper_version": "1.0.0",
  "theaters": [
    {
      "id": "draamateater",
      "name": "Eesti Draamateater",
      "source_url": "https://www.draamateater.ee/kava",
      "scraped_ok": true,
      "error": null,
      "shows": [
        {
          "id": "draamateater:hamlet:2026-05-10T19:00",
          "title": "Hamlet",
          "datetime_str": "10.05.2026 kell 19:00",
          "iso_datetime": "2026-05-10T19:00:00+03:00",
          "venue": "Suur saal",
          "ticket_url": "https://www.piletilevi.ee/...",
          "ticket_status": "available",
          "price_range": "12-30 €"
        }
      ]
    },
    {
      "id": "linnateater",
      "name": "Tallinna Linnateater",
      "source_url": "https://linnateater.ee/kava",
      "scraped_ok": true,
      "error": null,
      "shows": [ ]
    }
  ]
}
```

## Litsents

MIT. Teatrite andmed kuuluvad teatritele. Seda repot kasutada ainult isiklikuks teabeks.
