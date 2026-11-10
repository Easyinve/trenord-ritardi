# Ritardi e cancellazioni dei treni Trenord

Raccoglie ogni mezz'ora lo stato dei treni Trenord e lo salva giorno per giorno.

- `archivio/AAAA-MM-GG.csv`: una riga per treno, con ritardo in minuti, fermate cancellate e avvisi.
- `archivio/riepilogo.csv`: una riga per giorno, con treni monitorati, cancellati, in ritardo oltre 5 minuti, ritardo medio e massimo.

## Da dove vengono i dati

- L'elenco dei treni del giorno viene dal [feed GTFS di Trenord](https://www.dati.lombardia.it/d/3z4k-mxz9) sul portale open data di Regione Lombardia.
- Lo stato di ogni treno viene dall'API pubblica usata dalla pagina "Circolazione in tempo reale" di trenord.it.

La raccolta gira su GitHub Actions (`.github/workflows/raccolta.yml`). Lo script fa circa due richieste al secondo e si ferma se il sito risponde 403: in quel caso l'esecuzione risulta fallita.

## Leggere i dati in locale

```bash
git pull
python3 trenord_scraper.py report --archivio archivio --date 2026-11-10
python3 trenord_scraper.py report --archivio archivio --days 7
```

## Limiti

- L'API conserva i dati in tempo reale per circa tre ore dopo l'arrivo: se la raccolta salta per più di tre ore, i treni arrivati nel frattempo restano senza ritardo.
- Un treno conta come cancellato per intero se tutte le fermate risultano soppresse, in parte se lo sono solo alcune.
- Il ritardo è quello all'arrivo a destinazione; per i treni non ancora arrivati è l'ultimo rilevato.
