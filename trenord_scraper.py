#!/usr/bin/env python3
"""Raccoglie ogni giorno cancellazioni e ritardi dei treni Trenord.

Come funziona
-------------
1. L'elenco dei treni previsti per la giornata viene dal feed GTFS ufficiale
   di Trenord, pubblicato sul portale open data di Regione Lombardia.
2. Per ogni treno si interroga l'API pubblica usata dal sito trenord.it
   (la stessa della pagina "Circolazione in tempo reale"), che restituisce
   stato, ritardo e fermate soppresse.
3. I risultati finiscono in un database SQLite, una riga per treno e per giorno.

Due vincoli decidono quando interrogare un treno:

- l'API conserva i dati in tempo reale solo per circa tre ore dopo l'arrivo;
- il sito tiene in cache ogni risposta per due ore, quindi chiedere lo stesso
  treno più spesso restituisce sempre la stessa fotografia.

Per questo ogni treno viene interrogato una volta sola, una ventina di minuti
dopo l'arrivo previsto. Se a quel punto non risulta ancora arrivato lo si
richiede una seconda volta due ore dopo, quando la cache è scaduta. La raccolta
va lanciata ogni 30 minuti (`watch`, cron o GitHub Actions) e a ogni passaggio
fa poche decine di richieste.

Il sito blocca chi fa troppe richieste ravvicinate (risposta 403): lo script
va piano di proposito, circa due richieste al secondo, e si ferma subito se
viene bloccato. Non alzare la velocità.

Uso
---
    python3 trenord_scraper.py collect            # un passaggio di raccolta
    python3 trenord_scraper.py watch --every 30   # raccolta continua
    python3 trenord_scraper.py report             # riepilogo di oggi
    python3 trenord_scraper.py report --date 2026-11-10 --csv treni.csv
    python3 trenord_scraper.py report --days 7    # una riga per giorno
    python3 trenord_scraper.py mensile --mese 2026-11   # indennizzo per direttrice

Con `--archivio CARTELLA` i dati vengono letti e riscritti anche come CSV, un
file per giorno più `riepilogo.csv`: è il formato usato su GitHub Actions, dove
il database non sopravvive tra un'esecuzione e l'altra.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sqlite3
import statistics
import sys
import time
import zipfile
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

API_TRENO = "https://admin.trenord.it/store-management-api/mia/train/{numero}"
GTFS_URL = "https://www.dati.lombardia.it/download/3z4k-mxz9/application%2Fzip"
USER_AGENT = "trenord-stats/1.0 (script di monitoraggio puntualita)"
ROMA = ZoneInfo("Europe/Rome")

CARTELLA = Path(__file__).resolve().parent / "dati"
DB_PATH = CARTELLA / "trenord.sqlite"
GTFS_PATH = CARTELLA / "gtfs_trenord.zip"

PAUSA = 0.5           # secondi tra una richiesta e l'altra
ATTESA_ARRIVO_MIN = 20  # si interroga un treno 20 minuti dopo l'arrivo previsto
SCADENZA_ORE = 3      # dopo l'arrivo previsto + 3 ore l'API non ha più i dati
CACHE_MIN = 120       # il sito tiene in cache ogni risposta per due ore
ATTESA_BLOCCO_MIN = 90  # in `watch`, pausa dopo un blocco del sito


class Bloccato(Exception):
    """Il sito ha rifiutato la richiesta (403 o 429)."""

STATI = {"N": "non partito", "V": "in viaggio", "A": "arrivato"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS treni (
    data            TEXT NOT NULL,     -- giorno di circolazione, AAAA-MM-GG
    codice          TEXT NOT NULL,     -- codice trasporto, univoco nel giorno
    numero          TEXT NOT NULL,     -- numero del treno
    linea           TEXT,
    categoria       TEXT,
    direttrice      TEXT,
    origine         TEXT,
    destinazione    TEXT,
    partenza_prev   TEXT,              -- HH:MM:SS
    arrivo_prev     TEXT,
    stato           TEXT,              -- N, V, A
    cancellato      INTEGER DEFAULT 0, -- 1 se soppresso per intero
    fermate         INTEGER,
    fermate_cancellate INTEGER DEFAULT 0,
    ritardo         INTEGER,           -- minuti; all'arrivo se arrivato, altrimenti l'ultimo rilevato
    ritardo_max     INTEGER,           -- ritardo massimo rilevato lungo il percorso
    arrivato        INTEGER DEFAULT 0, -- 1 se c'è l'orario effettivo di arrivo a destinazione
    ha_dati         INTEGER DEFAULT 0, -- 1 se l'API ha fornito dati in tempo reale
    avvisi          TEXT,
    grezzo          TEXT,              -- risposta JSON, salvata solo per i casi anomali
    primo_rilevamento  TEXT,
    ultimo_rilevamento TEXT,           -- momento a cui risale l'ultima risposta ricevuta
    PRIMARY KEY (data, codice)
);
"""


def adesso() -> datetime:
    return datetime.now(ROMA)


def apri_db() -> sqlite3.Connection:
    CARTELLA.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    return db


# --------------------------------------------------------------------------
# Elenco dei treni del giorno (GTFS)
# --------------------------------------------------------------------------

def scarica_gtfs(forza: bool = False) -> None:
    """Scarica il GTFS se manca o se ha più di un giorno."""
    CARTELLA.mkdir(parents=True, exist_ok=True)
    recente = GTFS_PATH.exists() and time.time() - GTFS_PATH.stat().st_mtime < 86400
    if recente and not forza:
        return
    try:
        r = requests.get(GTFS_URL, headers={"User-Agent": USER_AGENT}, timeout=120)
        r.raise_for_status()
        zipfile.ZipFile(io.BytesIO(r.content)).testzip()
        GTFS_PATH.write_bytes(r.content)
    except Exception as e:  # se il portale non risponde si usa la copia vecchia
        if not GTFS_PATH.exists():
            raise SystemExit(f"Impossibile scaricare il GTFS di Trenord: {e}")
        print(f"Attenzione: GTFS non aggiornato ({e}), uso la copia salvata.", file=sys.stderr)


def _minuti(hhmmss: str) -> int:
    """Minuti dalla mezzanotte; nel GTFS le ore possono superare le 24."""
    h, m, _ = hhmmss.split(":")
    return int(h) * 60 + int(m)


def treni_del_giorno(giorno: date) -> dict[str, list[list[int]]]:
    """Treni previsti in `giorno`: {numero: [[partenza, arrivo], ...]} in minuti.

    Lo stesso numero può comparire più volte nella giornata, per questo ogni
    numero ha un elenco di corse. Il risultato viene salvato su file perché la
    lettura del GTFS richiede qualche secondo.
    """
    cache = CARTELLA / f"treni_{giorno.isoformat()}.json"
    if cache.exists():
        return json.loads(cache.read_text())

    scarica_gtfs()
    aaaammgg = giorno.strftime("%Y%m%d")
    with zipfile.ZipFile(GTFS_PATH) as z:
        with z.open("calendar_dates.txt") as f:
            servizi = {
                r["service_id"]
                for r in csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
                if r["date"] == aaaammgg and r["exception_type"] == "1"
            }
        corse: dict[str, str] = {}  # trip_id -> numero treno
        with z.open("trips.txt") as f:
            for r in csv.DictReader(io.TextIOWrapper(f, "utf-8-sig")):
                if r["service_id"] not in servizi or r["route_id"] == "TN_Bus":
                    continue
                # trip_short_name è "2620" oppure "RE_11 - 2153"
                numero = r["trip_short_name"].split(" - ")[-1].strip()
                if numero.isdigit():
                    corse[r["trip_id"]] = numero
        estremi: dict[str, list[int]] = {}
        with z.open("stop_times.txt") as f:
            for r in csv.DictReader(io.TextIOWrapper(f, "utf-8-sig")):
                if r["trip_id"] not in corse:
                    continue
                p, a = _minuti(r["departure_time"]), _minuti(r["arrival_time"])
                e = estremi.setdefault(r["trip_id"], [p, a])
                e[0], e[1] = min(e[0], p), max(e[1], a)

    treni: dict[str, list[list[int]]] = {}
    for trip_id, e in estremi.items():
        treni.setdefault(corse[trip_id], []).append(e)
    if treni:
        cache.write_text(json.dumps(treni))
        for vecchio in CARTELLA.glob("treni_*.json"):  # si tengono solo gli ultimi giorni
            if vecchio.name < f"treni_{(giorno - timedelta(days=3)).isoformat()}.json":
                vecchio.unlink()
    return treni


# --------------------------------------------------------------------------
# Interrogazione dell'API e lettura della risposta
# --------------------------------------------------------------------------

_sessione = requests.Session()
_sessione.headers["User-Agent"] = USER_AGENT


def _momento_risposta(r: requests.Response) -> datetime:
    """A quando risale davvero la risposta, che può arrivare dalla cache del sito.

    L'intestazione Expires indica la scadenza della copia in cache: togliendo
    la durata della cache si ottiene il momento in cui è stata generata.
    """
    try:
        generata = parsedate_to_datetime(r.headers["Expires"]) - timedelta(minutes=CACHE_MIN)
        if timedelta(0) <= adesso() - generata <= timedelta(minutes=CACHE_MIN + 5):
            return generata.astimezone(ROMA)
    except (KeyError, TypeError, ValueError):
        pass
    return adesso()


def scarica_treno(numero: str) -> tuple[list, datetime] | None:
    """Risposta dell'API per un numero di treno e momento a cui risale.

    Restituisce None se la richiesta fallisce.
    """
    for tentativo in range(3):
        try:
            r = _sessione.get(API_TRENO.format(numero=numero), timeout=20)
        except requests.RequestException:
            time.sleep(5 * (tentativo + 1))
            continue
        if r.status_code in (403, 429):
            raise Bloccato(f"risposta {r.status_code}")
        time.sleep(PAUSA)
        if r.status_code == 404:
            return [], adesso()
        if r.status_code == 200:
            try:
                return r.json(), _momento_risposta(r)
            except ValueError:
                return None
        time.sleep(5 * (tentativo + 1))
    return None


def leggi_corsa(corsa: dict) -> dict | None:
    """Trasforma una corsa della risposta API in una riga per il database."""
    tratte = corsa.get("journey_list") or []
    if not tratte:
        return None
    treno = tratte[0].get("train", {})
    fermate = [p for t in tratte for p in t.get("pass_list", [])]
    if not fermate:
        return None

    ritardi = []
    for p in fermate:
        reale = p.get("actual_data") or {}
        for chiave in ("arr_delay_actual", "dep_delay_actual"):
            if isinstance(reale.get(chiave), (int, float)):
                ritardi.append(int(reale[chiave]))

    arrivo = (fermate[-1].get("actual_data") or {}).get("arr_delay_actual")
    arrivato = isinstance(arrivo, (int, float))
    ritardo = int(arrivo) if arrivato else treno.get("delay")
    if ritardo is None and ritardi:
        ritardo = ritardi[-1]

    n_cancellate = sum(1 for p in fermate if p.get("cancelled"))
    cancellato = bool(corsa.get("cancelled")) or n_cancellate == len(fermate)
    stato = treno.get("status")
    anomalo = cancellato or n_cancellate or stato not in STATI
    avvisi = " | ".join(
        dict.fromkeys(a.get("message", "").strip() for a in treno.get("alerts") or [] if a.get("message"))
    )

    d = corsa.get("date") or treno.get("date") or ""
    return {
        "data": f"{d[:4]}-{d[4:6]}-{d[6:8]}",
        "codice": str(treno.get("CodiceTrasporto1") or treno.get("train_id")),
        "numero": str(treno.get("train_id") or treno.get("train_name")),
        "linea": treno.get("line"),
        "categoria": treno.get("train_category"),
        "direttrice": treno.get("direttrice"),
        "origine": (corsa.get("dep_station") or {}).get("station_ori_name"),
        "destinazione": (corsa.get("arr_station") or {}).get("station_ori_name"),
        "partenza_prev": corsa.get("dep_time"),
        "arrivo_prev": corsa.get("arr_time"),
        "stato": stato,
        "cancellato": int(cancellato),
        "fermate": len(fermate),
        "fermate_cancellate": n_cancellate,
        "ritardo": ritardo,
        "ritardo_max": max(ritardi) if ritardi else ritardo,
        "arrivato": int(arrivato),
        "ha_dati": int(ritardo is not None or cancellato or n_cancellate > 0),
        "avvisi": avvisi or None,
        "grezzo": json.dumps(_senza_calendario(corsa), ensure_ascii=False) if anomalo else None,
    }


def _senza_calendario(corsa: dict) -> dict:
    """Copia della corsa senza il lungo elenco delle date di circolazione."""
    copia = json.loads(json.dumps(corsa))
    for t in copia.get("journey_list", []):
        t.get("train", {}).pop("schedule", None)
    return copia


def salva(db: sqlite3.Connection, riga: dict, ora: str) -> None:
    vecchia = db.execute(
        "SELECT * FROM treni WHERE data = ? AND codice = ?", (riga["data"], riga["codice"])
    ).fetchone()
    if vecchia is None:
        colonne = list(riga) + ["primo_rilevamento", "ultimo_rilevamento"]
        db.execute(
            f"INSERT INTO treni ({', '.join(colonne)}) VALUES ({', '.join('?' * len(colonne))})",
            [*riga.values(), ora, ora],
        )
        return
    # L'API dimentica i dati in tempo reale qualche ora dopo l'arrivo: una
    # risposta vuota non deve sovrascrivere quello che è già stato rilevato.
    if vecchia["ha_dati"] and not riga["ha_dati"]:
        db.execute(
            "UPDATE treni SET ultimo_rilevamento = ? WHERE data = ? AND codice = ?",
            (ora, riga["data"], riga["codice"]),
        )
        return
    if vecchia["ritardo_max"] is not None:
        riga["ritardo_max"] = max(vecchia["ritardo_max"], riga["ritardo_max"] or vecchia["ritardo_max"])
    riga["avvisi"] = riga["avvisi"] or vecchia["avvisi"]
    riga["grezzo"] = riga["grezzo"] or vecchia["grezzo"]
    assegna = ", ".join(f"{c} = ?" for c in riga)
    db.execute(
        f"UPDATE treni SET {assegna}, ultimo_rilevamento = ? WHERE data = ? AND codice = ?",
        [*riga.values(), ora, riga["data"], riga["codice"]],
    )


# --------------------------------------------------------------------------
# Archivio in CSV (un file per giorno)
# --------------------------------------------------------------------------

COLONNE_CSV = [
    "data", "codice", "numero", "linea", "categoria", "direttrice", "origine", "destinazione",
    "partenza_prev", "arrivo_prev", "stato", "cancellato", "fermate", "fermate_cancellate",
    "ritardo", "ritardo_max", "arrivato", "ha_dati", "avvisi", "primo_rilevamento", "ultimo_rilevamento",
]
COLONNE_RIEPILOGO = [
    "data", "treni", "cancellati", "parziali", "misurati", "arrivati", "in_ritardo",
    "minuti_totali", "media", "mediana", "massimo",
]
SOGLIA_RIEPILOGO = 5  # minuti oltre i quali un treno conta come in ritardo in riepilogo.csv


def carica_archivio(db: sqlite3.Connection, cartella: Path, giorni: list[str] | None = None) -> None:
    """Porta nel database i CSV dell'archivio (tutti, o solo i giorni indicati)."""
    file = sorted(cartella.glob("20??-??-??.csv")) if giorni is None else [cartella / f"{g}.csv" for g in giorni]
    segnaposto = ", ".join("?" * len(COLONNE_CSV))
    for f in file:
        if not f.exists():
            continue
        with open(f, newline="", encoding="utf-8") as fh:
            righe = [[r.get(c) or None for c in COLONNE_CSV] for r in csv.DictReader(fh)]
        db.execute("DELETE FROM treni WHERE data = ?", (f.stem,))
        db.executemany(f"INSERT OR REPLACE INTO treni ({', '.join(COLONNE_CSV)}) VALUES ({segnaposto})", righe)
    db.commit()


def esporta_archivio(db: sqlite3.Connection, cartella: Path, giorni: list[str]) -> None:
    """Riscrive i CSV dei giorni indicati e aggiorna riepilogo.csv."""
    cartella.mkdir(parents=True, exist_ok=True)
    nuovi = {}
    for g in giorni:
        righe = db.execute(
            f"SELECT {', '.join(COLONNE_CSV)} FROM treni WHERE data = ? ORDER BY partenza_prev, codice", (g,)
        ).fetchall()
        if not righe:
            continue
        with open(cartella / f"{g}.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(COLONNE_CSV)
            w.writerows(list(r) for r in righe)
        s = riepilogo(db, g, SOGLIA_RIEPILOGO)
        nuovi[g] = {c: (round(s[c], 2) if isinstance(s[c], float) else s[c]) for c in COLONNE_RIEPILOGO}

    percorso = cartella / "riepilogo.csv"
    tutti = {}
    if percorso.exists():
        with open(percorso, newline="", encoding="utf-8") as fh:
            tutti = {r["data"]: r for r in csv.DictReader(fh)}
    tutti.update(nuovi)
    with open(percorso, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, COLONNE_RIEPILOGO, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(tutti[g] for g in sorted(tutti))
    if nuovi:
        esporta_mensile(db, cartella, {g[:7] for g in nuovi})


# --------------------------------------------------------------------------
# Riepilogo mensile per direttrice (soglia per l'indennizzo agli abbonati)
# --------------------------------------------------------------------------

# L'indennizzo spetta se in un mese almeno il 10% delle corse di una direttrice
# è stato soppresso, del tutto o in parte, o è arrivato con più di 15 minuti di
# ritardo. Per gli abbonamenti IVOL il calcolo è su tutte le direttrici insieme.
SOGLIA_INDENNIZZO_PCT = 10.0
RITARDO_INDENNIZZO_MIN = 15
TUTTE = "Tot"

# Trenord pubblica ogni mese una tabella con l'"Indice" di ogni direttrice: è
# la percentuale di corse soppresse, anche in parte, o arrivate con più di 15
# minuti di ritardo. La tabella usa codici diversi da quelli dell'API: questa è
# la corrispondenza, presa da www.trenord.it/store-api/directors/<codice>.
CODICE_UFFICIALE = {
    "D001": "01", "D002": "03", "D003": "04", "D004": "05", "D005": "06", "D006": "07",
    "D007": "08", "D008": "09", "D009": "10", "D010": "11", "D011": "12", "D012": "15",
    "D013": "16", "D014": "17", "D015": "18", "D016": "19", "D017": "20", "D018": "21",
    "D019": "22", "D020": "23", "D021": "24", "D022": "25", "D023": "26", "D024": "27",
    "D025": "28", "D026": "29", "D027": "40", "D028": "34", "D029": "35", "D030": "35",
    "D031": "36", "D032": "31", "D033": "32", "D034": "30", "D035": "33", "D036": "37",
    "D037": "50", "D038": "02", "D039": "13", "D040": "14", "D041": "39", "D042": "42",
}
# Nomi come nella tabella di Trenord
DIRETTRICI = {
    "01": "Novara-Milano-Treviglio", "02": "Saronno-Seregno-Milano-Albairate",
    "03": "Domodossola-Gallarate-Milano", "04": "P.to Ceresio-Varese-Gallarate-Milano",
    "05": "Luino-Gallarate-Milano", "06": "Chiasso-Como-Monza-Milano",
    "07": "Tirano-Sondrio-Lecco-Milano", "08": "Lecco-Molteno-Monza-Milano",
    "09": "Chiavenna-Colico", "10": "Lecco-Molteno-Como", "11": "Lecco-Bergamo-Brescia",
    "12": "Bergamo-Carnate-Milano", "13": "Seregno-Carnate (fuori dalla tabella Trenord)",
    "14": "Bergamo-Treviglio", "15": "Bergamo-Pioltello-Milano", "16": "Cremona-Treviglio",
    "17": "Verona-Brescia-Treviglio-Milano", "18": "Brescia-Piadena-Parma",
    "19": "Brescia-Cremona", "20": "Mantova-Cremona-Lodi-Milano", "21": "Piacenza-Lodi-Milano",
    "22": "Alessandria-Pavia-Milano", "23": "Stradella-Pavia-Milano", "24": "Pavia-Codogno",
    "25": "Alessandria-Mortara-Milano", "26": "Mortara-Novara",
    "27": "Pavia-Torreberetti-Alessandria", "28": "Pavia-Mortara-Vercelli",
    "29": "Voghera-Piacenza", "30": "Laveno-Varese-Saronno-Milano", "31": "Como-Saronno-Milano",
    "32": "Novara-Saronno-Milano", "33": "Asso-Seveso-Milano", "34": "Brescia-Iseo-Edolo",
    "35": "Malpensa-Milano", "36": "Saronno-Milano-Lodi (S1, S3, S12)",
    "37": "Mariano/Camnago-Seveso-Milano (S2, S4)", "39": "Lecco-Carnate-Milano",
    "40": "Treviglio-Milano Passante-Varese", "42": "Malpensa-Varese-Mendrisio-Como (S10, S40, S50)",
    "50": "Milano Rogoredo-Milano Bovisa (fuori dalla tabella Trenord)",
    "n.d.": "Direttrice non indicata",
    TUTTE: "Totale servizio Trenord (abbonamento IVOL)",
}
COLONNE_MENSILE = [
    "mese", "cod_dir", "direttrice", "giorni", "corse", "rilevate", "soppresse", "soppresse_in_parte",
    "ritardo_oltre_15", "irregolari", "indice", "copertura_pct", "indennizzo",
]


def mensile(db: sqlite3.Connection, mese: str) -> list[dict]:
    """Una riga per direttrice, più una per il totale, per il mese AAAA-MM.

    Le direttrici hanno codici e nomi della tabella mensile di Trenord, e
    "indice" è calcolato come il suo "Indice".

    Una corsa è "rilevata" se si sa com'è finita: soppressa, anche in parte,
    oppure arrivata con un ritardo registrato. I treni ancora in viaggio non
    contano finché non arrivano. La percentuale è calcolata sulle corse
    rilevate, perché di quelle senza dati non si può dire se fossero regolari.
    """
    gruppi: dict[str, dict] = {}
    for r in db.execute("SELECT * FROM treni WHERE data LIKE ?", (mese + "-%",)):
        for chiave in (CODICE_UFFICIALE.get(r["direttrice"], "n.d."), TUTTE):
            g = gruppi.setdefault(chiave, {
                "giorni": set(), "corse": 0, "rilevate": 0, "soppresse": 0,
                "soppresse_in_parte": 0, "ritardo_oltre_15": 0,
            })
            g["giorni"].add(r["data"])
            g["corse"] += 1
            giunto = r["ritardo"] is not None and (r["arrivato"] or r["stato"] == "A")
            if r["cancellato"]:
                g["soppresse"] += 1
            elif r["fermate_cancellate"]:
                g["soppresse_in_parte"] += 1
            elif giunto and r["ritardo"] > RITARDO_INDENNIZZO_MIN:
                g["ritardo_oltre_15"] += 1
            if r["cancellato"] or r["fermate_cancellate"] or giunto:
                g["rilevate"] += 1

    righe = []
    for chiave in sorted(gruppi, key=lambda k: (k == TUTTE, k)):
        g = gruppi[chiave]
        irregolari = g["soppresse"] + g["soppresse_in_parte"] + g["ritardo_oltre_15"]
        pct = round(100 * irregolari / g["rilevate"], 2) if g["rilevate"] else None
        righe.append({
            "mese": mese, "cod_dir": chiave, "direttrice": DIRETTRICI.get(chiave, ""),
            **g, "giorni": len(g["giorni"]), "irregolari": irregolari, "indice": pct,
            "copertura_pct": round(100 * g["rilevate"] / g["corse"], 1),
            "indennizzo": "" if pct is None else ("si" if pct >= SOGLIA_INDENNIZZO_PCT else "no"),
        })
    return righe


def esporta_mensile(db: sqlite3.Connection, cartella: Path, mesi: set[str]) -> None:
    """Aggiorna mensile.csv per i mesi indicati, rileggendo i CSV di quei mesi."""
    percorso = cartella / "mensile.csv"
    vecchie = []
    if percorso.exists():
        with open(percorso, newline="", encoding="utf-8") as fh:
            vecchie = [r for r in csv.DictReader(fh) if r["mese"] not in mesi and "cod_dir" in r]
    nuove = []
    for mese in sorted(mesi):
        carica_archivio(db, cartella, sorted(f.stem for f in cartella.glob(f"{mese}-??.csv")))
        nuove += mensile(db, mese)
    with open(percorso, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, COLONNE_MENSILE, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(vecchie + nuove, key=lambda r: (r["mese"], r["cod_dir"] == TUTTE, r["cod_dir"])))


def stampa_mensile(mese: str, archivio: Path | None) -> None:
    db = apri_db()
    if archivio:
        carica_archivio(db, archivio, sorted(f.stem for f in archivio.glob(f"{mese}-??.csv")))
    righe = mensile(db, mese)
    if not righe:
        print(f"Nessun dato per {mese}.")
        return
    totale = righe[-1]
    print(f"\nDettaglio del servizio Trenord, {mese}")
    print(f"Indice: % di corse soppresse, anche in parte, o arrivate con più di {RITARDO_INDENNIZZO_MIN} minuti di ritardo.")
    print(f"Indennizzo se l'Indice è almeno {SOGLIA_INDENNIZZO_PCT:.0f}%. Giorni raccolti: {totale['giorni']}.\n")
    print(f"{'Cod':<5}{'Direttrice':<47}{'corse':>7}{'rilev.':>8}{'soppr.':>8}{'parz.':>7}{'>15 min':>9}{'Indice':>8}  Indennizzo")
    for r in righe:
        if r["cod_dir"] == TUTTE:
            print("-" * 111)
        indice = "–" if r["indice"] is None else f"{r['indice']:.2f}".replace(".", ",")
        esito = {"si": "SI", "no": "No"}.get(r["indennizzo"], "–")
        print(f"{r['cod_dir']:<5}{r['direttrice'][:46]:<47}{r['corse']:>7}{r['rilevate']:>8}{r['soppresse']:>8}"
              f"{r['soppresse_in_parte']:>7}{r['ritardo_oltre_15']:>9}{indice:>8}  {esito}")
    print(f"\nCorse con esito rilevato: {str(totale['copertura_pct']).replace('.', ',')}% del totale. "
          "L'Indice è calcolato su queste.")
    print("Stima su dati non ufficiali: l'elenco che dà diritto all'indennizzo è quello pubblicato da Trenord.")


# --------------------------------------------------------------------------
# Raccolta
# --------------------------------------------------------------------------

def da_interrogare(db: sqlite3.Connection, giorno: date, tutti: bool) -> list[str]:
    """Numeri di treno da chiedere all'API in questo passaggio."""
    previsti = treni_del_giorno(giorno)
    if tutti:
        return sorted(previsti, key=int)
    ora = adesso()
    minuti_ora = (ora.date() - giorno).days * 1440 + ora.hour * 60 + ora.minute

    # corse già viste: (numero, partenza HH:MM) -> riga del database
    viste = {
        (r["numero"], (r["partenza_prev"] or "")[:5]): r
        for r in db.execute("SELECT * FROM treni WHERE data = ?", (giorno.isoformat(),))
    }
    scelti = []
    for numero, corse in previsti.items():
        for partenza, arrivo in corse:
            if not arrivo + ATTESA_ARRIVO_MIN <= minuti_ora <= arrivo + SCADENZA_ORE * 60:
                continue                    # troppo presto, o dati non più disponibili
            r = viste.get((numero, f"{partenza // 60 % 24:02d}:{partenza % 60:02d}"))
            if r is not None:
                if r["cancellato"] or (r["arrivato"] and r["ha_dati"]):
                    continue                # esito definitivo già registrato
                ultima = datetime.fromisoformat(r["ultimo_rilevamento"])
                if ora < ultima + timedelta(minutes=CACHE_MIN + 1):
                    continue                # il sito darebbe ancora la stessa risposta
            scelti.append(numero)
            break
    return sorted(scelti, key=int)


def collect(tutti: bool = False, silenzioso: bool = False, archivio: Path | None = None) -> None:
    """Un passaggio di raccolta. Solleva Bloccato se il sito rifiuta le richieste."""
    db = apri_db()
    ora = adesso()
    # Fino alle 3 di notte si seguono ancora i treni partiti il giorno prima.
    giorni = [ora.date()] if ora.hour >= 3 else [ora.date() - timedelta(days=1), ora.date()]
    if archivio:
        carica_archivio(db, archivio, [g.isoformat() for g in giorni])
    numeri = sorted({n for g in giorni for n in da_interrogare(db, g, tutti)}, key=int)
    if not silenzioso:
        durata = len(numeri) * (PAUSA + 0.3) / 60
        print(f"[{ora:%H:%M}] {len(numeri)} treni da interrogare (circa {durata:.0f} min)…", flush=True)

    falliti = 0
    try:
        for i, numero in enumerate(numeri, 1):
            esito = scarica_treno(numero)
            if esito is None:
                falliti += 1
                continue
            risposta, momento = esito
            for corsa in risposta:
                riga = leggi_corsa(corsa)
                if riga:
                    salva(db, riga, momento.isoformat(timespec="seconds"))
            if i % 100 == 0:
                db.commit()
                if not silenzioso:
                    print(f"  {i}/{len(numeri)}", flush=True)
    finally:
        db.commit()
        if archivio:  # si salva anche quello raccolto prima di un eventuale blocco
            esporta_archivio(db, archivio, [g.isoformat() for g in giorni])
        db.close()
    if not silenzioso:
        coda = f", {falliti} richieste fallite" if falliti else ""
        print(f"[{adesso():%H:%M}] fatto{coda}.", flush=True)


def watch(ogni_minuti: int) -> None:
    print(f"Raccolta ogni {ogni_minuti} minuti. Ctrl+C per fermare.")
    while True:
        inizio = time.time()
        try:
            collect()
        except Bloccato as e:
            print(f"Il sito ha bloccato le richieste ({e}): pausa di {ATTESA_BLOCCO_MIN} minuti.", file=sys.stderr)
            time.sleep(ATTESA_BLOCCO_MIN * 60)
            continue
        except Exception as e:  # un errore di rete non deve fermare il ciclo
            print(f"Errore nel passaggio: {e}", file=sys.stderr)
        time.sleep(max(60, ogni_minuti * 60 - (time.time() - inizio)))


# --------------------------------------------------------------------------
# Riepilogo
# --------------------------------------------------------------------------

def riepilogo(db: sqlite3.Connection, giorno: str, soglia: int) -> dict | None:
    righe = db.execute("SELECT * FROM treni WHERE data = ?", (giorno,)).fetchall()
    if not righe:
        return None
    cancellati = [r for r in righe if r["cancellato"]]
    parziali = [r for r in righe if not r["cancellato"] and r["fermate_cancellate"]]
    misurati = [r for r in righe if not r["cancellato"] and r["ritardo"] is not None]
    ritardi = [r["ritardo"] for r in misurati]
    in_ritardo = [x for x in ritardi if x > soglia]
    return {
        "data": giorno,
        "treni": len(righe),
        "cancellati": len(cancellati),
        "parziali": len(parziali),
        "misurati": len(misurati),
        "arrivati": sum(1 for r in misurati if r["arrivato"]),
        "in_ritardo": len(in_ritardo),
        "minuti_totali": sum(x for x in ritardi if x > 0),
        "media": statistics.mean(ritardi) if ritardi else None,
        "media_ritardatari": statistics.mean(in_ritardo) if in_ritardo else None,
        "mediana": statistics.median(ritardi) if ritardi else None,
        "massimo": max(ritardi) if ritardi else None,
        "fasce": {
            "in orario o in anticipo (≤ 0)": sum(1 for x in ritardi if x <= 0),
            "1-5 min": sum(1 for x in ritardi if 1 <= x <= 5),
            "6-15 min": sum(1 for x in ritardi if 6 <= x <= 15),
            "16-30 min": sum(1 for x in ritardi if 16 <= x <= 30),
            "31-60 min": sum(1 for x in ritardi if 31 <= x <= 60),
            "oltre 60 min": sum(1 for x in ritardi if x > 60),
        },
        "righe": righe,
        "peggiori": sorted(misurati, key=lambda r: -r["ritardo"])[:10],
        "lista_cancellati": cancellati,
        "lista_parziali": parziali,
    }


def _pct(parte: int, totale: int) -> str:
    return f"{100 * parte / totale:.1f}%".replace(".", ",") if totale else "–"


def _num(x: float | None) -> str:
    return "–" if x is None else f"{x:.1f}".replace(".", ",")


def report(giorno: str, soglia: int, csv_path: str | None, giorni: int, archivio: Path | None = None) -> None:
    db = apri_db()
    if archivio:
        carica_archivio(db, archivio)
    if giorni > 1:
        fine = datetime.strptime(giorno, "%Y-%m-%d").date()
        print(f"{'data':<12}{'treni':>7}{'cancellati':>12}{'parziali':>10}{'misurati':>10}"
              f"{f'ritardo>{soglia}':>12}{'media min':>11}{'max':>6}")
        for k in range(giorni - 1, -1, -1):
            s = riepilogo(db, (fine - timedelta(days=k)).isoformat(), soglia)
            if s:
                print(f"{s['data']:<12}{s['treni']:>7}{s['cancellati']:>12}{s['parziali']:>10}"
                      f"{s['misurati']:>10}{s['in_ritardo']:>12}{_num(s['media']):>11}"
                      f"{s['massimo'] if s['massimo'] is not None else '–':>6}")
        return

    s = riepilogo(db, giorno, soglia)
    if not s:
        print(f"Nessun dato per il {giorno}. Lancia prima `collect`.")
        return

    print(f"\nTrenord, {giorno}")
    print("=" * 40)
    print(f"Treni monitorati:            {s['treni']}")
    print(f"Cancellati per intero:       {s['cancellati']} ({_pct(s['cancellati'], s['treni'])})")
    print(f"Cancellati in parte:         {s['parziali']} ({_pct(s['parziali'], s['treni'])})")
    print(f"Con ritardo rilevato:        {s['misurati']} (di cui {s['arrivati']} già arrivati)")
    print(f"In ritardo oltre {soglia} minuti:   {s['in_ritardo']} ({_pct(s['in_ritardo'], s['misurati'])} dei rilevati)")
    print(f"Ritardo medio:               {_num(s['media'])} min (mediana {_num(s['mediana'])})")
    print(f"Ritardo medio dei ritardatari: {_num(s['media_ritardatari'])} min")
    print(f"Minuti di ritardo in totale: {s['minuti_totali']}")
    print(f"Ritardo massimo:             {s['massimo'] if s['massimo'] is not None else '–'} min")
    print("\nDistribuzione dei ritardi")
    for fascia, n in s["fasce"].items():
        print(f"  {fascia:<32}{n:>5}  {_pct(n, s['misurati'])}")

    def riga_treno(r: sqlite3.Row) -> str:
        return (f"  {r['numero']:>6} {r['linea'] or '':<6} {(r['partenza_prev'] or '')[:5]} "
                f"{r['origine']} → {r['destinazione']}")

    if s["lista_cancellati"]:
        print("\nTreni cancellati")
        for r in s["lista_cancellati"]:
            print(riga_treno(r))
    if s["lista_parziali"]:
        print("\nTreni cancellati in parte")
        for r in s["lista_parziali"]:
            print(f"{riga_treno(r)}  ({r['fermate_cancellate']} fermate su {r['fermate']})")
    if s["peggiori"]:
        print("\nI ritardi maggiori")
        for r in s["peggiori"]:
            print(f"{riga_treno(r)}  +{r['ritardo']} min")

    if csv_path:
        colonne = [c for c in s["righe"][0].keys() if c != "grezzo"]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colonne)
            w.writerows([r[c] for c in colonne] for r in s["righe"])
        print(f"\nDettaglio per treno salvato in {csv_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Cancellazioni e ritardi dei treni Trenord, giorno per giorno.")
    sub = p.add_subparsers(dest="comando", required=True)

    c = sub.add_parser("collect", help="un passaggio di raccolta sui treni di oggi")
    c.add_argument("--tutti", action="store_true", help="interroga subito tutti i treni del giorno (circa 2.500 richieste, 30 minuti); di norma non serve")
    c.add_argument("--silenzioso", action="store_true", help="nessun messaggio (utile con cron)")
    c.add_argument("--archivio", type=Path, metavar="CARTELLA", help="legge e riscrive i dati come CSV in questa cartella")

    w = sub.add_parser("watch", help="raccolta continua")
    w.add_argument("--every", type=int, default=30, metavar="MINUTI", help="intervallo tra i passaggi (default 30)")

    r = sub.add_parser("report", help="riepilogo di un giorno")
    r.add_argument("--date", default=adesso().date().isoformat(), help="giorno AAAA-MM-GG (default oggi)")
    r.add_argument("--soglia", type=int, default=5, help="minuti oltre i quali un treno conta come in ritardo (default 5)")
    r.add_argument("--csv", metavar="FILE", help="salva il dettaglio per treno in un CSV")
    r.add_argument("--days", type=int, default=1, metavar="N", help="tabella degli ultimi N giorni fino a --date")
    r.add_argument("--archivio", type=Path, metavar="CARTELLA", help="carica prima i CSV di questa cartella")

    m = sub.add_parser("mensile", help="corse irregolari per direttrice e soglia per l'indennizzo")
    m.add_argument("--mese", default=adesso().strftime("%Y-%m"), help="mese AAAA-MM (default quello in corso)")
    m.add_argument("--archivio", type=Path, metavar="CARTELLA", help="carica prima i CSV di questa cartella")

    a = p.parse_args()
    if a.comando == "collect":
        try:
            collect(a.tutti, a.silenzioso, a.archivio)
        except Bloccato as e:
            sys.exit(f"Il sito ha bloccato le richieste ({e}). Riprova più tardi, senza insistere.")
    elif a.comando == "watch":
        watch(a.every)
    elif a.comando == "mensile":
        stampa_mensile(a.mese, a.archivio)
    else:
        report(a.date, a.soglia, a.csv, a.days, a.archivio)


if __name__ == "__main__":
    main()
