# DM Assistant — Real-Time D&D Event Enrichment

Progetto conclusivo del corso **Technologies for Advanced Programming (TAP)** — AA 2025/2026.

Una pipeline di **stream processing** che arricchisce in tempo reale gli eventi di una
sessione di Dungeons & Dragons con suggerimenti tattici generati da un LLM (**Claude**),
visualizzati su una dashboard per il Dungeon Master.

**Guida passo-passo completa (installazione, avvio, demo):** vedi **[GUIDA.md](GUIDA.md)**.

---

## Architettura

```
YouTube ─(Whisper)→ transcript.jsonl ─→ Producer ─→ Kafka(dnd-events)
        ─→ Spark Structured Streaming ─(API)→ Claude ─→ Kafka(dnd-enriched)
        ─→ Logstash ─→ Elasticsearch ─→ Kibana (dashboard live)
```

| Fase | Tecnologia | Ruolo |
|---|---|---|
| Sorgente | **Whisper** | Trascrive un video YouTube in eventi con timestamp |
| Simulazione | **Producer (Python)** | Rigioca gli eventi rispettando i tempi reali |
| Streaming | **Apache Kafka** | Bus di messaggi (`dnd-events`, `dnd-enriched`) |
| Processing | **Spark Structured Streaming** | Legge e arricchisce ogni evento, filtra il "non gioco" |
| ML / servizio esterno | **Claude API** | Genera `dm_hint` e `party_risk` per ogni evento |
| Ingestion / Indexing | **Logstash → Elasticsearch** | Invia e indicizza gli eventi arricchiti |
| Visualization | **Kibana** | Dashboard real-time per il Dungeon Master |
| Orchestrazione | **Docker Compose** | Avvia tutto con un comando |

---

## Requisiti

- **Docker Desktop** con almeno **8 GB di RAM** assegnati.
- Una **chiave API Claude** (Anthropic) → <https://console.anthropic.com>.

Il progetto include già una sessione di esempio (`data/transcript.jsonl`): funziona
out-of-the-box senza dover trascrivere alcun video. Tutti i passaggi di installazione e
avvio sono descritti in [GUIDA.md](GUIDA.md).

---

## Struttura

```
dnd-realtime-dm/
├── docker-compose.yml        # orchestrazione di tutti i servizi
├── .env.example              # variabili (chiave Claude, modello)
├── data/                     # transcript.jsonl (gli eventi della partita)
├── transcribe/transcribe.py  # Whisper: YouTube → transcript.jsonl
├── producer/producer.py      # simulatore della partita → Kafka
├── spark/stream_job.py       # Spark + enrichment con Claude (cuore)
├── logstash/pipeline/        # Kafka(dnd-enriched) → Elasticsearch
├── kibana/setup.sh           # crea la data view automaticamente
├── GUIDA.md                  # guida passo-passo (italiano)
└── README.md
```
