# ⚔️ DM Assistant — Real-Time D&D Event Enrichment

> Progetto conclusivo del corso **Technologies for Advanced Programming (TAP)** — AA 2025/2026.
> Una pipeline di **stream processing** che arricchisce in tempo reale gli eventi di una
> sessione di Dungeons & Dragons con suggerimenti tattici generati da un LLM (**Claude**),
> visualizzati su una dashboard per il Dungeon Master.

📘 **Guida passo-passo completa (per chi parte da zero):** vedi **[GUIDA.md](GUIDA.md)**.

---

## 🧩 Architettura

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
| Ingestion / Indexing | **Logstash → Elasticsearch** | Indicizza gli eventi arricchiti |
| Visualization | **Kibana** | Dashboard real-time per il Dungeon Master |
| Orchestrazione | **Docker Compose** | Avvia tutto con un comando |

---

## ✅ Requisiti

- **Docker Desktop** con almeno **8 GB di RAM** assegnati.
- Una **chiave API Claude** (Anthropic) → <https://console.anthropic.com>.

---

## 🚀 Avvio rapido (un comando)

```bash
# 1. Clona il repo
git clone https://github.com/TUO-UTENTE/dnd-realtime-dm.git
cd dnd-realtime-dm

# 2. Inserisci la tua chiave Claude
cp .env.example .env
#   apri .env e incolla ANTHROPIC_API_KEY=sk-ant-...

# 3. Avvia l'intera pipeline
docker compose up --build
```

Poi apri la dashboard su **<http://localhost:5601>** → **Discover** (o crea una Dashboard).

Il progetto include già una sessione di esempio (`data/transcript.jsonl`), quindi funziona
**out-of-the-box** senza dover trascrivere alcun video.

### Rivedere la partita da capo
```bash
docker compose restart producer
```

### Fermare e ripulire
```bash
docker compose down -v
```

---

## 🎬 (Opzionale) Usare un tuo video YouTube

```bash
cd transcribe
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # richiede anche ffmpeg installato
python transcribe.py "https://www.youtube.com/watch?v=XXXX" --model small --lang it
```
Genera `data/transcript.jsonl`. Dettagli in [GUIDA.md](GUIDA.md).

---

## 📂 Struttura

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

---

## 🌐 Porte esposte

| Servizio | URL |
|---|---|
| Kibana | http://localhost:5601 |
| Elasticsearch | http://localhost:9200 |
| Kafka (da host) | localhost:29092 |

---

## 🔒 Sicurezza & note

- La sicurezza di Elasticsearch è **disattivata**: configurazione pensata per uso **locale/didattico**.
- La chiave API resta nel file `.env`, che è in `.gitignore` e **non** viene caricato su GitHub.

---

## 📜 Licenza

Rilasciato per scopi didattici (corso TAP). Adatta la licenza secondo necessità (es. MIT).
