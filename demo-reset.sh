#!/usr/bin/env bash
# =============================================================================
#  demo-reset.sh — Riparti puliti per una demo dal vivo.
#
#  Cosa fa:
#    1) Svuota SOLO i risultati vecchi (cancella l'indice Elasticsearch
#       "dnd-enriched"). La dashboard e la data view di Kibana RESTANO salvate.
#    2) Rigioca la partita dall'inizio (avvia il producer), a ritmo guardabile.
#
#  Prerequisito: lo stack deve essere già avviato ("docker compose up").
#
#  Uso:
#     ./demo-reset.sh                  # SPEED=4, START_DELAY=3 (default)
#     SPEED=2 ./demo-reset.sh          # ancora più lento (più tempo per parlare)
#     SPEED=8 START_DELAY=0 ./demo-reset.sh
# =============================================================================
set -e

SPEED="${SPEED:-4}"            # 4 = 4x il tempo reale (più basso = più lento)
START_DELAY="${START_DELAY:-3}"  # secondi di attesa prima del primo evento

echo "[reset] Svuoto i risultati vecchi (indice dnd-enriched)..."
curl -s -X DELETE "http://localhost:9200/dnd-enriched" >/dev/null || true
sleep 1

echo "[reset] Rigioco la partita: SPEED=${SPEED}, START_DELAY=${START_DELAY}s"
echo "[reset] Apri Kibana sulla dashboard 'DM Assistant - Live' e guarda gli eventi arrivare."
docker compose run --rm -e SPEED="$SPEED" -e START_DELAY="$START_DELAY" producer
