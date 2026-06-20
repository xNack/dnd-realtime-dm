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
#     ./demo-reset.sh                  # SPEED=4, START_DELAY=3 (default: demo veloce)
#     SPEED=2 ./demo-reset.sh          # via di mezzo (più tempo per parlare)
#     SPEED=8 START_DELAY=0 ./demo-reset.sh
#
#  PARTITA A TEMPO REALE (stessi tempi del video originale, ~15 min):
#     SPEED=1 START_DELAY=0 ./demo-reset.sh
#     (SPEED=1 rispetta gli intervalli esatti tra le frasi; START_DELAY=0 fa partire
#      subito, utile per sincronizzare con il video).
# =============================================================================
set -e

SPEED="${SPEED:-4}"            # 4 = 4x il tempo reale (più basso = più lento; 1 = tempo reale)
# START_DELAY = secondi di attesa prima di inviare il PRIMO evento. Serve a dare
# tempo agli altri servizi di essere pronti. Qui basta 3s perché lo stack è già su;
# metti 0 per partire subito (utile per sincronizzare con il video).
START_DELAY="${START_DELAY:-3}"

echo "[reset] Svuoto i risultati vecchi (indice dnd-enriched)..."
curl -s -X DELETE "http://localhost:9200/dnd-enriched" >/dev/null || true
sleep 1

echo "[reset] Rigioco la partita: SPEED=${SPEED}, START_DELAY=${START_DELAY}s"
echo "[reset] Apri Kibana sulla dashboard 'DM Assistant - Live' e guarda gli eventi arrivare."
docker compose run --rm -e SPEED="$SPEED" -e START_DELAY="$START_DELAY" producer
