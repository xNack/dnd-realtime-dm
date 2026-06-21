#!/usr/bin/env bash
# =============================================================================
#  demo-reset.sh — Riparti puliti per una demo dal vivo (replay ripetibile).
#
#  Cosa fa, in ordine:
#    1) Ricrea il servizio Spark: così riparte dalla "coda vuota" di Kafka e
#       NON elabora gli eventi rimasti da una partita precedente (è ciò che
#       causava lo sfasamento dell'ordine).
#    2) Svuota i risultati vecchi (cancella l'indice Elasticsearch "dnd-enriched").
#       La dashboard e la data view di Kibana RESTANO salvate.
#    3) Aspetta che Spark sia di nuovo in ascolto.
#    4) Rigioca la partita dall'inizio (avvia il producer).
#
#  Risultato: ogni volta che lo lanci ottieni UNA partita pulita, in ordine.
#
#  Prerequisito: lo stack deve essere già avviato ("docker compose up").
#
#  Uso:
#     ./demo-reset.sh                  # SPEED=4 (demo veloce)
#     SPEED=2 ./demo-reset.sh          # via di mezzo (più tempo per parlare)
#
#  PARTITA A TEMPO REALE (stessi tempi del video originale, ~15 min):
#     SPEED=1 ./demo-reset.sh
#     (SPEED=1 rispetta gli intervalli esatti tra le frasi. Per sincronizzare
#      col video, premi Play quando compare la riga "[producer] inviato 00:00:00".)
# =============================================================================
set -e

SPEED="${SPEED:-4}"             # 4 = 4x il tempo reale (più basso = più lento; 1 = tempo reale)
START_DELAY="${START_DELAY:-0}" # 0: il producer parte subito (lo script aspetta già Spark)

echo "[reset] 1/4 Ricreo Spark (riparte dalla coda vuota: niente eventi vecchi)..."
docker compose up -d --force-recreate spark >/dev/null

echo "[reset] 2/4 Svuoto i risultati vecchi (indice dnd-enriched)..."
curl -s -X DELETE "http://localhost:9200/dnd-enriched" >/dev/null || true

echo "[reset] 3/4 Attendo che Spark sia in ascolto..."
for _ in $(seq 1 45); do
  if docker compose logs spark 2>/dev/null | grep -q "In ascolto"; then break; fi
  sleep 2
done
sleep 3   # piccolo margine perché Spark fissi la posizione "latest"

echo "[reset] 4/4 Rigioco la partita: SPEED=${SPEED}, START_DELAY=${START_DELAY}s"
echo "[reset] Apri Kibana sulla dashboard 'DM Assistant - Live' e guarda gli eventi arrivare."
docker compose run --rm -e SPEED="$SPEED" -e START_DELAY="$START_DELAY" producer
