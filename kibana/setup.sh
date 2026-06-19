#!/bin/sh
# =============================================================================
#  Crea automaticamente la "data view" di Kibana (l'indice da visualizzare).
#  Gira una volta sola all'avvio, poi il container termina.
# =============================================================================
set -e

echo "[kibana-setup] Attendo che Kibana sia disponibile..."
until curl -s http://kibana:5601/api/status | grep -q 'available'; do
  sleep 5
done

echo "[kibana-setup] Kibana è pronto. Creo la data view 'dnd-enriched*'..."
curl -s -X POST "http://kibana:5601/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
        "data_view": {
          "title": "dnd-enriched*",
          "name": "D&D Enriched",
          "timeFieldName": "@timestamp",
          "allowNoIndex": true
        }
      }' || echo "[kibana-setup] (la data view potrebbe già esistere: nessun problema)"

echo ""
echo "[kibana-setup] Fatto. Apri Kibana su http://localhost:5601 -> Discover."
