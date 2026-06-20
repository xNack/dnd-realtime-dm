#!/usr/bin/env python3
# =============================================================================
#  IL SIMULATORE DELLA PARTITA
# =============================================================================
#
#  COSA FA:
#  - Legge il file con gli eventi della partita (data/transcript.jsonl).
#  - Invia un evento alla volta a Kafka (sul topic "dnd-events").
#  - Aspetta tra un evento e l'altro per imitare il ritmo della partita reale,
#    così sembra che la sessione stia avvenendo "in diretta".
# =============================================================================

import os
import json
import time

from kafka import KafkaProducer   # libreria per inviare messaggi a Kafka


# --- Impostazioni (arrivano dal docker-compose) ---
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "dnd-events")
FILE = os.getenv("TRANSCRIPT", "/app/data/transcript.jsonl")
SESSION_ID = os.getenv("SESSION_ID", "session-001")
CAMPAIGN = os.getenv("CAMPAIGN", "Campagna senza nome")
VELOCITA = float(os.getenv("SPEED", "8"))         # 8 = rigioca 8 volte più veloce
ATTESA_INIZIALE = float(os.getenv("START_DELAY", "25"))


def in_secondi(tempo):
    """Trasforma un orario 'HH:MM:SS' nel numero di secondi (es. 00:02:40 -> 160)."""
    pezzi = [int(x) for x in tempo.split(":")]
    while len(pezzi) < 3:        # se manca l'ora, mettiamo 0 davanti
        pezzi.insert(0, 0)
    ore, minuti, secondi = pezzi
    return ore * 3600 + minuti * 60 + secondi


def collega_kafka():
    """Si collega a Kafka. Se non è ancora pronto, riprova ogni 3 secondi."""
    for tentativo in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA,
                # ogni messaggio viene convertito da dizionario a testo JSON (in byte)
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                # anche la CHIAVE del messaggio viene convertita in byte
                key_serializer=lambda k: k.encode("utf-8"),
            )
        except Exception:
            print("[producer] Kafka non è ancora pronto, riprovo tra 3s...", flush=True)
            time.sleep(3)
    raise RuntimeError("Non riesco a collegarmi a Kafka.")


def main():
    # 1) Aspettiamo che Kafka e Spark si siano avviati.
    print(f"[producer] Attendo {ATTESA_INIZIALE:.0f}s che tutto sia pronto...", flush=True)
    time.sleep(ATTESA_INIZIALE)
    producer = collega_kafka()

    # 2) Leggiamo tutti gli eventi dal file (una riga = un evento).
    eventi = []
    with open(FILE, "r", encoding="utf-8") as f:
        for riga in f:
            riga = riga.strip()
            if riga:
                eventi.append(json.loads(riga))
    print(f"[producer] {len(eventi)} eventi da inviare (velocità {VELOCITA}x).", flush=True)

    # 3) Inviamo gli eventi uno a uno, rispettando i tempi della partita.
    secondo_precedente = None
    for evento in eventi:
        secondo_attuale = in_secondi(evento.get("t", "00:00:00"))

        # Calcoliamo quanto aspettare = distanza di tempo tra questo e il precedente.
        if secondo_precedente is not None:
            attesa = (secondo_attuale - secondo_precedente) / VELOCITA
            time.sleep(min(max(attesa, 0), 30))   # mai più di 30s di pausa
        secondo_precedente = secondo_attuale

        # Costruiamo il messaggio e lo inviamo a Kafka.
        messaggio = {
            "t": evento.get("t"),
            "text": evento.get("text", ""),
            "session_id": SESSION_ID,
            "campaign": CAMPAIGN,
        }
        # Usiamo session_id come CHIAVE del messaggio. In Kafka, tutti i messaggi
        # con la stessa chiave finiscono nella stessa "partizione" e quindi
        # mantengono l'ORDINE. Così gli eventi di una partita restano in sequenza,
        # anche se un domani avessimo più sessioni sullo stesso topic.
        producer.send(TOPIC, key=SESSION_ID, value=messaggio)
        producer.flush()   # assicura l'invio immediato
        print(f"[producer] inviato {messaggio['t']}  {messaggio['text'][:60]}", flush=True)

    print("[producer] Tutti gli eventi inviati. Partita finita!", flush=True)


if __name__ == "__main__":
    main()
