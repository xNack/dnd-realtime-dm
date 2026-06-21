#!/usr/bin/env python3
# =============================================================================
#  Il cuore del progetto — Spark legge gli eventi e li fa arricchire da Claude
# =============================================================================
#
#  Come leggere questo file:
#
#  - Spark è un "motore" che legge dati che arrivano in continuazione (uno
#    "stream"). Qui i dati arrivano da Kafka, dal topic "dnd-events".
#  - Spark lavora a piccoli gruppi di messaggi alla volta, chiamati "micro-batch".
#    Per ogni gruppo, Spark esegue la nostra funzione `elabora_gruppo`.
#  - Dentro quella funzione, per ogni evento chiamiamo Claude (l'intelligenza
#    artificiale) e gli chiediamo un consiglio per il Dungeon Master.
#  - Il risultato (evento "arricchito") viene rispedito a Kafka, sul topic
#    "dnd-enriched", da cui poi Logstash lo salva in Elasticsearch.
#
#  Nota: questa versione è semplificata. Non tiene il conto dei punti vita (HP),
#  perché nelle partite reali online spesso non si capisce chi perde vita. Ci
#  concentriamo su ciò che conta davvero: il consiglio tattico per il DM.
#
#  Il file è diviso in 5 parti numerate. Leggile in ordine.
# =============================================================================

import os
import json
import datetime
import urllib.request                  # per chiamare l'API di Claude via HTTP (niente librerie extra)

from pyspark.sql import SparkSession   # serve per avviare Spark


# -----------------------------------------------------------------------------
# Parte 1 — Impostazioni (lette dal docker-compose, così non scriviamo dati
#           "fissi" nel codice). os.getenv("NOME", "default") legge una variabile.
# -----------------------------------------------------------------------------
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")   # indirizzo di Kafka
TOPIC_IN = os.getenv("SOURCE_TOPIC", "dnd-events")    # da dove leggiamo
TOPIC_OUT = os.getenv("SINK_TOPIC", "dnd-enriched")   # dove scriviamo
MODELLO = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5")  # quale modello Claude
CHIAVE_API = os.getenv("ANTHROPIC_API_KEY", "")       # la tua chiave segreta

if not CHIAVE_API or "INSERISCI" in CHIAVE_API:
    print("\n[!] Manca la chiave API! Apri il file .env e incolla ANTHROPIC_API_KEY.\n", flush=True)

# Indirizzo dell'API di Claude (la chiamiamo via HTTP, senza librerie aggiuntive).
API_URL = "https://api.anthropic.com/v1/messages"


# -----------------------------------------------------------------------------
# Parte 2 — Un po' di "contesto": teniamo da parte le ultime frasi, così Claude
#           capisce meglio la situazione (cosa è successo poco prima).
# -----------------------------------------------------------------------------
ultimi_eventi = []   # lista delle ultime frasi della partita


# -----------------------------------------------------------------------------
# Parte 3 — La chiamata a Claude.
#           Gli mandiamo la frase grezza (+ le ultime frasi come contesto).
#           Lui ci risponde con un piccolo JSON (un dizionario) già pronto.
# -----------------------------------------------------------------------------

# Questo testo spiega a Claude come deve rispondere (sempre solo JSON).
ISTRUZIONI = (
    "Sei l'assistente di un Dungeon Master in una partita di Dungeons & Dragons. "
    "Ti do una frase che descrive cosa è appena successo al tavolo. "
    "ATTENZIONE: il testo arriva da una trascrizione automatica di una sessione giocata "
    "online, quindi può contenere errori, nomi storpiati, parole sbagliate o più momenti "
    "uniti insieme. Spesso è il master che narra l'esito. Interpreta con buon senso e, se "
    "un'informazione non è chiara, NON inventarla (usa \"\" o null). "
    "Rispondi SOLO con un JSON con questi campi: "
    '"is_game_event" (true SOLO se è un vero evento di gioco: tiro di dado, attacco, '
    'incantesimo, prova di abilità, danno o cura; false se sono chiacchiere, battute, '
    'discussioni sulle regole o fuori gioco), '
    '"character" (nome del personaggio che agisce, "" se non si capisce), '
    '"action" (un breve RIASSUNTO in italiano corretto di cosa succede, max 12 parole, '
    'correggendo gli errori di trascrizione e rendendo la frase chiara e leggibile), '
    '"roll" (numero del dado se citato, altrimenti null), '
    '"roll_type" ("attack"/"skill"/"saving_throw"/"damage"/"other"), '
    '"party_risk" ("low"/"medium"/"high": quanto sembra pericolosa la situazione per il gruppo), '
    '"dm_hint" (UN consiglio tattico/narrativo per il DM, in italiano, max 35 parole).'
)

def chiedi_a_claude(frase):
    # Prepariamo il messaggio da inviare, con le ultime frasi come contesto.
    messaggio = (
        f"Ultime frasi: {json.dumps(ultimi_eventi[-5:], ensure_ascii=False)}\n"
        f"Nuova frase: \"{frase}\"\n"
        "Rispondi solo con il JSON."
    )

    # Prepariamo la richiesta HTTP verso l'API di Claude.
    payload = json.dumps({
        "model": MODELLO,
        "max_tokens": 400,
        "system": ISTRUZIONI,
        "messages": [{"role": "user", "content": messaggio}],
    }).encode("utf-8")
    richiesta = urllib.request.Request(
        API_URL, data=payload, method="POST",
        headers={
            "x-api-key": CHIAVE_API,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
    )
    with urllib.request.urlopen(richiesta, timeout=30) as r:
        risposta = json.loads(r.read().decode("utf-8"))

    # La risposta contiene il testo generato: prendiamolo e trasformiamolo in dizionario.
    testo = risposta["content"][0]["text"].strip()
    # A volte Claude mette il JSON dentro ```...```: teniamo solo da { a }.
    if "{" in testo and "}" in testo:
        testo = testo[testo.find("{"): testo.rfind("}") + 1]
    return json.loads(testo)   # da testo JSON a dizionario


# -----------------------------------------------------------------------------
# Parte 4 — Da evento grezzo a evento arricchito.
#           Usiamo la risposta di Claude per costruire l'evento finale,
#           scartando ciò che non è un vero evento di gioco.
# -----------------------------------------------------------------------------
def arricchisci(evento):
    frase = evento.get("text", "")

    # Proviamo a chiamare Claude. Se qualcosa va storto, non blocchiamo tutto:
    # usiamo dei valori "di riserva" così la pipeline continua a funzionare.
    try:
        ai = chiedi_a_claude(frase)
    except Exception as errore:
        print(f"[!] Claude non ha risposto bene: {errore}", flush=True)
        # In caso di errore teniamo l'evento (is_game_event=True) per non perdere dati.
        ai = {"is_game_event": True, "character": "", "action": frase[:60],
              "roll": None, "roll_type": "other",
              "party_risk": "unknown", "dm_hint": "(suggerimento non disponibile)"}

    # Filtro: se Claude dice che non è un vero evento di gioco (chiacchiere, regole,
    # battute...), lo scartiamo: non lo mandiamo avanti e non finisce in dashboard.
    if not ai.get("is_game_event", True):
        print(f"[--] scartato (non è gioco): {frase[:60]}", flush=True)
        return None   # None = "salta questo evento"

    # Costruiamo l'evento arricchito (quello che finirà in Kibana).
    arricchito = {
        "event_time": evento.get("t"),
        "session_id": evento.get("session_id"),
        "campaign": evento.get("campaign"),
        "raw_text": frase,
        "character": ai.get("character", ""),
        "action": ai.get("action", ""),
        "roll": ai.get("roll"),
        "roll_type": ai.get("roll_type", "other"),
        "party_risk": ai.get("party_risk", "unknown"),
        "dm_hint": ai.get("dm_hint", ""),
        "ingest_ts": datetime.datetime.utcnow().isoformat() + "Z",
    }

    ultimi_eventi.append({"t": evento.get("t"), "text": frase})
    print(f"[OK] {arricchito['event_time']} {arricchito['character']} "
          f"-> rischio={arricchito['party_risk']} | {arricchito['dm_hint']}", flush=True)
    return arricchito


# -----------------------------------------------------------------------------
# Parte 5 — Il collegamento con Spark.
#           `elabora_gruppo` viene chiamata da Spark per ogni micro-batch.
# -----------------------------------------------------------------------------
def elabora_gruppo(gruppo_df, numero_gruppo):
    # `gruppo_df` è una tabella di Spark con i messaggi arrivati da Kafka.
    # Il valore del messaggio è in byte: lo convertiamo in testo e lo portiamo
    # nel programma Python con .collect() (sono pochi messaggi, va bene così).
    # Ordiniamo per "offset" Kafka: così elaboriamo gli eventi nello STESSO ordine
    # in cui sono stati inviati (= ordine della partita). Senza, Spark potrebbe
    # processarli in ordine sparso dentro il micro-batch e l'orario di elaborazione
    # (@timestamp) risulterebbe leggermente fuori sequenza.
    righe = (gruppo_df.selectExpr("CAST(value AS STRING) AS testo", "offset")
             .orderBy("offset").collect())
    if not righe:
        return   # nessun messaggio in questo gruppo: non facciamo nulla

    risultati = []
    for riga in righe:
        evento = json.loads(riga["testo"])     # da testo JSON a dizionario
        arricchito = arricchisci(evento)       # <-- qui avviene la magia (Claude)
        if arricchito is None:
            continue                           # evento scartato dal filtro: lo saltiamo
        risultati.append((json.dumps(arricchito, ensure_ascii=False),))

    if not risultati:
        return   # tutti gli eventi del gruppo erano chiacchiere: niente da scrivere

    # Rimandiamo gli eventi arricchiti a Kafka, sul topic "dnd-enriched".
    spark = gruppo_df.sparkSession
    tabella_out = spark.createDataFrame(risultati, ["value"])
    (tabella_out.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("topic", TOPIC_OUT)
        .save())


def main():
    # 1) Avviamo Spark.
    spark = SparkSession.builder.appName("DnD-DM-Assistant").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")   # meno messaggi tecnici a video
    print(f"[avvio] In ascolto su Kafka topic '{TOPIC_IN}'", flush=True)

    # 2) Diciamo a Spark di leggere in streaming da Kafka.
    eventi = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA)
              .option("subscribe", TOPIC_IN)
              .option("startingOffsets", "latest")   # leggi solo i nuovi eventi
              .option("maxOffsetsPerTrigger", 10)    # max 10 eventi per micro-batch:
              .load())                               # così Spark scrive spesso e i record
                                                     # compaiono "a ondate" (utile per la demo)

    # 3) Per ogni micro-batch, esegui la nostra funzione `elabora_gruppo`.
    query = (eventi.writeStream
             .foreachBatch(elabora_gruppo)
             .option("checkpointLocation", "/tmp/spark-checkpoint-dnd")
             .start())

    # 4) Resta in ascolto per sempre (finché non fermiamo il programma).
    query.awaitTermination()


if __name__ == "__main__":
    main()
