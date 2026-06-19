# Spiegazione del codice — DM Assistant (Real-Time D&D)

Dispensa di studio per l'esame TAP. Spiega **a parole semplici** ogni pezzo della pipeline e ogni file di codice. Leggila dall'alto verso il basso: prima il quadro generale, poi i singoli file, infine le domande tipiche.

---

## 1. L'idea in una frase

Durante una partita di **Dungeons & Dragons** vengono dette tante frasi. Il progetto le prende, le fa **arricchire dall'intelligenza artificiale (Claude)** con un consiglio per il "Dungeon Master" (il narratore), e mostra tutto in una **dashboard in tempo reale**.

È un esempio di **stream processing**: dati che arrivano in continuazione e vengono elaborati al volo, non tutti insieme alla fine.

---

## 2. Il viaggio di un evento (la cosa più importante da saper spiegare)

```
[1] transcript.jsonl     file con le frasi della partita (già trascritte con Whisper)
        │
        ▼
[2] PRODUCER             rigioca le frasi una a una, rispettando i tempi → le manda a Kafka
        │
        ▼
[3] KAFKA (dnd-events)   "nastro trasportatore" che conserva i messaggi in ordine
        │
        ▼
[4] SPARK                legge gli eventi a piccoli gruppi (micro-batch)
        │                  e per ognuno chiama...
        ▼
[5] CLAUDE (API)         l'IA: capisce la frase e risponde con un JSON
        │                  (personaggio, azione, rischio, consiglio per il DM)
        ▼
[6] KAFKA (dnd-enriched) Spark rimette gli eventi ARRICCHITI su un secondo topic
        │
        ▼
[7] LOGSTASH             legge dnd-enriched e li salva in...
        │
        ▼
[8] ELASTICSEARCH        database che indicizza i dati (ricerca veloce)
        │
        ▼
[9] KIBANA               dashboard: feed dei consigli + grafici, in tempo reale
```

Tutto è orchestrato da **Docker Compose**: un solo comando (`docker compose up`) accende gli 8 servizi e li collega.

---

## 3. Le tecnologie, spiegate semplici

- **Kafka** = un "nastro trasportatore" di messaggi. Chi scrive si chiama *producer*, chi legge *consumer*. I messaggi sono organizzati in **topic** (qui due: `dnd-events` per i grezzi, `dnd-enriched` per gli arricchiti). Kafka **conserva l'ordine** e ricorda fin dove ogni consumer ha letto (l'**offset**).
- **Spark (Structured Streaming)** = il motore che elabora i dati in arrivo. Lavora a **micro-batch**: piccoli gruppi di messaggi, uno dopo l'altro.
- **Claude (API)** = il modello di intelligenza artificiale. È l'**enrichment esterno**: gli mandiamo la frase, lui ci restituisce informazioni strutturate (JSON).
- **Logstash** = il "tubo" che porta i dati dentro Elasticsearch (ingestion).
- **Elasticsearch** = il database che **indicizza** i dati per cercarli e aggregarli velocemente.
- **Kibana** = l'interfaccia grafica (dashboard) che legge da Elasticsearch.
- **Docker Compose** = avvia tutti i servizi insieme, isolati in container, già collegati in rete.

---

## 4. I file, uno per uno

### 4.1 `data/transcript.jsonl` — i dati di partenza
Un file di testo, **una riga = un evento**, in formato JSON:
```json
{"t": "00:00:33", "text": "vi faccio tirare l'iniziativa"}
```
- `t` = il momento (orario) in cui la frase è stata detta nella partita.
- `text` = la frase trascritta.

È stato creato **una volta** con Whisper (vedi `transcribe/transcribe.py`) partendo da un video. Durante la demo NON si trascrive dal vivo: si **rigioca** questo file.

---

### 4.2 `producer/producer.py` — il simulatore della partita ("replay")
**Cosa fa:** legge `transcript.jsonl` e manda le frasi a Kafka **una alla volta**, aspettando tra l'una e l'altra lo stesso tempo che c'era nella partita vera. Così, per Kafka e Spark, è come se la sessione stesse avvenendo ORA.

**Pezzi chiave da spiegare:**
- `in_secondi("00:02:40")` → converte l'orario in secondi, per calcolare quanto aspettare.
- `collega_kafka()` → si connette a Kafka; se non è pronto riprova (i servizi si avviano in tempi diversi).
- Il ciclo `for evento in eventi`: calcola l'attesa = differenza di tempo tra due frasi, diviso `SPEED`. Poi `time.sleep(...)` e invia il messaggio.
- **`SPEED`** = velocità di rigioco. `SPEED=1` = tempo reale; `SPEED=8` = 8 volte più veloce.
- **chiave del messaggio = `session_id`**: in Kafka i messaggi con la stessa chiave vanno nella stessa *partizione* e quindi **mantengono l'ordine**.

> Concetto d'esame: questo è il **replay** di dati registrati come se fossero live. Il producer rispetta la cadenza temporale originale.

---

### 4.3 `spark/stream_job.py` — il cuore (Spark + Claude)
È il file più importante. Diviso in 5 parti:

- **Parte 1 — Impostazioni:** legge da variabili d'ambiente (Kafka, topic, modello Claude, chiave API). Niente dati "fissi" nel codice.
- **Parte 2 — Contesto:** tiene le ultime 5 frasi, così Claude capisce meglio cosa sta succedendo.
- **Parte 3 — `chiedi_a_claude(frase)`:** costruisce la richiesta HTTP all'API di Claude. Nel campo `system` (la variabile `ISTRUZIONI`) gli diciamo **come** rispondere: solo un JSON con campi precisi (`is_game_event`, `character`, `action`, `roll`, `roll_type`, `party_risk`, `dm_hint`). Poi legge la risposta e la trasforma in dizionario.
- **Parte 4 — `arricchisci(evento)`:** chiama Claude; se la frase **non è un vero evento di gioco** (`is_game_event = false`, cioè chiacchiere) la **scarta**; altrimenti costruisce l'evento arricchito da salvare. Se Claude dà errore, usa valori "di riserva" per non bloccare la pipeline (robustezza).
- **Parte 5 — `elabora_gruppo(...)` + `main()`:** è la parte Spark.
  - `readStream ... .format("kafka") ... subscribe("dnd-events")` → legge in streaming.
  - `startingOffsets = "latest"` → legge solo i **nuovi** eventi.
  - `maxOffsetsPerTrigger = 10` → max **10 eventi per micro-batch**: così Spark scrive spesso e i record compaiono "a ondate" (importante per la demo dal vivo).
  - `foreachBatch(elabora_gruppo)` → per ogni micro-batch esegue la nostra funzione.
  - Dentro `elabora_gruppo`: prende i messaggi, per ognuno chiama `arricchisci()`, e i risultati li **riscrive su Kafka** nel topic `dnd-enriched`.
  - `checkpointLocation` → Spark salva il punto a cui è arrivato (per riprendere senza duplicare).

> Concetto d'esame: Spark fa **stream processing a micro-batch**; l'arricchimento è una **chiamata a un servizio ML esterno** (Claude) dentro `foreachBatch`.

---

### 4.4 `logstash/pipeline/logstash.conf` — l'ingestion
Tre blocchi:
- **input:** legge dal topic Kafka `dnd-enriched` (consumer group `logstash-dnd`, codec JSON).
- **filter:** copia il campo `ingest_ts` (prodotto da Spark) nel campo speciale **`@timestamp`**, che Kibana usa come asse del tempo. Rimuove campi tecnici inutili.
- **output:** salva ogni evento su **Elasticsearch**, indice `dnd-enriched`; e lo stampa anche a video (`stdout`) per vederlo durante la demo.

---

### 4.5 `docker-compose.yml` — l'orchestratore
Avvia **tutti i servizi con un solo comando** (`docker compose up`) e li collega.

**I 7 servizi:** `kafka` (bus messaggi) · `kafka-init` (crea i topic, poi si spegne) · `elasticsearch` (archivio) · `kibana` (dashboard) · `kibana-setup` (crea la data view, poi si spegne) · `logstash` (ingestion verso Elasticsearch) · `spark` (elabora + chiama Claude) · `producer` (rigioca la partita).

**Ordine di avvio:** prima le fondamenta **kafka** ed **elasticsearch** (in parallelo); poi i preparatori una tantum (**kafka-init**, **kibana-setup**); infine i lavoratori del flusso (**logstash**, **spark**, **producer**). Il producer è l'ultimo (aspetta 25s che Spark sia in ascolto). L'ordine è garantito da `depends_on` + `healthcheck`.

**Da citare:** `mem_limit` (tetto RAM per stare nei 16 GB) · `volumes` (salvano i dati tra un riavvio e l'altro) · `networks` (rete comune: i container si chiamano per nome, es. `kafka:9092`).

---

### 4.6 `transcribe/transcribe.py` — (usato una volta) Whisper
Scarica l'audio di un video con `yt-dlp` e lo trascrive con **Whisper**, producendo `transcript.jsonl`. NON serve durante la demo: serviva solo a creare i dati di partenza.

---

## 5. Concetti chiave (ripasso veloce)

- **Topic Kafka**: una "coda" tematica di messaggi. Qui due: grezzi e arricchiti.
- **Producer / Consumer**: chi scrive / chi legge su Kafka.
- **Offset / Consumer group**: Kafka ricorda fin dove un gruppo ha letto.
- **Micro-batch (Spark)**: piccolo gruppo di messaggi elaborati insieme.
- **Enrichment**: aggiungere informazioni a un dato grezzo (qui via Claude).
- **Indice (Elasticsearch)**: struttura che rende le ricerche/aggregazioni veloci.
- **Data view (Kibana)**: dice a Kibana quale indice mostrare e qual è il campo data.
- **`@timestamp`**: il campo tempo che Kibana usa per ordinare e per i grafici.
- **Replay**: rigiocare dati registrati rispettando i tempi originali, per simulare il "live".

---

## 6. Domande tipiche del prof (con risposta breve)

**Perché Kafka e non chiamare Claude direttamente dal producer?**
Per **disaccoppiare** le parti: il producer non sa nulla di Claude. Kafka fa da cuscinetto: se Spark è lento o si ferma, i messaggi restano nel topic e non si perdono. È il modello tipico delle pipeline real-time.

**Perché due topic (`dnd-events` e `dnd-enriched`)?**
Separano i dati **grezzi** (in ingresso) da quelli **arricchiti** (in uscita). Così altri consumer potrebbero leggere l'uno o l'altro in modo indipendente.

**Cosa fa esattamente Spark qui?**
Legge lo stream da Kafka a micro-batch, per ogni evento chiama Claude (enrichment) e filtra le chiacchiere, poi riscrive i risultati su Kafka. È il nodo di **processing** della pipeline.

**Perché i record compaiono con un piccolo ritardo rispetto all'audio?**
È la **latenza reale** della pipeline: ogni frase attraversa Kafka → Spark → Claude (1-3 s) → Elasticsearch → Kibana. Dimostra che l'elaborazione è davvero in streaming, non finta.

**La trascrizione è dal vivo?**
No: è stata fatta prima con Whisper. Durante la demo il producer fa **replay** del file trascritto, simulando l'arrivo in tempo reale.

**Come garantite l'ordine degli eventi?**
Usando `session_id` come **chiave** del messaggio Kafka: stessi messaggi → stessa partizione → ordine mantenuto.

**Cosa succede se Claude dà errore?**
La funzione `arricchisci` ha un blocco `try/except` con valori di riserva: la pipeline **non si blocca**, l'evento passa comunque (robustezza).

**Perché `maxOffsetsPerTrigger = 10`?**
Limita i messaggi per micro-batch così Spark scrive spesso e i risultati arrivano "a ondate", invece di tutti insieme alla fine. Migliore per una demo dal vivo.

**Dove sono le credenziali?**
La chiave API di Claude sta solo nel file `.env` (non versionato su GitHub). Il codice la legge da variabile d'ambiente.

---

## 7. Come studiare (ordine consigliato)

1. Memorizza lo **schema del viaggio di un evento** (sezione 2): è il filo conduttore.
2. Leggi `pipeline_spiegazione.md` per il "perché" di ogni scelta.
3. Apri `spark/stream_job.py` con questa dispensa accanto: è il pezzo su cui il prof insisterà.
4. Dai un'occhiata a `producer.py` e `logstash.conf` (più semplici).
5. Tieni a mente le **domande tipiche** (sezione 6).
6. Prova la demo con `COMANDI_ESAME.txt`.
