# La pipeline spiegata passo-passo (con esempi reali)

Questo file segue **un solo evento** della partita lungo TUTTA la pipeline, mostrando
com'è fatto il dato a ogni passaggio.

> Idea chiave: un dato entra **grezzo** (una frase) ed esce **arricchito** (un
> suggerimento per il Dungeon Master, pronto da vedere su una dashboard).
> Questo è lo *stream processing*.

---

## Mappa generale

```
[1] Video YouTube  ──►  [2] Whisper  ──►  transcript.jsonl
                                               │
                                               ▼
                                     [3] Producer (replay)
                                               │  topic Kafka: dnd-events
                                               ▼
                                     [4] Apache Kafka
                                               │
                                               ▼
                            [5] Spark  ──►  [6] Claude (AI)
                                               │  topic Kafka: dnd-enriched
                                               ▼
                                     [7] Logstash
                                               │
                                               ▼
                                     [8] Elasticsearch
                                               │
                                               ▼
                                     [9] Kibana (dashboard)
```

Si segue un momento **reale** della partita: all'inizio del combattimento il
master dice una frase trascritta in modo impreciso — **"warador è il primo"** — e si vede
come la pipeline la trasforma in un consiglio chiaro per il Dungeon Master.

> Perché questo esempio è utile: la trascrizione è "sporca" (minuscole, nome storpiato).
> Claude non solo interpreta, ma **corregge** (`Warador`) e riassume. È proprio il valore
> aggiunto del progetto.

---

## [1] Sorgente — Video YouTube

Il punto di partenza è un video di una sessione di D&D: può essere un video di YouTube
oppure un file video locale. Contiene solo **audio**: le voci dei giocatori. Non è ancora
un "dato" utilizzabile.

**Cosa entra:** un file video/audio (es. `sessione.mp4`).

---

## [2] Whisper — da audio a testo

Whisper (il modello di OpenAI per il riconoscimento vocale) ascolta l'audio e lo trascrive
in frasi, **tenendo i tempi** (minuto:secondo) di ognuna. Viene eseguito **una volta sola**,
in locale, prima di avviare la pipeline, tramite lo script `transcribe/transcribe.py`.

**Cosa produce:** il file `data/transcript.jsonl` (generato da `transcribe.py`). Una riga =
una frase = un evento.

Esempio di **UNA riga** del file:

```json
{"t": "00:00:47", "text": "warador è il primo"}
```

- `t` = momento in cui è stata detta la frase
- `text` = la frase trascritta (ancora "grezza": tutto minuscolo, senza interpretazione)

---

## [3] Producer — "rigioca" la partita

Il producer (`producer/producer.py`) **rilegge** quel file e invia gli eventi a Kafka uno
alla volta, aspettando tra l'uno e l'altro per imitare il ritmo reale ("replay"). Prima di
inviare, aggiunge dei **metadati** (a quale sessione/campagna appartiene l'evento).

**Cosa produce:** un messaggio Kafka sul topic **`dnd-events`**.

Esempio del **messaggio** inviato:

```json
{
  "t": "00:00:47",
  "text": "warador è il primo",
  "session_id": "session-001",
  "campaign": "La Cripta del Re Goblin"
}
```

> Il messaggio viene inviato con **chiave = `session_id`**. In Kafka, i messaggi con la
> stessa chiave vanno nella stessa partizione e quindi **restano in ordine**.

---

## [4] Apache Kafka

Kafka è una coda di messaggi in tempo reale. Tiene gli eventi in ordine sul topic
`dnd-events` e li mette a disposizione di chi li vuole leggere (qui: Spark).

**Perché serve:** separa chi *produce* (il producer) da chi *consuma* (Spark). Se Spark
è lento o si riavvia, i messaggi restano lì pronti. Permette anche il *replay* e di
aggiungere in futuro altri consumatori.

**Il dato qui NON cambia:** è lo stesso messaggio del punto [3], in attesa di essere letto.

---

## [5] Spark — legge e coordina l'arricchimento

Spark (lo script `spark/stream_job.py`) legge gli eventi da `dnd-events` a piccoli gruppi
("micro-batch", qui fino a 10 messaggi per volta). Attenzione a due livelli diversi:

- **Spark** consegna un *gruppo* di frasi tutte insieme (il micro-batch);
- il **nostro codice** poi cicla il gruppo **una frase alla volta**.

Quindi l'arricchimento avviene **per singolo evento**: per ogni frase, una alla volta:

1. chiama **Claude** una volta (vedi punto [6]), passandogli **due cose**:
   - la **frase nuova** da interpretare;
   - le **ultime 5 frasi** già viste, come *contesto*. Servono perché una frase isolata
     spesso non si capisce: con ciò che è stato detto poco prima, Claude coglie il senso.
2. scarta le frasi che non sono vero gioco (chiacchiere, regole, fuori gioco);
3. usa la risposta per costruire l'**evento arricchito**.

> Esempio: la frase nuova `"warador è il primo"`, da sola, è ambigua ("il primo di cosa?").
> Ma con le ultime frasi come contesto (`"...le cose per l'iniziativa"`, `"uffa"`), Claude
> capisce che si parla dell'iniziativa di combattimento e risponde correttamente.

**Cosa Spark manda a Claude (esempio):**

```
Ultime frasi: [ ..."io ho sistemato tutte le cose per l'iniziativa", "uffa"... ]
Nuova frase: "warador è il primo"
```

---

## [6] Claude — l'intelligenza artificiale

Claude legge la frase, capisce cosa è successo e risponde **solo con un JSON** già
strutturato, con il suggerimento per il DM.

**Cosa Claude risponde (esempio):**

```json
{
  "is_game_event": true,
  "character": "Warador",
  "action": "Warador ha il turno per primo nel combattimento",
  "roll": null,
  "roll_type": "other",
  "party_risk": "medium",
  "dm_hint": "Warador agisce per primo: descrivi l'ambiente di battaglia e consenti azioni tatticamente vantaggiose dalla sua posizione iniziale."
}
```

Si nota cosa ha fatto Claude: ha **corretto** il nome (`warador` → `Warador`), ha **riscritto**
la frase storpiata in italiano chiaro, e ha aggiunto un **consiglio** per il DM. Qui non
c'è un tiro di dado, quindi `roll` è `null` e `roll_type` è `"other"`.

Il campo `is_game_event` è il **filtro**: se fosse `false` (chiacchiere, regole, fuori
gioco — la maggioranza delle frasi!) Spark scarterebbe la frase. Qui è `true`, quindi
Spark assembla l'evento finale.

---

## [6→7] L'evento arricchito torna su Kafka

Spark scrive il risultato sul topic **`dnd-enriched`**.

Esempio del **messaggio arricchito**:

```json
{
  "event_time": "00:00:47",
  "session_id": "session-001",
  "campaign": "La Cripta del Re Goblin",
  "raw_text": "warador è il primo",
  "character": "Warador",
  "action": "Warador ha il turno per primo nel combattimento",
  "roll": null,
  "roll_type": "other",
  "party_risk": "medium",
  "dm_hint": "Warador agisce per primo: descrivi l'ambiente di battaglia e consenti azioni tatticamente vantaggiose...",
  "ingest_ts": "2026-06-10T14:23:53Z"
}
```

Si nota la differenza con il punto [3]: prima c'era solo la frase grezza `"warador è il primo"`;
ora c'è **l'interpretazione completa** (nome corretto, azione chiara) + il suggerimento per
il DM. Ecco l'arricchimento. È stato aggiunto anche `ingest_ts` = quando Spark l'ha elaborato.

---

## [7] Logstash — porta i dati in Elasticsearch

Logstash (configurato in `logstash/pipeline/logstash.conf`) è lo strumento di *ingestion*:
legge i messaggi da `dnd-enriched`, li interpreta come JSON e li **consegna a Elasticsearch**,
un documento alla volta. Imposta anche il campo data (`@timestamp`) usato per ordinarli nel tempo.

**Il dato qui resta lo stesso** del punto [6→7]; cambia solo "dove va": da Kafka a Elasticsearch.

---

## [8] Elasticsearch — l'archivio ricercabile

Ogni evento arricchito diventa un **documento** dentro l'indice `dnd-enriched`.
Elasticsearch li conserva e permette di **cercarli, filtrarli e contarli** velocemente
(es. "tutti gli eventi con `party_risk = high`" oppure "solo la campagna X").

**Cosa contiene:** lo stesso JSON del punto [6→7], salvato e indicizzato.

---

## [9] Kibana — la dashboard del Dungeon Master

Kibana legge da Elasticsearch e **mostra i dati graficamente**, aggiornandosi da solo a
ogni nuovo evento. Il nostro evento "Warador è il primo" appare:

- nel **feed eventi** (tabella con `event_time`, `character`, `action`, `dm_hint`);
- nel grafico del **rischio del party** (`party_risk`);
- nell'**istogramma degli eventi nel tempo**.

**Cosa vede l'utente:** non più un JSON, ma una riga leggibile e un suggerimento pronto
all'uso per condurre la partita.

---

## Riepilogo: come cambia UN dato lungo la pipeline

| Tappa | Forma del dato (esempio) |
|---|---|
| [2] Whisper | `{"t":"00:00:47","text":"warador è il primo"}` |
| [3] Producer → Kafka | come sopra **+ session_id, campaign** |
| [5+6] Spark + Claude | corregge in **"Warador"**, aggiunge **action, party_risk, dm_hint** (e filtra il non-gioco) |
| [8] Elasticsearch | stesso JSON arricchito, **salvato e ricercabile** |
| [9] Kibana | una **riga/grafico** leggibile per il Dungeon Master |

In conclusione: **da una frase grezza a un consiglio tattico, in tempo reale.**
