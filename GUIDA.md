# GUIDA passo-passo — DM Assistant (Progetto TAP)

> Guida pensata per chi **parte da zero**: non serve conoscere Kafka, Spark o Elasticsearch.
> Segui i passi nell'ordine. Dove vedi un blocco grigio, è un comando da copiare nel terminale.

---

## 1. Che cos'è questo progetto (in parole semplici)

Immagina una partita di **Dungeons & Dragons**. Ogni cosa che succede (un attacco, un tiro
di dado, una cura) è un piccolo "evento". Questo progetto prende quel flusso di eventi
**in tempo reale**, lo fa leggere a un'intelligenza artificiale (**Claude**) e mostra al
Dungeon Master, su una dashboard, dei **suggerimenti tattici** e il **livello di rischio**
del gruppo mentre la partita va avanti.

È l'esempio perfetto di **stream processing**: un segnale entra grezzo → viene arricchito → esce utile.

Lo schema dell'architettura e il ruolo di ogni tecnologia (Kafka, Spark, Claude,
Logstash, Elasticsearch, Kibana, Docker) sono descritti nel [README.md](README.md).

---

## 2. Cosa ti serve PRIMA di iniziare (prerequisiti)

1. **Docker Desktop** installato e avviato.
   - Scaricalo da <https://www.docker.com/products/docker-desktop/>
   - Importante (Mac 16 GB): in **Settings → Resources → Memory** assegna a Docker
     circa **8 GB**. Lo stack è già limitato per consumarne ~6 GB di picco, quindi
     lasci RAM libera a macOS. Dettagli e regolazioni nella **sezione 13 (RAM)**.
   - Verifica che funzioni:
     ```bash
     docker --version
     docker compose version
     ```

2. **Una chiave API di Claude** (la otteniamo allo Step 2, è gratis creare l'account
   e si parte con un piccolo credito; l'uso di questo progetto costa pochi centesimi).

3. *(Solo se vuoi trascrivere un tuo video)* **Python 3** e **ffmpeg** sul tuo computer.
   Non è obbligatorio: nel progetto c'è già un `transcript.jsonl` di esempio pronto.

---

## 3. Step 1 — Scarica il progetto

Se il progetto è su GitHub:
```bash
git clone https://github.com/xNack/dnd-realtime-dm.git
cd dnd-realtime-dm
```

Se invece hai già la cartella `dnd-realtime-dm` sul computer, aprila nel terminale:
```bash
cd percorso/della/cartella/dnd-realtime-dm
```

---

## 4. Step 2 — Crea l'account Claude e ottieni la chiave API

> Importante: l'account dell'**app/chat** di Claude è diverso dall'accesso alla
> **Console per sviluppatori** (quella delle API). Per questo progetto serve la
> **Console**, su `console.anthropic.com`. Puoi comunque accedere con la stessa email.

### 4.1 — Crea l'account (se non lo hai ancora)
1. Apri **<https://console.anthropic.com>**.
2. Clicca **Sign up** e registrati con email + password, oppure con **Continue with Google**.
3. Controlla la posta: ti arriva un'email di **verifica** → clicca il link per confermare.
4. Inserisci nome e (se richiesto) il numero di telefono per la verifica via SMS.
5. Quando ti viene chiesto il tipo di utilizzo, puoi indicare un uso personale/didattico.

### 4.2 — Aggiungi un piccolo credito (necessario per le API)
Le API non sono incluse nell'abbonamento del chatbot: si pagano a consumo, ma costano
**pochissimo** (vedi sezione 11). Per attivarle:
1. In Console, menu in alto/laterale → **Billing** (o **Plans & Billing**).
2. Clicca **Add credits / Set up payment**, inserisci la carta e aggiungi un piccolo
   importo (es. **5 $**): è più che sufficiente per molte sessioni di test.

### 4.3 — Genera la chiave API
1. Vai in **Settings → API Keys** (oppure menu **API Keys**).
2. Clicca **Create Key**, dai un nome (es. `tap-dnd`) e conferma.
3. **Copia subito** la chiave: inizia con `sk-ant-...` e si vede **una sola volta**.
 - Conservala in un posto sicuro. Se la perdi, ne crei semplicemente una nuova.
 - **Non** condividerla e **non** caricarla su GitHub (il file `.env` è già in `.gitignore`).

Questa chiave è ciò che incollerai nel file `.env` allo Step 3.

> Come funziona l'API di Claude (in breve): il nostro programma manda a Claude un
> messaggio (l'evento di gioco + le ultime frasi come contesto) e Claude risponde con un
> JSON che contiene il suggerimento per il DM. Tutto questo è già scritto in
> `spark/stream_job.py` — non devi programmare nulla, ti basta inserire la chiave.

---

## 5. Step 3 — Configura il file `.env`

Nella cartella del progetto, copia il file di esempio e poi modificalo:

```bash
cp .env.example .env
```

Apri il file `.env` con un editor di testo e incolla la tua chiave:

```
ANTHROPIC_API_KEY=sk-ant-la-tua-chiave-qui
CLAUDE_MODEL=claude-haiku-4-5
```

- `claude-haiku-4-5` è il modello veloce ed economico (consigliato per il real-time).
- Se vuoi suggerimenti più "ricchi" puoi usare `claude-sonnet-4-6` (più lento e costoso).

---

## 6. Step 4 (OPZIONALE) — Trascrivi un tuo video con Whisper

Il progetto include già `data/transcript.jsonl` (una sessione di esempio): **puoi saltare
questo step** e andare diretto allo Step 5. Ma per la demo "vera" conviene usare un tuo video.

> **Consiglio sulla scelta del video:** prendi un segmento di **10–15 minuti di
> combattimento** (tanti attacchi, tiri, azioni). Evita le parti dove si spiega il
> programma o si fanno lunghe intro: sono "rumore" e allungano solo i tempi. La
> trascrizione automatica del parlato è imperfetta (nomi storpiati, frasi spezzate):
> è normale, Claude è già istruito a interpretarla con buon senso.

### Passo 1 — Installa ffmpeg (serve a Whisper per leggere l'audio)
- macOS: `brew install ffmpeg`
- Windows: `winget install ffmpeg` (oppure scaricalo da ffmpeg.org)
- Linux: `sudo apt install ffmpeg`

Verifica che sia installato:
```bash
ffmpeg -version
```

### Passo 2 — Prepara l'ambiente Python e installa le dipendenze
```bash
cd transcribe
python -m venv .venv
source .venv/bin/activate        # su Windows: .venv\Scripts\activate
pip install -r requirements.txt  # installa Whisper e yt-dlp
```

### Passo 3 — Lancia la trascrizione con il modello `medium`
Usa **`--model medium`**: è più preciso di `small` sui nomi e sull'italiano parlato veloce
(è più lento, ma lo lanci una volta sola). Puoi passare un **link YouTube** oppure un
**file già sul tuo computer**: lo script capisce da solo quale dei due è.
```bash
# opzione A — da YouTube
python transcribe.py "https://www.youtube.com/watch?v=XXXXXXXX" --model medium --lang it

# opzione B — da un file sul tuo computer (mp4, mkv, mp3, m4a...)
python transcribe.py "/Users/tuonome/Desktop/sessione.mp4" --model medium --lang it

# opzione C — scarica SOLO una parte del video YouTube (CONSIGLIATA: risparmi tempo e crediti)
python transcribe.py "https://youtu.be/XXXX" --start 00:10:00 --end 00:25:00 --model medium --lang it
```
La **prima volta** Whisper scarica il modello `medium` (~1.5 GB): è normale che ci metta
un po'. Al termine viene creato/aggiornato `../data/transcript.jsonl`.

> L'opzione C (`--start` / `--end`) scarica e trascrive **solo il segmento indicato**
> (es. dal minuto 10 al 25). È il modo migliore per prendere una scena di combattimento
> da un video lungo senza scaricare/trascrivere tutte le 3 ore.

### Passo 4 — (consigliato) Accorcia la trascrizione
Apri `data/transcript.jsonl` con un editor di testo ed **elimina le righe iniziali/finali
fuori gioco** (intro, spiegazioni). Una riga = un evento; tenere solo il combattimento
rende la demo più pulita e riduce le chiamate a Claude.

### Passo 5 — Torna nella cartella principale
```bash
deactivate
cd ..
```
Ora sei pronto per lo Step 5 (`docker compose up --build`).

> Modelli Whisper, dal più veloce al più preciso: `tiny` < `base` < `small` < **`medium`** < `large`.
> Per questo progetto **`medium`** è il miglior compromesso. Su Mac senza GPU la trascrizione
> richiede grosso modo un tempo simile alla durata del video (per questo conviene un clip breve).

---

## 7. Step 5 — Avvia TUTTO con un comando

Dalla cartella del progetto:

```bash
docker compose up --build
```

- La **prima volta** ci vogliono alcuni minuti: Docker scarica e costruisce le immagini.
- Vedrai scorrere i log di tutti i servizi. È normale.
- Lascia questa finestra aperta: è il "motore" acceso.

Cosa succede dietro le quinte, in ordine:
1. Partono Kafka, Elasticsearch e Kibana.
2. Spark si collega a Kafka e si mette in ascolto.
3. Dopo ~25 secondi il **producer** inizia a rigiocare la partita.
4. Per ogni evento, Spark chiama Claude e pubblica l'evento arricchito.
5. Logstash lo indicizza in Elasticsearch e Kibana lo mostra.

Vedrai nei log righe come:
```
dnd-spark    | [enrich] 00:07:52 Thorin -> risk=medium | Il boss è ferito: spingi le guardie...
```

Per **fermare tutto**: premi `Ctrl + C` nella finestra, poi (se vuoi ripartire da zero):
```bash
docker compose down -v
```

---

## 8. Step 6 — Guarda la dashboard (la demo per l'esame)

1. Apri il browser su **<http://localhost:5601>** (Kibana).
2. La **data view** `D&D Enriched` viene creata automaticamente. Se Kibana ti chiede di
 crearne una a mano: menu → **Stack Management → Data Views → Create data view**,
   nome indice `dnd-enriched*`, campo tempo `@timestamp`.
3. Menu → **Discover**: vedrai gli eventi arricchiti che arrivano in tempo reale.
   In alto a destra imposta l'intervallo su **"Last 15 minutes"** e attiva
   l'aggiornamento automatico (Auto-refresh, es. ogni 5s).
   Aggiungi a sinistra i campi: `character`, `dm_hint`, `party_risk`, `action`.

### Costruire una dashboard semplice (5 minuti)

Fai questi passi **mentre la pipeline gira**, così vedi i pannelli popolarsi.

**Apri il costruttore**

1. Menu → **Dashboard → Create dashboard**: vedi una tela vuota.
2. In alto a destra metti l'intervallo su **"Last 1 hour"** (così i dati ci sono di sicuro).

**Pannello 1 — Feed dei consigli DM (il cuore della demo)**

Si prende da Discover, perché mostra i record "uno a uno" come una chat:

1. → **Discover**, con le colonne `character`, `action`, `party_risk`, `dm_hint`.
2. In alto a destra **Save** → nome `Feed DM`.
3. Torna in dashboard → **Add from library** → seleziona `Feed DM`.

**Pannello 2 — Rischio del party (torta)**

1. Nella dashboard → **Create visualization** (si apre **Lens**).
2. Selettore tipo grafico → **Pie**.
3. Trascina il campo `party_risk` in **"Slice by"** (metrica = *Count of records*).
4. **Save and return**.

**Pannello 3 — Eventi nel tempo (mostra il real-time)**

1. **Create visualization** → Lens → tipo **Bar vertical stacked**.
2. **Horizontal axis** = `@timestamp` (istogramma temporale).
3. **Vertical axis** = *Count of records*.
4. *(Facoltativo)* **Break down by** = `party_risk`: barre colorate per rischio.
5. **Save and return**.

**Pannello 4 (facoltativo) — Totale eventi**

1. **Create visualization** → Lens → tipo **Metric** → *Count of records* → **Save and return**.

**Salva e attiva il live**

1. In alto **Save** → nome `DM Assistant – Live`.
2. Attiva l'**auto-refresh** con l'icona calendario/orologio (la stessa di Discover),
   es. ogni 5s, oppure premi **Refresh** a mano: i pannelli si aggiornano a ogni nuovo evento.

> Vuoi rivedere la partita da capo durante la demo? Usa lo script pronto (svuota i
> risultati vecchi e rigioca la partita a ritmo guardabile):
> ```bash
> ./demo-reset.sh
> ```
> In alternativa, a mano in un secondo terminale:
> ```bash
> curl -X DELETE http://localhost:9200/dnd-enriched   # svuota i risultati
> docker compose run --rm producer                    # rigioca la partita
> ```
> Cancella **solo** l'indice dei dati: la dashboard e la data view restano salvate.
> NON usare `docker compose down -v` (cancellerebbe anche dashboard e data view).

---

## 9. Come spiegarlo all'esame (mini-scaletta)

All'esame hai 30 minuti: design + demo + domande. Una traccia:

1. **Problema & dati**: una sessione D&D è un flusso di eventi con timestamp → caso d'uso
   naturale per lo stream processing. La sorgente reale è un video YouTube trascritto con Whisper.
2. **Architettura**: mostra lo schema (sezione 1) e segui il percorso di un evento.
3. **Ingestion (Logstash)**: legge gli eventi arricchiti da Kafka e li indicizza in Elasticsearch.
4. **Streaming (Kafka)**: disaccoppia produttore e consumatore; due topic `dnd-events` e `dnd-enriched`.
5. **Processing (Spark Structured Streaming)**: legge in micro-batch, filtra il "non gioco"
   e chiama l'LLM (`foreachBatch`).
6. **Machine Learning / servizio esterno (Claude API)**: genera `dm_hint` e `party_risk`.
7. **Indexing (Elasticsearch) + Visualization (Kibana)**: la dashboard live.
8. **Orchestrazione (Docker Compose)**: tutto parte con un comando.
9. **Demo live**: apri Kibana, fai `docker compose restart producer`, mostra i suggerimenti che arrivano.

Domande tipiche e risposte pronte:
- *Perché Kafka e non chiamare Spark direttamente?* → Disaccoppiamento, durabilità,
  possibilità di replay e di aggiungere altri consumatori.
- *Come eviti di elaborare le chiacchiere fuori gioco?* → Claude marca ogni frase con
  `is_game_event`; Spark scarta quelle non di gioco (filtro semantico via LLM).
- *Come gestisci gli errori dell'LLM?* → Se Claude non risponde o dà JSON non valido, c'è
  un fallback che non blocca la pipeline (vedi `arricchisci` in `stream_job.py`).

---

## 9-bis. Checklist giorno d'esame

> Stampa o tieni aperta questa lista. È pensata per non lasciare nulla al caso.

### La sera prima (a casa)
- [ ] Trascritto il clip con Whisper `medium` e accorciato `data/transcript.jsonl` (solo combattimento).
- [ ] Lanciato **almeno una volta** `docker compose up --build` con successo (così le immagini Docker restano in cache).
- [ ] Visto arrivare gli eventi in **Kibana → Discover**.
- [ ] **Creata e salvata la dashboard** in Kibana (così non la costruisci davanti al prof).
- [ ] File `.env` con la chiave `ANTHROPIC_API_KEY` corretta.
- [ ] **Credito API** disponibile su console.anthropic.com (controlla il saldo).
- [ ] Progetto **caricato su GitHub** (richiesto dal prof) — senza il file `.env`!
- [ ] Docker Desktop con ~8 GB di RAM assegnati.
- [ ] `docker compose down` a fine prova (per ripartire pulito il giorno dopo).

### Cosa portare
- [ ] Il portatile **carico** (+ alimentatore).
- [ ] **Hotspot dal telefono** pronto: serve internet per le chiamate a Claude. è il rischio n.1.
- [ ] La presentazione del design (schema della pipeline).
- [ ] Il link al repo GitHub.

### In aula, prima di iniziare
- [ ] Connesso a internet (wifi aula o hotspot) e verificato che funzioni.
- [ ] Avviato in anticipo: `docker compose up -d` (lascia ~1-2 min che Kibana parta).
- [ ] Aperto **http://localhost:5601** e caricata la **dashboard salvata**.
- [ ] Impostato l'intervallo tempo su **"Last 15 minutes"** + **auto-refresh** (5s).

### Durante la demo
- [ ] Spiega lo schema seguendo **un evento** lungo la pipeline (vedi `pipeline_spiegazione.md`).
- [ ] Al momento clou lancia in un secondo terminale: `docker compose restart producer`.
- [ ] Mostra gli eventi e i **suggerimenti AI** che compaiono dal vivo nella dashboard.
- [ ] Tieni pronte le **risposte alle domande tipiche** (sezione 9).

### Se qualcosa va storto
- [ ] Kibana vuoto → controlla l'intervallo tempo e l'auto-refresh.
- [ ] Nessun `dm_hint` → problema di internet/credito API: passa all'hotspot.
- [ ] Tutto bloccato → `docker compose down -v` e poi `docker compose up` (tieni questo come ultima spiaggia).

---

## 10. Risoluzione problemi (Troubleshooting)

| Sintomo | Causa probabile | Soluzione |
|---|---|---|
| Kibana non si apre su :5601 | Si avvia per ultimo, ci mette 1-2 min | Aspetta e ricarica la pagina |
| In Discover non vedo dati | L'intervallo tempo è sbagliato | Imposta "Last 15 minutes" + auto-refresh |
| Nei log di Spark: errore API key | Chiave mancante/sbagliata in `.env` | Controlla `ANTHROPIC_API_KEY`, poi `docker compose up` di nuovo |
| `dm_hint` vuoti o "(non disponibile)" | Credito API esaurito o modello errato | Verifica il credito su console.anthropic.com e il valore di `CLAUDE_MODEL` |
| Elasticsearch va in crash all'avvio | Poca RAM per Docker | Dai ≥ 8 GB a Docker (Settings → Resources) |
| Voglio ripartire pulito | Dati vecchi in Kafka/ES | `docker compose down -v` poi `docker compose up --build` |
| La partita "scorre" troppo veloce/lenta | Velocità di rigioco | Cambia `SPEED` nel servizio `producer` in `docker-compose.yml` |

Per vedere i log di un singolo servizio:
```bash
docker compose logs -f spark      # oppure: producer, logstash, kafka, kibana
```

---

## 11. Quanto costa? (API Claude)

Ogni evento è **una** chiamata a Claude con poche centinaia di token. Con il modello
`claude-haiku-4-5` una sessione di esempio (circa 25 eventi) costa **pochi centesimi**.
Per limitare i costi durante i test puoi accorciare `data/transcript.jsonl`.

---

## 12. Struttura dei file (per orientarti)

```
dnd-realtime-dm/
├── docker-compose.yml      ← avvia tutto
├── .env.example            ← copia in .env e metti la chiave Claude
├── data/
│   └── transcript.jsonl    ← la partita (eventi). Generato da Whisper o di esempio
├── transcribe/             ← Whisper: da video YouTube a transcript.jsonl
│   └── transcribe.py
├── producer/               ← simulatore: rigioca la partita verso Kafka
│   └── producer.py
├── spark/                  ← CUORE: Spark + chiamata a Claude
│   ├── Dockerfile
│   └── stream_job.py
├── logstash/pipeline/
│   └── logstash.conf       ← Kafka(dnd-enriched) → Elasticsearch
├── kibana/
│   └── setup.sh            ← crea la data view automaticamente
├── GUIDA.md                ← questo file
└── README.md               ← descrizione per GitHub
```

---

## 13. Limitare e controllare la RAM (Mac 16 GB)

Lo stack è composto da molti servizi Java/JVM (che tendono a "mangiare" RAM). Per questo
il `docker-compose.yml` è **già configurato** per stare comodamente su un MacBook con
**16 GB**: ogni servizio ha un tetto massimo di memoria (`mem_limit`) e un heap JVM ridotto.

### Quanto consuma (di picco)

| Servizio | mem_limit | Heap JVM |
|---|---|---|
| Elasticsearch | 1200 MB | 512 MB |
| Spark | 1500 MB | driver 900 MB |
| Kafka | 1024 MB | 512 MB |
| Kibana | 1024 MB | 512 MB |
| Logstash | 768 MB | 256 MB |
| Producer | 256 MB | — |
| **Totale** | **~5,7 GB** | |

Restano diversi GB liberi per macOS e il browser.

### Cosa impostare in Docker Desktop
**Settings → Resources → Memory**: assegna a Docker circa **8 GB** (non tutti i 16, così
macOS resta fluido). Premi **Apply & Restart**.

### Se hai ancora poca RAM o vuoi ridurre ulteriormente
Puoi abbassare i valori nel `docker-compose.yml` (cerca `mem_limit` e `*_JAVA_OPTS`).
Esempi sicuri:
- Elasticsearch: `ES_JAVA_OPTS=-Xms384m -Xmx384m` e `mem_limit: 900m`
- Spark: `--driver-memory 700m` e `mem_limit: 1200m`
- Kibana: lo usi solo per guardare → va bene così; se serve, `--max-old-space-size=400`.

> Non scendere troppo con Elasticsearch (sotto ~384 MB può non avviarsi).

### Controllare i consumi reali mentre gira
In un secondo terminale:
```bash
docker stats
```
Mostra RAM e CPU usate da ogni container in tempo reale. Premi `Ctrl + C` per uscire.

### Far girare meno servizi insieme (se proprio serve)
Puoi avviare lo stack a step, per non caricare tutto in una volta:
```bash
docker compose up -d kafka elasticsearch kibana   # prima l'infrastruttura
docker compose up spark logstash producer         # poi l'elaborazione
```

---

Buon lavoro e buona avventura!
