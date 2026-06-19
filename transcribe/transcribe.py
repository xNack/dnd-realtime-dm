#!/usr/bin/env python3
# =============================================================================
#  TRASCRIZIONE DEL VIDEO (si esegue UNA volta sola, sul tuo computer)
# =============================================================================
#
#  COSA FA (in parole semplici):
#  1. Scarica l'audio di un video YouTube (con il programma yt-dlp).
#  2. Lo trasforma in testo con Whisper (l'AI di OpenAI per il riconoscimento
#     vocale), tenendo anche i tempi (minuto:secondo) di ogni frase.
#  3. Salva tutto in data/transcript.jsonl: una riga per frase, così:
#        {"t": "00:02:40", "text": "Thorin attacca il goblin, tira 19"}
#
#  Questo file diventa la "sorgente dati" che poi la pipeline rigioca in diretta.
#
#  USO (puoi passare un LINK YouTube OPPURE un file già sul tuo computer):
#     python transcribe.py "https://www.youtube.com/watch?v=XXXX"
#     python transcribe.py "/Users/tuonome/Desktop/sessione.mp4"
#     python transcribe.py "sessione.mp4" --model medium --lang it
#
#  Per scaricare SOLO una parte del video YouTube (consigliato: 10-15 min):
#     python transcribe.py "https://youtu.be/XXXX" --start 00:10:00 --end 00:25:00 --model medium
# =============================================================================

import argparse
import json
import os
import subprocess
import tempfile


def formato_tempo(secondi):
    """Trasforma dei secondi (es. 160) in 'HH:MM:SS' (es. '00:02:40')."""
    s = int(secondi)
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def scarica_audio(url, cartella, start=None, end=None):
    """Scarica solo l'audio del video in formato mp3 usando yt-dlp.
    Se start/end sono indicati, scarica SOLO quella parte (es. 00:10:00 - 00:25:00)."""
    print("[1/3] Scarico l'audio dal video...")
    destinazione = os.path.join(cartella, "audio.%(ext)s")
    cmd = ["yt-dlp", "-x", "--audio-format", "mp3", "--no-playlist", "-o", destinazione]
    if start or end:
        s = start or "00:00:00"
        e = end or "99:59:59"
        print(f"     (scarico solo la parte da {s} a {e})")
        cmd += ["--download-sections", f"*{s}-{e}", "--force-keyframes-at-cuts"]
    cmd.append(url)
    subprocess.run(cmd, check=True)   # se fallisce, ferma il programma con un errore chiaro
    # yt-dlp aggiunge l'estensione: cerchiamo il file appena creato.
    for ext in ("mp3", "m4a", "webm", "wav"):
        percorso = os.path.join(cartella, f"audio.{ext}")
        if os.path.exists(percorso):
            return percorso
    raise FileNotFoundError("Non ho trovato il file audio scaricato.")


def trascrivi(audio, modello, lingua):
    """Usa Whisper per trasformare l'audio in frasi con i tempi."""
    import whisper   # importato qui, così serve solo se usi davvero questo script
    print(f"[2/3] Trascrivo con Whisper '{modello}' (può volerci qualche minuto)...")
    motore = whisper.load_model(modello)
    risultato = motore.transcribe(audio, language=lingua)
    return risultato["segments"]   # lista di frasi, ognuna con inizio e testo


def main():
    # Leggiamo le opzioni scritte dall'utente nel terminale.
    p = argparse.ArgumentParser()
    p.add_argument("sorgente", help="Link YouTube OPPURE percorso di un file video/audio sul tuo computer")
    p.add_argument("--model", default="small", help="tiny|base|small|medium|large")
    p.add_argument("--lang", default="it", help="Lingua dell'audio (default: it)")
    p.add_argument("--start", default=None, help="Inizio della parte da scaricare, es. 00:10:00 (solo per URL YouTube)")
    p.add_argument("--end", default=None, help="Fine della parte da scaricare, es. 00:25:00 (solo per URL YouTube)")
    p.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "data", "transcript.jsonl"))
    args = p.parse_args()

    # Lavoriamo in una cartella temporanea (viene cancellata da sola alla fine).
    with tempfile.TemporaryDirectory() as cartella:
        # Se "sorgente" è un file che esiste sul computer -> lo usiamo direttamente.
        # Altrimenti lo trattiamo come un link da scaricare da YouTube.
        if os.path.isfile(args.sorgente):
            print(f"[1/3] Uso il file locale: {args.sorgente}")
            audio = args.sorgente
        else:
            audio = scarica_audio(args.sorgente, cartella, args.start, args.end)
        frasi = trascrivi(audio, args.model, args.lang)

    # Scriviamo il file finale: una riga JSON per ogni frase.
    out = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    print(f"[3/3] Salvo {len(frasi)} frasi in: {out}")
    with open(out, "w", encoding="utf-8") as f:
        for frase in frasi:
            testo = frase["text"].strip()
            if testo:
                riga = {"t": formato_tempo(frase["start"]), "text": testo}
                f.write(json.dumps(riga, ensure_ascii=False) + "\n")

    print("Fatto! Ora avvia la pipeline con:  docker compose up --build")


if __name__ == "__main__":
    main()
