# Immagine Python leggera ("slim") come base per il producer.
FROM python:3.11-slim

# Cartella di lavoro dentro il container.
WORKDIR /app

# Unica dipendenza: la libreria per inviare messaggi a Kafka.
# --no-cache-dir mantiene l'immagine più piccola.
RUN pip install --no-cache-dir "kafka-python>=2.0.2"

# Copiamo lo script del producer dentro l'immagine.
COPY producer.py /app/producer.py

# Comando di avvio: esegue il producer.
# -u = output "non bufferizzato", così i print compaiono subito nei log di Docker.
CMD ["python", "-u", "producer.py"]
