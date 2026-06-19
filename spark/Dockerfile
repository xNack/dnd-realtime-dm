# Immagine UFFICIALE di Apache Spark 3.5 con Python (PySpark).
# Aggiungiamo i jar del connettore Kafka per Spark Structured Streaming, così il
# primo avvio NON deve scaricarli da internet (più veloce e affidabile).
# La chiamata a Claude avviene via HTTP standard (modulo urllib), quindi NON
# serve installare alcuna libreria Python aggiuntiva.
FROM spark:3.5.1-python3

USER root

# --- Connettore Kafka per Spark (Scala 2.12, Spark 3.5.1) + dipendenze ---
# ADD scarica i file dall'URL direttamente dentro l'immagine (non serve curl).
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# Rendiamo i jar leggibili e prepariamo la cartella di lavoro
RUN chmod 644 /opt/spark/jars/*.jar
WORKDIR /app
