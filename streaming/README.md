# Part 1: Named Entity Streaming (Spark Structured Streaming)

`named_entity_stream.py` reads the Hacker News messages that `producer/news_producer.py` sends to Kafka **topic1**. It pulls the named entities out of each message with spaCy, keeps a running count for every entity, and sends the updated counts to Kafka **topic2** every 10 seconds. Logstash reads topic2 into Elasticsearch, and Kibana shows the top 10 entities.

```text
producer/news_producer.py → topic1 → named_entity_stream.py (Spark + spaCy) → topic2 → Logstash → Elasticsearch → Kibana
```

## What counts as a named entity

spaCy's `en_core_web_sm` model labels spans of text. We keep the labels that are actual names:

| Label | Meaning | Example |
|---|---|---|
| PERSON | People | Sam Altman |
| ORG | Companies, agencies, institutions | OpenAI, NASA |
| GPE | Countries, cities, states | Texas, China |
| LOC | Other locations | Silicon Valley |
| NORP | Nationalities, religious or political groups | American |
| FAC | Buildings, airports, highways | |
| PRODUCT | Products | iPhone |
| EVENT | Named events | |
| WORK_OF_ART | Titles of books, songs, and so on | |
| LAW | Named laws | GDPR |

Numbers, dates, times, money, and percentages are left out. An entity is counted **once per message**, even if the message mentions it several times.

## Message formats

**In (topic1)**, as produced by `news_producer.py`:
```json
{"id": "41234567", "text": "OpenAI releases ...", "source": "hackernews", "timestamp": 1731440000, "collected_at": "..."}
```

**Out (topic2)**: one message per entity whose count changed in that batch:
```json
{"entity": "OpenAI", "count": 12, "timestamp": "2026-11-12T14:30:00.000-06:00"}
```
The Kafka message **key** is the entity name.

## Setup (one time)

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```
You also need Java 17 and Kafka running with `topic1` and `topic2` created.

## How to run

Run these from the **repo root**, using three Terminal windows.

**Terminal 1: start the Spark app.** The Kafka connector version must match your PySpark version, so this command looks up the version for you:
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:$(python -c "import pyspark; print(pyspark.__version__)") \
  streaming/named_entity_stream.py \
  --input-topic topic1 \
  --output-topic topic2 \
  --checkpoint checkpoints/named_entity_stream
```

**Terminal 2: start the producer:**
```bash
python producer/news_producer.py --limit 20 --interval 60
```

Every 10 seconds, Terminal 1 prints the current top 10 entities.

**Terminal 3 (optional): look at what's going into topic2:**
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic topic2 --from-beginning
```

Press **Ctrl+C** to stop.

### Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `--bootstrap` | `$KAFKA_BOOTSTRAP_SERVERS` or `localhost:9092` | Kafka address |
| `--input-topic` | `topic1` | Where the raw messages come from |
| `--output-topic` | `topic2` | Where the entity counts go |
| `--checkpoint` | `checkpoints/named_entity_stream` | Folder where Spark saves its progress |
| `--trigger` | `10 seconds` | How often the counts update |
| `--starting-offsets` | `earliest` | `earliest` also counts messages already in topic1; `latest` counts only new ones |
| `--show-top` | `10` | How many top entities to print in the terminal (`0` turns this off) |

### Starting over from zero

Spark saves its counts in the checkpoint folder, so restarting the app **continues** the old counts. To reset everything, before the real one-hour run for example, delete that folder:
```bash
rm -rf checkpoints/named_entity_stream
```

## Note for the Logstash / Kibana step

Each topic2 message contains an entity's **latest total count**, so newer messages replace older ones rather than adding to them. In the Logstash `elasticsearch` output, set `document_id => "%{entity}"`. That way each entity is a single document that is overwritten with its newest count:

```conf
input {
  kafka {
    bootstrap_servers => "kafka:29092"   # use "localhost:9092" if Logstash runs outside Docker
    topics => ["topic2"]
    codec => "json"
  }
}
output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "named-entities"
    document_id => "%{entity}"
  }
}
```

In Kibana, build a **bar chart** on the `named-entities` index: put the **Top 10 values of `entity.keyword`** on one axis, ranked by the **Maximum of `count`**, and put the count on the other axis.
