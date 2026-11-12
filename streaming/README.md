# Part 1: Named Entity Streaming

This Spark app finds **names of people, companies, and places** in live Hacker News posts and keeps a **running count** of how often each one is mentioned.

## Where it fits

```text
Producer  →  Kafka topic1  →  THIS APP  →  Kafka topic2  →  Logstash  →  Elasticsearch  →  Kibana chart
```

1. `producer/news_producer.py` sends Hacker News posts to **topic1**.
2. **This app** reads each post, finds the names with spaCy, and adds them to the running count.
3. Every 10 seconds, it sends the updated counts to **topic2**, where the Kibana top-10 chart picks them up.

## Example

A post says: *"Microsoft announced a new AI research center in Seattle."*

The app finds **Microsoft**, **AI**, and **Seattle** and adds 1 to each count:

```text
+---------+-----+
|entity   |count|
+---------+-----+
|Microsoft|1    |
|AI       |1    |
|Seattle  |1    |
+---------+-----+
```

When another post mentions Microsoft, its count goes up to 2.

## How to run

You need: Java 17, Python packages from `requirements.txt`, and Kafka running with `topic1` and `topic2`.

**One-time setup:**
```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

**Terminal 1: start this app** (from the repo folder):
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:$(python -c "import pyspark; print(pyspark.__version__)") \
  streaming/named_entity_stream.py
```
Wait until you see `Streaming: topic1 -> named entities -> topic2`.

**Terminal 2: start the producer:**
```bash
python producer/news_producer.py --limit 20 --interval 60
```

Every 10 seconds, Terminal 1 prints the current **top 10 names**. Press **Ctrl + C** to stop.

> **Start over from zero:** the app remembers its counts between runs. To reset them, run `rm -rf checkpoints/named_entity_stream` before starting.

## Settings (all optional)

The defaults work as-is. Change them by adding these to the `spark-submit` command:

| Setting | Default | What it does |
|---|---|---|
| `--bootstrap` | `localhost:9092` | Kafka address |
| `--input-topic` | `topic1` | Where posts come from |
| `--output-topic` | `topic2` | Where counts go |
| `--checkpoint` | `checkpoints/named_entity_stream` | Where Spark saves its progress |
| `--trigger` | `10 seconds` | How often counts update |
| `--starting-offsets` | `earliest` | `earliest` = also count posts already in topic1; `latest` = only new posts |
| `--show-top` | `10` | How many top names to print (`0` = none) |

## What counts as a "name"

| Type | Examples |
|---|---|
| People | Jensen Huang, Sam Altman |
| Companies and organizations | SpaceX, NASA, Microsoft |
| Places | Texas, Seattle, China |
| Other names | Nationalities, products, events, laws |

Numbers, dates, and money amounts are skipped. A name counts **once per post**, even if the post repeats it.

**Known limitation:** spaCy's small English model sometimes mislabels common words (for example, it may tag "AI" as an organization) or splits a name oddly.

## Output format (topic2)

Each message is JSON with the entity's **latest total count**:

```json
{"entity": "Microsoft", "count": 2, "timestamp": "2026-11-12T14:30:00.000-06:00"}
```

### For the Logstash / Kibana step

Because each message holds the newest total, Logstash should **overwrite** each entity instead of adding duplicates. In the Elasticsearch output, set `document_id => "%{entity}"`:

```conf
output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "named-entities"
    document_id => "%{entity}"
  }
}
```

In Kibana, make a **bar chart** of the top 10 values of `entity.keyword`, sized by the **maximum** of `count`.
