# Real-Time News Analytics with Apache Kafka and Spark

A real-time big data analytics project built for CS 4371: Foundations of Big Data Management and Analytics at the University of Texas at Dallas.

## Project Overview

This project collects live text from Hacker News, streams the messages through Apache Kafka, and uses Apache Spark to process and analyze them.

The project is designed to provide two main capabilities:

1. **Named Entity Analytics:** Extract and count mentions of people, organizations, and locations using Spark Structured Streaming and spaCy. Visualize the ten most frequently mentioned entities using Elasticsearch and Kibana.
2. **News Topic Classification:** Train a text classifier using Spark MLlib and the AG News dataset to classify incoming messages into World, Sports, Business, or Sci/Tech.

## Architecture

```text
Hacker News API
      |
      v
Python Producer
      |
      v
Kafka (topic1)
      |
      +-----------------------+
      |                       |
      v                       v
Spark Named Entity       Spark MLlib
Streaming                Classification
      |                       |
      v                       v
Kafka (topic2)           Topic Counts
      |
      v
Logstash
      |
      v
Elasticsearch
      |
      v
Kibana Dashboard
```

## Technologies

- Python 3.11
- Apache Spark 4.0.1 / PySpark
- Apache Kafka 4.1.1
- Docker Desktop
- spaCy
- Spark MLlib
- Logstash, Elasticsearch, and Kibana
- Git and GitHub

## Current Progress

- [x] Set up Python, Java, Spark, and Kafka
- [x] Test Kafka producer and consumer
- [x] Implement live Hacker News story collection
- [x] Save collected messages in JSONL format
- [ ] Expand collection to at least 1,000 messages
- [ ] Implement Spark named entity streaming
- [ ] Build Elasticsearch and Kibana visualization
- [ ] Train AG News classification pipeline
- [ ] Evaluate and stream classification results

## Getting Started

### Prerequisites

- Python 3.11
- Java 17
- Docker Desktop
- Git

### Python environment

```bash
uv venv --python 3.11 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

The `requirements.txt` file will be added as the project develops.

### Start Kafka

Start the Kafka Docker container, then create the required topics `topic1` and `topic2`.

### Run the data producer

```bash
python producer/news_producer.py --limit 20 --interval 60
```

The producer collects Hacker News stories, sends messages to `topic1`, and saves them to `data/raw/hackernews.jsonl`.

## Dataset

**Live source:** Hacker News Firebase API.

**Classifier training dataset:** AG News (120,000 labeled training articles).

Raw data, trained models, and local configuration files are excluded from the public repository.

## Results

Results, evaluation metrics, and visualization screenshots will be added after implementation and testing.

## Team

CS 4371 — Group Project 1, Fall 2026.

Team contributions will be documented in the final project report.

## Academic Integrity

AI-assisted development will be disclosed in the submitted report according to the course requirements.