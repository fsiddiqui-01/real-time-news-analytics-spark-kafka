# Kibana / Logstash / Elasticsearch part

Run from each tool's own install folder. Replace /path/to with your own paths.

1. Elasticsearch: add `xpack.security.enabled: false` to config/elasticsearch.yml, then `bin/elasticsearch`
2. Kibana: set `elasticsearch.hosts: ["http://localhost:9200"]` in config/kibana.yml, then `bin/kibana`
3. Logstash: `bin/logstash -f /path/to/logstash-topic2.conf`
4. In Kibana, create a data view for `entity-counts` (no time field), then a bar chart:
   horizontal axis = top 10 values of `entity.keyword` ranked by Maximum of `count`, vertical axis = Maximum of `count`.

Expected topic2 message format: {"entity": "...", "count": N, "timestamp": "..."}
Screenshots: top10_15min.png, top10_30min.png, top10_45min.png, top10_60min.png
