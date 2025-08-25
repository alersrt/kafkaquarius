# kafkaquarius

CLI tool for Kafka's messages migration

```shell
kafkaquarius-current-linux search \
  --consumer-group=kafkaquarius \
  --source-broker=localhost:9092 \
  --source-topic=sample-topic \
  --filter-file=examples/filter.txt \
  --output-file=examples/test.txt
```

```shell
kafkaquarius-current-linux migrate \
  --consumer-group=kafkaquarius \
  --source-broker=localhost:9092 \
  --source-topic=sample-topic \
  --target-broker=localhost:9092 \
  --target-topic=sample-topic-dest \
  --filter-file=examples/filter.txt
```

Filter example: [filter.txt](examples/filter.txt)
