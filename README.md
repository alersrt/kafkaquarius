# kafkaquarius

CLI tool for Kafka's messages migration

```
Usage of kafkaquarius-current-linux:
migrate
search
```

```
kafkaquarius-current-linux migrate
Usage of migrate:
  -consumer-group string
        required
  -filter-file string
        required
  -since-time int
        unix epoch time
  -source-broker string
        required
  -source-topic string
        required
  -target-broker string
        --source-broker is used if empty
  -target-topic string
        --source-topic is used if empty
  -threads-number int
         (default 1)
  -to-time int
        unix epoch time, now by default
```

```
kafkaquarius-current-linux search
Usage of search:
  -consumer-group string
        required
  -filter-file string
        required
  -output-file string
        
  -since-time int
        unix epoch time
  -source-broker string
        required
  -source-topic string
        required
  -threads-number int
         (default 1)
  -to-time int
        unix epoch time, now by default
```

Filter example: [filter.txt](examples/filter.txt)
