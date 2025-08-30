# kafkaquarius

CLI tool for Kafka's messages migration. It can be useful for the next cases:

- You need to resend Kafka's messages to the same or another topic or even broker. And especially if you need to do it with filtration.
- You need to search some messages in concrete topic and store them.

This app provides these possibilities.

The key features:

- Search messages with possibility to save them in [JSON Line][jsonl] format
- Migrate messages between topics and clusters.
- Possibility to specify time range for speed up.
- Possibility to specify number of threads (consumers) for consuming.

## Usage

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
        unix epoch time, 0 by default
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
        unix epoch time, 0 by default
  -source-broker string
        required
  -source-topic string
        required
  -threads-number int
         (default 1)
  -to-time int
        unix epoch time, now by default
```

### Filter format

The filtration mechanism is based on the [cel-go][cel-go] package which is implementation of [CEL][cel] spec with some additional extensions:
- `regex`
- `strings`
- `encoders`
- `math`
- `sets`
- `lists`

Filter example: [filter.txt](examples/filter.txt).

## Examples

### Search messages

```shell
 ./build/bin/kafkaquarius-current-linux search --consumer-group=kafkaquarius --source-broker=localhost:9092 --source-topic=test-topic --filter-file=examples/filter.txt --output-file=examples/out.jsonl --threads-number=10 --since-time=1735664400 --to-time=1738342800
```

```
Total:  1000
Found:  5
Proc:   5
Errors: 0
Time:   1m1s
```

### Migrate messages

```shell
./build/bin/kafkaquarius-current-linux migrate --consumer-group=kafkaquarius --source-broker=localhost:9092 --source-topic=test-topic --target-topic=target-test-topic --filter-file=examples/filter.txt --threads-number=10 --since-time=1735664400 --to-time=1738342800
```

```
Total:  1000
Found:  5
Proc:   5
Errors: 0
Time:   1m1s
```

[jsonl]: https://jsonlines.org/

[cel]: https://github.com/google/cel-spec

[cel-go]: https://github.com/google/cel-go
