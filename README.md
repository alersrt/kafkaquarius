# kafkaquarius

CLI tool for Kafka's messages migration. It can be useful for the next cases:

-   You need to resend Kafka's messages to the same or another topic or even broker. And especially if you need to do it with filtration.
-   You need to search some messages in concrete topic and store them.
-   You need to send messages from stored backup or build them from provided data with template.

This app provides these possibilities.

The key features:

-   Search messages with possibility to save them in [JSON Line][jsonl] format
-   Migrate messages between topics and clusters.
-   Possibility to specify time range for speed up.
-   Possibility to specify number of threads (consumers) for consuming.
-   Backup messages and restore them from this backup.

## Filter and transform format

The filtration mechanism is based on the [cel-go][cel-go] package which is implementation of [CEL][cel] spec with some additional extensions:

-   [`optional_types`](https://pkg.go.dev/github.com/google/cel-go/cel#OptionalTypes)
-   [`regex`](https://pkg.go.dev/github.com/google/cel-go/ext#Regex)
-   [`bingings`](https://pkg.go.dev/github.com/google/cel-go/ext#Bindings)
-   [`strings`](https://pkg.go.dev/github.com/google/cel-go/ext#Strings)
-   [`encoders`](https://pkg.go.dev/github.com/google/cel-go/ext#Encoders)
-   [`math`](https://pkg.go.dev/github.com/google/cel-go/ext#Math)
-   [`sets`](https://pkg.go.dev/github.com/google/cel-go/ext#Sets)
-   [`lists`](https://pkg.go.dev/github.com/google/cel-go/ext#Lists)
-   [`two_var_comprehensions`](https://pkg.go.dev/github.com/google/cel-go/ext#TwoVarComprehensions)

The CEL transform can be also useful for the building objects from the scratch, for example to build kafka messages for producing or supplying backup and restoring.

Examples:

-   filter example: [filter.txt](examples/filter.txt)
-   migration transform: [migration_transform.txt](examples/migration_transform.txt)
-   backup transform: [store_transform.txt](examples/store_transform.txt)
-   restore transform: [restore_transform.txt](examples/restore_transform.txt)
-   build elements from scratch: [data.jsonl](examples/data.jsonl) and [build.txt](examples/build.txt)

Entrypoint source is `self` variable.

Kafka message format description:

```json
{
    "TopicPartition": {
        "Topic": "<topic name>",
        "Partition": 10,
        "Offset": 0,
        "Metadata": null,
        "Error": null,
        "LeaderEpoch": 0
    },
    "Value": "<bytes>",
    "Key": "<bytes>",
    "Timestamp": "2024-10-30T05:00:05.734+07:00",
    "TimestampType": 1,
    "Opaque": null,
    "Headers": [
        {
            "Key": "<header name>",
            "Value": "<bytes>"
        }
    ],
    "LeaderEpoch": 0
}
```

Some additional functions for cel:

-   `uuid()` - generates random uuid (v4), also available `uuid(b'...'` and `uuid("...")`
-   `uuid.v[1,4,6,7]()` - generates uuid of specified version (`uuid.v1()`, `uuid.v4`, `uuid.v6` or `uuid.v7`)
-   `marschal(any)` - marshal provided data to bytes
-   `unmarshal([]byte)` - unmarshal bytes to data
-   `<timestamp>.unix()` - get unix time in seconds
-   `<timestamp>.unixMilli()` - get unix time in milliseconds
-   `<timestamp>.unixSubmilli()` - get unix time in seconds with milliseconds as fractional part

## Pay attention!

By default, you can't to specify source as destination without direct allowance via special flag `--leeroy=true`.

## Usage

```
Usage of kafkaquarius-current-linux:
migrate
search
produce
```

```
Usage of migrate:
  -consumer-group string
        required
  -filter-file string
        required, CEL filter
  -leeroy
        fatuity and courage
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
  -template-file string
        optional, CEL transform
  -threads-number int
         (default 1)
  -to-time int
        unix epoch time, infinity by default
```

```
Usage of search:
  -consumer-group string
        required
  -filter-file string
        required, CEL filter
  -output-file string

  -since-time int
        unix epoch time, 0 by default
  -source-broker string
        required
  -source-topic string
        required
  -template-file string
        optional, CEL transform
  -threads-number int
         (default 1)
  -to-time int
        unix epoch time, infinity by default
```

```
Usage of produce:
  -filter-file string
        optional, CEL filter
  -source-file string
        required, JSONL
  -target-broker string
        required
  -target-topic string
        required
  -template-file string
        required, CEL transform
```

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

### Produce messages

```shell
./build/bin/kafkaquarius-current-linux produce --target-broker=localhost:9094 --target-topic=some-target-topic --template-file=examples/restore_transform.txt --source-file=examples/out.jsonl
```

[jsonl]: https://jsonlines.org/
[cel]: https://github.com/google/cel-spec
[cel-go]: https://github.com/google/cel-go
