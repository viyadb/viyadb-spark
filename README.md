viyadb-spark
=============

Data processing backend for ViyaDB based on Spark.

[![Build Status](https://travis-ci.org/viyadb/viyadb-spark.png)](https://travis-ci.org/viyadb/viyadb-spark)

There are two processes defined in this project:

 * Streaming process
 * Batch process

Streaming process reads events in real-time, pre-aggregates them, and dumps loadable into ViyaDB TSV files
to a deep storage. Batch process creates historical view of data containing events from previous batch plus
events created afterwards in the streaming process.

## Prerequisites

 * [Consul](http://www.consul.io)
 
Table configuration must present in Consul under the key: `<consul prefix>/<table name>/table`

## Building

```bash
mvn package
```

## Running

```bash
spark-submit --class <jobClass> target/viyadb-spark_2.11-0.0.1-uberjar.jar \
    --consul-host "<consul host>" --consul-prefix "viyadb-cluster" \
    --table "<table name>"
```

To run streaming job use `com.github.viyadb.spark.streaming.Job` for `jobClass`, to run batch job
use `com.github.viyadb.spark.batch.Job`.

To see all available options, run:

```bash
spark-submit --class <jobClass> target/viyadb-spark_2.11-0.0.1-uberjar.jar --help
```
