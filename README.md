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

The process can be graphically presented like this:


                                           +----------------+
                                           |                |
                                           |  Streaming Job |
                                           |                |
                                           +----------------+
                                                   |
                                                   |  writes current events
                                                   v
             +------------------+         +--------+---------+
             | Previous Period  |         | Current Period   |
             | Real-Time Events |--+      | Real-Time Events |
             +------------------+  |      +------------------+
                                   |
             +------------------+  |      +------------------+
             | Historical       |  |      | Historical       |
             | Events           |  |      | Events           |
             +------------------+  |      +------------------+      ...
                |                  |                   ^
     -----------|------------------|-------------------|----------------------------->
                |                  |                   |                Timeline
                |                  v                   |
                |              +-------------+         |
                |              |             |         |  unions previous period events
                +------------> |  Batch Job  |---------+  with all the historical events
                               |             |            that existed before
                               +-------------+


## Features

### Real-time Process

Real-time process responsibility:

 * Read data from a source (for now only Kafka support is provided as part of the code, but it can be easily extended), and parse it
 * Aggregate events by configured time window 
 * Generate data loadable by ViyaDB (TSV format)

### Batch Process

Batch process does the following:

 * Reads events that were generated by the real-time process 
 * Optionally, clean the dataset out from irrelevant events
 * Aggregate the dataset 
 * Partition the data to equal parts in terms of data size (aggregated rows number), and write these partitions back to historical storage

## Prerequisites

 * [Consul](http://www.consul.io)
 
Consul is used for storing configuration as well as for synchronizing different parts that ViyaDB cluster consists of.
For running either real-time or batch processes table configuration must present in Consul under the key: `<consul prefix>/tables/<table name>/config`.

## Building

```bash
mvn package
```

## Running

```bash
spark-submit --class <jobClass> target/viyadb-spark_2.11-0.0.1-uberjar.jar \
    --consul-host "<consul host>" --consul-prefix "viyadb" \
    --table "<table name>"
```

To run streaming job use `com.github.viyadb.spark.streaming.Job` for `jobClass`, to run batch job
use `com.github.viyadb.spark.batch.Job`.

To see all available options, run:

```bash
spark-submit --class <jobClass> target/viyadb-spark_2.11-0.0.1-uberjar.jar --help
```
