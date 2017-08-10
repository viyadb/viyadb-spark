viyadb-spark
=============

Data processing backend for ViyaDB based on Spark.

[![Build Status](https://travis-ci.org/viyadb/viyadb-spark.png)](https://travis-ci.org/viyadb/viyadb-spark)

## Prerequisites

 * [Consul](http://www.consul.io)
 
Table configuration must present in Consul under the key: `<consul prefix>/<table name>/table`

## Building

```bash
mvn package
```

## Running

```bash
spark-submit --class com.github.viyadb.spark.streaming.Job \
    target/viyadb-spark_2.11-0.0.1-uberjar.jar \
    --consul-host "<consul host>" \
    --consul-prefix "viyadb-cluster" \
    --table "<table name>"
```

To see all available options, run:

```bash
spark-submit --class com.github.viyadb.spark.streaming.Job \
    target/viyadb-spark_2.11-0.0.1-uberjar.jar \
    --help
```
