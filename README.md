# FoundationDB Storage Adapter for JanusGraph

[JanusGraph](http://janusgraph.org) is an [Apache TinkerPop](http://tinkerpop.apache.org) enabled graph database that supports a variety of storage and indexing backends. This project adds [FoundationDB](http://foundationdb.org) to the supported list of backends. FoundationDB is a distributed, ACID key-value store.

# Features

JanusGraph, coupled with the FoundationDB storage adapter provides the following unique features:

* High availability
* ACID transactions

# Compatibility Matrix

|FDB Storage Adapter|JanusGraph|FoundationDB|
|-:|-:|-:|
|0.1.0|0.3.0|5.2.5|
|0.2.0|0.4.0|6.2.18|

# Getting started

The FoundationDB storage adapter requires a single FoundationDB instance or cluster and the FoundationDB client libraries. Downloads for server and client can be found [here](https://apple.github.io/foundationdb/downloads.html). Docker images are available [here](https://hub.docker.com/r/foundationdb/foundationdb).

## Setting up FoundationDB

Mac install instructions can be found [here](https://apple.github.io/foundationdb/getting-started-mac.html) and Linux [here](https://apple.github.io/foundationdb/getting-started-linux.html).

## Installing the adapter from a binary release
Binary releases can be found on [GitHub](http://github.com/rngcntr/janusgraph-foundationdb/releases).

This installation procedure will copy the necessary libraries, properties, and Gremlin Server configuration files into your JanusGraph installation.

1. Download the JanusGraph [release](https://github.com/JanusGraph/janusgraph/releases) that is compatible up with the FoundationDB storage adapter.
2. Download the desired FoundationDB storage adapter release.
3. Unzip the storage adapter zip file and run `./install.sh $YOUR_JANUSGRAPH_INSTALL_DIRECTORY`

Assuming you have a FoundationDB cluster up and running, you can connect from the Gremlin console by running:

`gremlin> graph = JanusGraphFactory.open('conf/janusgraph-foundationdb.properties')`

To start Gremlin Server run `gremlin-server.sh` directly or `bin/janusgraph.sh start` which will also start a local Elasticsearch instance.

## Installing from source

Follow these steps if you'd like to use the latest version built from source.
1. Clone the repository.
    `git clone http://github.com/experoinc/janusgraph-foundationdb`
2. Build the distribution package.
    `mvn package -DskipTests`
3. Follow the binary installation steps starting at step 3.

# Configuration Options

|Property|Description|Default|
|-|-|-|
|`storage.fdb.directory`|Name of the JanusGraph storage directory in FoundationDB.|`janusgraph`|
|`storage.fdb.version`|The FoundationDB client version.|`6.2.18`|
|`storage.fdb.cluster-file-path`|The location of the `fdb.cluster` file.|`/etc/foundationdb/fdb.cluster`|
|`storage.fdb.isolation-level`|The three options are `serializable`, `read_committed_no_write`, and `read_committed_with_write`.|`serializable`|

## Isolation Levels
FoundationDB provides serializable isolation under a specific set of [constraints](https://apple.github.io/foundationdb/known-limitations.html#current-limitations). Namely transactions will fail if they take longer than 5 seconds or read/write more than 10,000,000 bytes. This adapter allows the user to relax the how JanusGraph uses FoundationDB transactions and to spread a single JanusGraph transaction over more than one FoundationDB transaction. `read_committed_no_write` allows reads to be spread across more than one transasction, but will fail any writes that are attempted outside of the first transaction period. `read_committed_with_write` allows reads and writes to extend over more than one single transaction. If this option is selected, invariants may be broken and the system will behave similarily to an eventually consistent system.

* **`SERIALIZABLE`** (current state: implemented)

    This isolation level aims to provide strict serializability of transactions and thereby deliver FoundationDB's full ACID support to JanusGraph.

* **`READ_COMMITTED_NO_WRITE`** (current state: partially implemented)

    This allows read accesses which happen within the scope of the same JanusGraph transaction to spread over multiple FoundationDB transactions. **This can cause inconsistencies within the read data**. In order to avoid inconsistent states of the stored data, all writes that are attempted outside the first transaction period will cause the JanusGrpah transaction to fail.

* **`READ_COMMITTED_WITH_WRITE`** (current state: not implemented)

    This allows both read and write accesses which happen within the scope of the same JanusGraph transaction to spread over multiple FoundationDB transactions. **This can cause an inconsistent state of the database**. It is not recommended to use this setting in multi threaded or multi user environments.
