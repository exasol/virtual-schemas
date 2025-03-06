# Frequently Asked Questions (FAQ) for Virtual Schema Developers

## Which Programming Languages can I use to Write a Virtual Schema?

1. Lua
2. Java
3. Python
4. R

## Which Programming Language Should I Choose for my Virtual Schema?

If you need low-latency, favor Lua, since the Lua interpreter is built directly into the Exasol engine and thus has minimal overhead.

If you can afford a second or so startup delay, first find out in which language the best libraries and APIs exists for your data source. Then, write the virtual schema adapter on top of those APIs.

API existence is also the main limiting factor for Lua implementations. While in the Python and Java world, you have libraries for nearly every imaginable data source, trying to find one for Lua often is a dead end.

In case of HTTP services you can of course use an HTTP client, but that means that you have to write the access layer in Lua yourself. 

You might also want to factor in who will maintain the implementation later. If you have a pool of Java developers, Python might not be the ideal choice (and vice versa).

## Can a Virtual Schema Write on the Remote Data Source?

No. Exasol only supports push-down of `SELECT` statements.

This design choice keeps the VS engine straightforward and highlights the absence of transaction management between Exasol and the data source.

## What Kind of Activity can I Build Into the `CREATE` Request Implementations?

Start with validating the input. Virtual Schemas are configured via properties. A typical property would be the name of the [connection database object](https://docs.exasol.com/saas/sql/create_connection.htm) which holds all the information required to find the data source and connect to it.

Steps to verify _before_ establishing the first connection:

1. Syntax and value ranges of all properties
2. Existence of database objects mentioned in properties
3. Are the objects accessible?
4. Validity of connection details

Next, connect to the data source and read its metadata, including its structure and data types.

Map the source's structure to tables and columns so that Exasol can treat them as internal tables.

Find equivalents for the source datatypes. When there is no exact match, pick the closest one and be prepared to convert data in the push-down request.

## What Kind of Activity can I Build Into the `REFRESH` Request Implementations?

Generally, the steps are very similar to a [`CREATE`](#what-kind-of-activity-can-i-build-into-the-create-request-implementations) request.

The key difference is that users expect the refresh process to run without interruptions.

That means that you should try to identify all property changes that do not affect the current setup of the Virtual Schema and handle them gracefully. That being said, there are not too many of those.

For example, renaming the Virtual Schema object will always cause an interruption.


## When is Snapshot Mode Allowed and When Recommended?

We recommend reading the main article on [Snapshot Mode](https://docs.exasol.com/saas/database_concepts/snapshot_mode.htm) on the Exasol Doc portal first.

In a nutshell, snapshot mode improves the efficiency of accessing Exasol's system tables.

> To avoid delayed execution in such a scenario, you can run your queries to the system tables in snapshot mode. Snapshot mode ignores existing transaction locks on system tables and does not create new ones. All the queries to the system tables will run in read-only mode and show you the latest object version (latest committed version).

In the context of the Virtual Schemas that is relevant for an Exasol-to-Exasol virtual schema only. Since snapshot mode is an Exasol feature, it won't help with reading 3rd-party metadata.

But, if you write a virtual schema that connects to Exasol, you should always use snapshot mode when accessing system tables.

Use a hint comment to tell Exasol that you want to run your query in snapshot mode:

```sql
/*snapshot execution*/ SELECT …
```

## How and When can I Access Exasol System Tables?

You can access Exasol system tables in any of the virtual schema request. Where possible, we recommend using an abstraction layer, such as the JDBC metadata API for Java. This reduces boilerplate code and ensures uniform access across Exasol versions.

If you directly access system tables, use [snapshot mode](#when-is-snapshot-mode-allowed-and-when-recommended).

## Can I Start Asynchronous Actions Where I Don't Care About the Response From a VS Request?

This is a question without a clear yes or no answer. While it is possible, the more important question is whether you should.

Most asynchronous request have a mechanism for delayed success notification. You can ignore this in theory, sending your request and be done.

There are very few use cases where this would be acceptable.

One possible use case might involve _optional_ telemetry. If it is really optional, you would send the telemetry in the hope it finds its way to the collecting service and accept if it does not.