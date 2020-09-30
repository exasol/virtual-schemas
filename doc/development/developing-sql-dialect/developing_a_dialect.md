# How To Develop and Test an SQL Dialect Adapter

This article describes how you can develop and test an SQL dialect adapter based on the Virtual Schema JDBC adapter.

## Content

* [Introduction](#introduction)
* [Developing a Dialect](#developing-a-dialect)

## Introduction

Before you start writing your own SQL adapter that integrates Virtual Schemas with the SQL dialect a specific data source uses, we first need to briefly discuss how Virtual Schemas are structured in general and the JDBC adapter in particular.

[Adapters](https://www.gofpatterns.com/structural-design-patterns/structural-patterns/adapter-pattern.php) (also known as wrappers) are a piece of code that enable interaction between two previously incompatible objects by planting an adapter layer in between that serves as a translator. 
In our case a Virtual Schema adapter implements an API defined by Exasol Virtual Schemas and translates all data accesses and manages type conversions between the adapted source and the Exasol database.

In the case of the JDBC adapter there are _two_ different adapter layers in between Exasol and the source. 
The first one from Exasol's perspective is the JDBC adapter which contains the common part of the translation between Exasol and a source for which a JDBC driver exists. 
The second layer is an SQL dialect adapter, that takes care of the specialties of the source databases.

The name SQL dialect adapter is derived from the non-standard implementation parts of SQL databases which are often referred to as "dialects" of the SQL language.

As an example, PostgreSQL handles some of the data types subtly different from Exasol and the SQL dialect adapter needs to deal with those differences by implementing conversion functions.

Below you can see a layer model of the Virtual Schemas when implemented with the JDBC adapter.
 The layers in the middle &mdash; i.e. everything that deals with translating between the source and Exasol &mdash; are provided in this repository.

    .-----------------------------------------.
    |  Exasol    |          Exasol            |
    |   core     |----------------------------|
    |            |//// Virtual Schema API ////|
    |------------|----------------------------|
    |            |       JDBC  Adapter        |   Common JDBC functions
    |  In this   |----------------------------|
    | repository |///// SQL Dialect API //////|
    |            |----------------------------|
    |            |    SQL Dialect Adapter     |   Even out specifics of the source database
    |------------|----------------------------|
    |            |///////// JDBC API /////////|
    |            |----------------------------|
    |            |  PostgresSQL JDBC Driver   |   JDBC compliant access to payload and metadata
    |  External  |----------------------------|
    |            |// PostgresSQL Native API //|
    |            |----------------------------|
    |            |         PostgreSQL         |   External data source
    '-----------------------------------------'

For more information about the structure of the Virtual Schemas check the UML diagrams provided in the directory [model/diagrams](../../../model/diagrams). You either need [PlantUML](http://plantuml.com/) to render them or an editor that has PlamtUML preview built in.

## Developing a Dialect

If you want to write an SQL dialect, you need to start by implementing the dialect adapter interfaces.

### Project Structure

This repository is structured as follows. 

    virtual-schemas
      |
      |
      |-- src
      |     |
      |     |-- main
      |     |     |
      |     |     |-- java               Productive code
      |     |     |
      |     |     '-- resources          Productive resources (e.g. service loader configuration)
      |     |
      |     '-- test
      |           |
      |           |-- java               Unit and integration tests
      |           |
      |           '-- resources          Test resources
      |    ...     

### Package Structure

The Java package structure of the `virtualschema-jdbc-adapter` reflects the separation into dialect-independent and dialect-specific parts. 

    com.exasol.adapter
      |
      |-- dialects                             Common code for all dialect adapters
      |     |
      |     |-- db2                            IBM DB2-specific dialect adapter implementation
      |     |
      |     |-- exasol                         Exasol-specific dialect adapter implementation
      |     |
      |     |-- hive                           Apache-Hive-specific dialect adapter implementation
      |     |
      |     '-- ...
      |
      '-- jdbc                                 Base implementation for getting metadata from JDBC

### Interfaces

| Interface                                                                                                                                                         | Implementation                | Purpose                                                                                |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------|----------------------------------------------------------------------------------------|
| [`com.exasol.adapter.dialects.SqlDialect`](https://github.com/exasol/virtual-schema-common-jdbc/blob/master/src/main/java/com/exasol/adapter/dialects/SqlDialect.java)               | mandatory                     | Define capabilities and which kind of support the dialect has for catalogs and schemas |
| [`com.exasol.adapter.dialects.SqlDialectFactory`](https://github.com/exasol/virtual-schema-common-jdbc/blob/master/src/main/java/com/exasol/adapter/dialects/SqlDialectFactory.java) | mandatory                     | Provide a way to instantiate the SQL dialect                                           |
| [`com.exasol.adapter.jdbc.RemoteMetadataReader`](https://github.com/exasol/virtual-schema-common-jdbc/blob/master/src/main/java/com/exasol/adapter/jdbc/RemoteMetadataReader.java)   | optional depending on dialect | Read top-level metadata and find remote tables                                         |
| [`com.exasol.adapter.jdbc.TableMetadataReader`](https://github.com/exasol/virtual-schema-common-jdbc/blob/master/src/main/java/com/exasol/adapter/jdbc/TableMetadataReader.java)     | optional depending on dialect | Decide which tables should be mapped and map data on table level                       |
| [`com.exasol.adapter.jdbc.ColumnMetadataReader`](https://github.com/exasol/virtual-schema-common-jdbc/blob/master/src/main/java/com/exasol/adapter/jdbc/ColumnMetadataReader.java)   | optional depending on dialect | Map data on column level                                                               |
| [`com.exasol.adapter.dialects.QueryRewriter`](https://github.com/exasol/virtual-schema-common-jdbc/blob/master/src/main/java/com/exasol/adapter/dialects/QueryRewriter.java)         | optional depending on dialect | Rewrite the original query into a dialect-specific one                                 |

### Registering the Dialect

The Virtual Schema adapter creates an instance of an SQL dialect on demand. You can pick any dialect that is listed in the `SqlDialects` registry. Each dialect needs a factory that can create an instance of that dialect. That factory must implement the interface 'SqlDialectFactory'.

We use Java's [Service Loader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) in order to load the dialect implementation. That means you need to register the factory of your new dialect as a service on the list in [`com.exasol.adapter.dialects.SqlDialectFactory`](https://github.com/exasol/virtual-schema-common-jdbc/blob/master/src/main/java/com/exasol/adapter/dialects/SqlDialectFactory.java).

```properties
com.exasol.adapter.dialects.athena.AthenaSqlDialectFactory
com.exasol.adapter.dialects.bigquery.BigQuerySqlDialectFactory
...
com.exasol.adapter.dialects.myawesomedialect.MyAweSomeSqlDialectFactory
...
```

### Writing the Dialect and its Unit Tests

Please follow our [step-by-step guide](step_by_step_guide_to_writing_your_own_dialect.md) when you are writing the implementation classes and unit tests. 

### Adding Documentation

Please also remember to [document the SQL dialect](../../dialects).

## See Also

* [Step-by-step guide to writing your own SQL dialect](step_by_step_guide_to_writing_your_own_dialect.md)
* [Integration testing with containers](integration_testing_with_containers.md)
* [Remote debugging](../remote_debugging.md)
* [Versioning](../versioning.md)
* [Static code analysis](../static_code_analysis.md)
