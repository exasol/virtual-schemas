# How To Develop and Test a SQL Dialect Adapter

This article describes how you can develop and test an SQL dialect adapter based on the Virtual Schema JDBC adapter.

## Content

* [Introduction](#introduction)
* [Developing a Dialect](#developing-a-dialect)

## Introduction

Before you start writing your own SQL adapter that integrates Virtual Schemas with the SQL dialect a specific data source uses, we first need to briefly discuss how Virtual Schemas are structured in general and the JDBC adapter in particular.

[Adapters](https://www.gofpatterns.com/structural-design-patterns/structural-patterns/adapter-pattern.php) (also known as wrappers) are a piece of code that enable interaction between two previously incompatible objects by planting an adapter layer in between that serves as a translator. In our case a Virtual Schema adapter implements an API defined by Exasol Virtual Schemas and translates all data accesses and type conversions between the adapted source and the Exasol database.

In the case of the JDBC adapter there are _two_ different adapter layers in between Exasol and the source. The first one from Exasol's perspective is the JDBC adapter which contains the common part of the translation between Exasol and a source for which a JDBC driver exists. The second layer is a SQL dialect adapter, that evens out the specialties of the source databases.

The name SQL dialect adapter is derived from the non-standard implementation parts of SQL databases which are often referred to as "dialects" of the SQL language.

As an example, PostgreSQL handles some of the data types subtly different from Exasol and the SQL dialect adapter needs to deal with those differences by implementing conversion functions.

Below you can see a layer model of the Virtual Schemas when implemented with the JDBC adapter. The layers in the middle -- i.e. everything that deals with translating between the source and Exasol -- are provided in this repository.

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

For more information about the structure of the Virtual Schemas check the UML diagrams provided in the directory [model/diagrams](../../model/diagrams). You either need [PlantUML](http://plantuml.com/) to render them or an editor that has PlamtUML preview built in.

## Developing a Dialect

If you want to write an SQL dialect, you need to start by implementing the dialect adapter interfaces.

### Project Structure

This repository contains Maven sub-projects that are structured as follows. 

    jdbc-adapter                               Parent project and integration test framework
      |
      |-- virtualschema-jdbc-adapter           The actual implementation files
      |     |
      |     |-- src
      |     |     |
      |     |     |-- main
      |     |     |     |
      |     |     |     |-- java               Productive code
      |     |     |     |
      |     |     |     '-- resources          Productive resources (e.g. service loader configuration)
      |     |     |
      |     |     '-- test
      |     |           |
      |     |           |-- java               Unit and integration tests
      |     |           |
      |     |           '-- resources          Test resources
      |    ...     
      |
      '-- virtualschema-jdbc-adapter-dist      Environment for creating the all-in-one adapter JAR

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

| Interface                                                                                                                                                 | Implementation                | Purpose                                                                                |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------|----------------------------------------------------------------------------------------|
| [`com.exasol.adapter.dialects.SqlDialect`](../../jdbc-adapter/virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/dialects/SqlDialect.java)             | mandatory                     | Define capabilities and which kind of support the dialect has for catalogs and schemas |
| [`com.exasol.adapter.jdbc.RemoteMetadataReader`](../../jdbc-adapter/virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/jdbc/RemoteMetadataReader.java) | optional depending on dialect | Read top-level metadata and find remote tables                                         |
| [`com.exasol.adapter.jdbc.TableMetadataReader`](../../jdbc-adapter/virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/jdbc/TableMetadataReader.java)   | optional depending on dialect | Decide which tables should be mapped and map data on table level                       |
| [`com.exasol.adapter.jdbc.ColumnMetadataReader`](../../jdbc-adapter/virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/jdbc/ColumnMetadataReader.java) | optional depending on dialect | Map data on column level                                                               |

### Registering the Dialect

The Virtual Schema adapter creates an instance of an SQL dialect on demand. You can pick any dialect that is listed in the `SqlDialects` registry.

To register your new dialect add it to the list in [sql_dialects.properties](../../jdbc-adapter/virtualschema-jdbc-adapter/src/main/resources/sql_dialects.properties).

```properties
com.exasol.adapter.dialects.supported=\
...
com.exasol.adapter.dialects.myawesomedialect.MyAweSomeSqlDialect
```

For tests or in case you want to exclude existing dialects in certain scenarios you can override the contents of this file 
by setting the system property `com.exasol.adapter.dialects.supported`.

### Writing the Dialect and its Unit Tests

Please follow our [step-by-step guide](step_by_step_guide_to_writing_your_own_dialect.md) when you are writing the implementation classes and unit tests. 

### Adding Documentation

Please also remember to [document the SQL dialect](../dialects).

## See Also

* [Step-by-step guide to writing your own SQL dialect](step_by_step_guide_to_writing_your_own_dialect.md)
* [Virtual Schema API Documentation](virtual_schema_api.md)
* [Integration testing with containers](integration_testing_with_containers.md)
* [Remote debugging](remote_debugging.md)
* [Versioning](versioning.md)
* [Static code analysis](static_code_analysis.md)