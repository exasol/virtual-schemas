# Virtual Schemas 

[![Build Status](https://travis-ci.org/exasol/virtual-schemas.svg?branch=master)](https://travis-ci.org/exasol/virtual-schemas)

SonarCloud results:

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtualschema-jdbc-adapter-main&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtualschema-jdbc-adapter-main)

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtualschema-jdbc-adapter-main&metric=security_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtualschema-jdbc-adapter-main)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtualschema-jdbc-adapter-main&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtualschema-jdbc-adapter-main)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtualschema-jdbc-adapter-main&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtualschema-jdbc-adapter-main)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtualschema-jdbc-adapter-main&metric=sqale_index)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtualschema-jdbc-adapter-main)

[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtualschema-jdbc-adapter-main&metric=code_smells)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtualschema-jdbc-adapter-main)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtualschema-jdbc-adapter-main&metric=coverage)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtualschema-jdbc-adapter-main)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtualschema-jdbc-adapter-main&metric=duplicated_lines_density)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtualschema-jdbc-adapter-main)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtualschema-jdbc-adapter-main&metric=ncloc)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtualschema-jdbc-adapter-main)

# Overview

Exasol **Virtual Schemas** are an abstraction layer that makes external data sources accessible in our data analytics platform through regular SQL commands. The contents of the external data sources are mapped to virtual tables which look like and can be queried as any regular Exasol table.

Regardless of whether that source is a relational database like ours, or it's like the structure of GitHub repositories, the interface that users see is always the same.

This means an user familiar with SQL will immediately feel at home when accessing remote data through Virtual Schemas.

## Deprecation Warning

Please use the entry point

    com.exasol.adapter.RequestDispatcher

in all your `CREATE JAVA ADAPTER SCRIPT` statements. The old entry point was removed with the Version 2.0.0 of Exasol's Virtual Schema.

## Features

* Read access to data on remote data sources
* Data in those sources appears as tables inside Exasol and can be queried using regular SQL statements.
* Supports the following relational databases as sources: [IBM DB2](https://www.ibm.com/db2/), [Exasol](https://www.exasol.com), Generic [JDBC](https://www.oracle.com/technetwork/java/overview-141217.html)-capable RDBMS, [Oracle](https://www.oracle.com), [PostgreSQL](https://postgresql.org/), [Amazon AWS Redshift](https://aws.amazon.com/redshift/), [Microsoft SQLServer](https://www.microsoft.com/en-us/sql-server/), [Sybase](http://www.sybase.com/), [Teradata](https://www.teradata.com/)
* Supports [Apache Impala](http://impala.apache.org/) (Hadoop-based analytical database) as a source
* Supports [Apache Hive](https://hive.apache.org/) (Hadoop-based data warehouse) as source
* Supports access to file-based columnar storage (e.g. [Tableau files](https://onlinehelp.tableau.com/current/pro/desktop/en-us/environ_filesandfolders.html))
  via [Amazon AWS Redshift Spectrum](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html) and [Amazon AWS Athena](https://aws.amazon.com/athena/)
* Pushes down queries to the remote source (some sources)
* Supports sources with no / one / multiple catalogs or schemas
* Allows limiting metadata mapping to selected catalogs and / or schemas
* Allows redirecting log output to a remote machine
* Allows remote debugging with the Java Debugger

## Customer Support

This is an open source project which is officially supported by Exasol. For any question, you can contact our support team.

# Table of Contents

## Information for Users

* [User Guide](doc/user-guide/user_guide.md)
* [Deployment Guide](doc/user-guide/deploying_the_virtual_schema_adapter.md) 

Supported dialects:

1. [Athena](doc/dialects/athena.md)
1. [Aurora](doc/dialects/aurora.md)
1. [Big Query](doc/dialects/bigquery.md)
1. [DB2](doc/dialects/db2.md)
1. [Exasol](doc/dialects/exasol.md)
1. [Hive](doc/dialects/hive.md)
1. [Impala](doc/dialects/impala.md)
1. [Oracle](doc/dialects/oracle.md)
1. [PostgreSQL](doc/dialects/postgresql.md)
1. [Redshift](doc/dialects/redshift.md)
1. [SAP HANA](.../dialects/saphana.md)
1. [SQL Server](doc/dialects/sql_server.md)
1. [Sybase ASE](doc/dialects/sybase.md)
1. [Teradata](doc/dialects/teradata.md)
1. Generic

## Information for Developers 

* [Virtual Schema API Documentation](doc/development/virtual_schema_api.md)
* [Developing and Testing an SQL Dialect](doc/development/developing_a_dialect.md)
* [Step-by-step guide to writing your own SQL dialect](doc/development/step_by_step_guide_to_writing_your_own_dialect.md)
* [Remote Logging](doc/development/remote_logging.md)
* [Remote Debugging](doc/development/remote_debugging.md)
* [Troubleshooting](doc/development/troubleshooting.md)
* [Versioning](doc/development/versioning.md)

## Dependencies

### Run Time Dependencies

Running the Virtual Schema requires a Java Runtime version 8 or later.

| Dependency                                                                          | Purpose                                                | License                       |
|-------------------------------------------------------------------------------------|--------------------------------------------------------|-------------------------------|
| [JSON-P](https://javaee.github.io/jsonp/)                                           | JSON Processing                                        | CDDL-1.0                      |
| [Exasol Script API](https://docs.exasol.com/database_concepts/udf_scripts.htm)      | Accessing Exasol features                              | MIT License                   |
| JDBC driver(s), depending on data source                                            | Connecting to the data source                          | Check driver documentation    |

### Build Time Dependencies

| Dependency                                                                          | Purpose                                                | License                       |
|-------------------------------------------------------------------------------------|--------------------------------------------------------|-------------------------------|
| [Apache Derby](https://db.apache.org/derby/)                                        | Pure-Java embedded database                            | Apache License 2.0            |
| [Apache HTTP Components](http://hc.apache.org/)                                     | HTTP communication                                     | Apache License 2.0            |
| [Apache Maven](https://maven.apache.org/)                                           | Build tool                                             | Apache License 2.0            |
| [Equals Verifier](https://jqno.nl/equalsverifier/)                                  | Testing `equals(...)` and `hashCode()` contracts       | Apache License 2.0            |
| [Exasol Virtual Schema Common](https://github.com/exasol/virtual-schema-common-java)| Common module of Exasol Virtual Schemas adapters       | MIT License                   |
| [Exec Maven Plugin](https://www.mojohaus.org/exec-maven-plugin/)                    | Helps execute system and Java programs.                | Apache License 2.0            |
| [Java Hamcrest](http://hamcrest.org/JavaHamcrest/)                                  | Checking for conditions in code via matchers           | BSD License                   |
| [JSONassert](http://jsonassert.skyscreamer.org/)                                    | Compare JSON documents for semantic equality           | Apache License 2.0            |
| [JUnit](https://junit.org/junit5)                                                   | Unit testing framework                                 | Eclipse Public License 1.0    |
| [J5SE](https://github.com/itsallcode/junit5-system-extensions)                      | JUnit5 extensions to test Java System.x functions      | Eclipse Public License 2.0    |
| [Mockito](http://site.mockito.org/)                                                 | Mocking framework                                      | MIT License                   |
| [SnakeYaml](https://bitbucket.org/asomov/snakeyaml/src/default/)                    | YAML parsing                                           | Apache License 2.0            |
