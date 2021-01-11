# Virtual Schemas 

<img alt="virtual-schemas logo" src="doc/images/virtual-schemas_128x128.png" style="float:left; padding:0px 10px 10px 10px;"/>

[![Build Status](https://travis-ci.com/exasol/virtual-schemas.svg?branch=master)](https://travis-ci.com/exasol/virtual-schemas)

SonarCloud results:

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtual-schema-jdbc-adapter&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtual-schema-jdbc-adapter)

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtual-schema-jdbc-adapter&metric=security_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtual-schema-jdbc-adapter)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtual-schema-jdbc-adapter&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtual-schema-jdbc-adapter)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtual-schema-jdbc-adapter&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtual-schema-jdbc-adapter)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtual-schema-jdbc-adapter&metric=sqale_index)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtual-schema-jdbc-adapter)

[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtual-schema-jdbc-adapter&metric=code_smells)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtual-schema-jdbc-adapter)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtual-schema-jdbc-adapter&metric=coverage)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtual-schema-jdbc-adapter)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtual-schema-jdbc-adapter&metric=duplicated_lines_density)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtual-schema-jdbc-adapter)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Avirtual-schema-jdbc-adapter&metric=ncloc)](https://sonarcloud.io/dashboard?id=com.exasol%3Avirtual-schema-jdbc-adapter)

# Overview

Exasol **Virtual Schemas** are an abstraction layer that makes external data sources accessible in our data analytics platform through regular SQL commands. The contents of the external data sources are mapped to virtual tables which look like and can be queried as any regular Exasol table.

Regardless of whether that source is a relational database like ours, or it's like the structure of GitHub repositories, the interface that users see is always the same.

This means an user familiar with SQL will immediately feel at home when accessing remote data through Virtual Schemas.

Virtual Schemas can be also described in known terms as `External Tables` or `Foreign Data Wrapper (FDW)`.


## Version Requirements

Virtual Schema  | Required Java Version | Lifecycle
----------------|-----------------------|--------------------------------
4.x.x           |                    11 | supported, active development
3.x.x           |                    11 | supported
2.x.x           |                     9 | discontinued
1.x.x           |                     8 | discontinued

Exasol Version  | Java Version Installed by Default in Language Container
----------------|--------------------------------------------------------
7.0             | 11
6.2             | 11
6.1             | 9
6.0             | 8

Note that you can update Exasol 6.0.x and 6.1.x with script language containers version 2019-11-26 or newer in order to get Java 11 support. We recommend the updating Exasol installation to at least 6.2.3 or newer for best results.

Please do not try to install language containers with lower Java versions on newer Exasol installations. This is untested.

Please contact the [Exasol Support Team](https://www.exasol.com/portal/display/EXA/Support+Dashboard) if you need help upgrading the language container.

## Deprecation Warning

Please use the entry point

    com.exasol.adapter.RequestDispatcher

in all your `CREATE JAVA ADAPTER SCRIPT` statements. The old entry point was removed with the Version 2.0.0 of Exasol's Virtual Schema.

## Features

* Read access to data on remote data sources
* Data in those sources appears as tables inside Exasol and can be queried using regular SQL statements.
* Supports the following relational databases as sources: [IBM DB2][db2-dialect-doc], [Exasol][exasol-dialect-doc], 
 Generic JDBC-capable RDBMS, [MySQL][mysql-dialect-doc], [Oracle][oracle-dialect-doc], [PostgreSQL][postgresql-dialect-doc], 
 [Amazon AWS Redshift][redshift-dialect-doc], [SAP HANA][sap-hana-dialect-doc], [Microsoft SQLServer][sql-server-dialect-doc],
 [Sybase][sybase-dialect-doc], [Teradata][teradata-dialect-doc].
* Supports [Apache Impala][impala-dialect-doc] (Hadoop-based analytical database) as a source
* Supports [Apache Hive][hive-dialect-doc] (Hadoop-based data warehouse) as source
* Supports access to file-based columnar storage (e.g. [Apache Parquet files](https://parquet.apache.org/documentation/latest/))
  via [Amazon AWS Redshift Spectrum](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html), 
  [Amazon AWS Athena][athena-dialect-doc], [AWS Aurora][aurora-dialect-doc] and [Google Big Query][big-query-dialect-doc].
* Pushes down queries to the remote source (some sources)
* Supports sources with no / one / multiple catalogs or schemas
* Allows limiting metadata mapping to selected catalogs and / or schemas
* Allows redirecting log output to a remote machine
* Allows remote debugging with the Java Debugger

## Limitations

* Supports only read access to the external data
* The Virtual Schema adapter takes about 1s to start.
That means that queries that involve tables from Virtual Schema will take at least 1s. 

## Customer Support

This is an open source project which is officially supported by Exasol. For any question, you can contact our support team.

# Table of Contents

## Information for Users

* [List of Supported Dialect](doc/user-guide/dialects.md)
* [User Guide](https://docs.exasol.com/database_concepts/virtual_schemas.htm)
* [Virtual Schema's Properties Reference](https://docs.exasol.com/database_concepts/virtual_schema/adapter_properties.htm)
* [Remote Logging](https://docs.exasol.com/database_concepts/virtual_schema/logging.htm)
* [FAQ](doc/user-guide/faq.md)

Additional resources:

* [Troubleshooting](doc/user-guide/troubleshooting.md)
* [Changelog](doc/changes/changelog.md)
* [Virtual Schema Privileges](https://docs.exasol.com/database_concepts/virtual_schema/virtual_schema_privilege.htm)

## Information for Developers 

* [Virtual Schema API Documentation][vs-api]
* [Developing and Testing an SQL Dialect](doc/development/developing-sql-dialect/developing_a_dialect.md)
* [Step-by-step guide to writing your own SQL dialect](doc/development/developing-sql-dialect/step_by_step_guide_to_writing_your_own_dialect.md)
* [Remote Debugging](doc/development/remote_debugging.md)
* [Troubleshooting](doc/development/troubleshooting.md)
* [Versioning](doc/development/versioning.md)

## Dependencies

### Run Time Dependencies

Running the Virtual Schema requires a Java Runtime version 9 or later.

| Dependency                                                                          | Purpose                                                | License                          |
|-------------------------------------------------------------------------------------|--------------------------------------------------------|----------------------------------|
| [JSON-P](https://javaee.github.io/jsonp/)                                           | JSON Processing                                        | CDDL-1.0                         |
| [Exasol Script API](https://docs.exasol.com/database_concepts/udf_scripts.htm)      | Accessing Exasol features                              | MIT License                      |
| [Exasol Virtual Schema JDBC](https://github.com/exasol/virtual-schema-common-jdbc)  | Common JDBC functions for Virtual Schemas adapters     | MIT License                      |
| JDBC driver(s), depending on data source                                            | Connecting to the data source                          | Check driver documentation       |

### Test Dependencies

| Dependency                                                                          | Purpose                                                | License                          |
|-------------------------------------------------------------------------------------|--------------------------------------------------------|----------------------------------|
| [Apache Maven](https://maven.apache.org/)                                           | Build tool                                             | Apache License 2.0               |
| [Apache Trift][apache-trift]                                                        | Need for Hive integration test                         | Apache License 2.0               |
| [Exasol JDBC Driver][exasol-jdbc-driver]                                            | JDBC driver for Exasol database                        | MIT License                      |
| [Exasol Testcontainers][exasol-testcontainers]                                      | Exasol extension for the Testcontainers framework      | MIT License                      |
| [HBase server][hbase-server]                                                        | The Hadoop database                                    | Apache License 2.0               |
| [Hive JDBC Driver][hive-jdbc-driver]                                                | JDBC driver for Hive database                          | Apache License 2.0               |
| [Java Hamcrest](http://hamcrest.org/JavaHamcrest/)                                  | Checking for conditions in code via matchers           | BSD License                      |
| [JUnit](https://junit.org/junit5)                                                   | Unit testing framework                                 | Eclipse Public License 1.0       |
| [Mockito](http://site.mockito.org/)                                                 | Mocking framework                                      | MIT License                      |
| [Microsoft JDBC Driver for SQL Server][sql-server-jdbc-driver]                      | JDBC driver for SQL Server database                    | MIT License                      |
| [Oracle JDBC Driver][oracle-jdbc-driver]                                            | JDBC driver for Oracle database                        | Oracle Technology Network License|
| [Testcontainers](https://www.testcontainers.org/)                                   | Container-based integration tests                      | MIT License                      |
| [Test Database Builder][test-bd-builder]                                            | Fluent database interfaces for testing                 | MIT License                      |

### Maven Plug-ins

| Plug-in                                                                             | Purpose                                                | License                          |
|-------------------------------------------------------------------------------------|--------------------------------------------------------|----------------------------------|
| [Maven Assembly Plugin][maven-assembly-plugin]                                      | Creating JAR                                           | Apache License 2.0               |
| [Maven Compiler Plugin](https://maven.apache.org/plugins/maven-compiler-plugin/)    | Setting required Java version                          | Apache License 2.0               |
| [Maven Enforcer Plugin][maven-enforcer-plugin]                                      | Controlling environment constants                      | Apache License 2.0               |
| [Maven Failsafe Plugin](https://maven.apache.org/surefire/maven-surefire-plugin/)   | Integration testing                                    | Apache License 2.0               |
| [Maven Jacoco Plugin](https://www.eclemma.org/jacoco/trunk/doc/maven.html)          | Code coverage metering                                 | Eclipse Public License 2.0       |
| [Maven Surefire Plugin](https://maven.apache.org/surefire/maven-surefire-plugin/)   | Unit testing                                           | Apache License 2.0               |
| [Sonatype OSS Index Maven Plugin][sonatype-oss-index-maven-plugin]                  | Checking Dependencies Vulnerability                    | ASL2                             |
| [Versions Maven Plugin][versions-maven-plugin]                                      | Checking if dependencies updates are available         | Apache License 2.0               |
| [Artifact Reference Checker Plugin][artifact-reference-checker-plugin]              | Check if artifact is referenced with correct version   | MIT License                      |

[artifact-reference-checker-plugin]: https://github.com/exasol/artifact-reference-checker-maven-plugin
[maven-assembly-plugin]: https://maven.apache.org/plugins/maven-assembly-plugin/
[apache-trift]: http://thrift.apache.org/
[exasol-jdbc-driver]: https://www.exasol.com/portal/display/DOWNLOAD/Exasol+Download+Section
[exasol-testcontainers]: https://github.com/exasol/exasol-testcontainers
[hbase-server]: http://hbase.apache.org/
[hive-jdbc-driver]: https://github.com/apache/hive/tree/master/jdbc/src/java/org/apache/hive/jdbc
[maven-enforcer-plugin]: http://maven.apache.org/enforcer/maven-enforcer-plugin/
[oracle-jdbc-driver]: https://www.oracle.com/database/technologies/appdev/jdbc.html
[sql-server-jdbc-driver]: https://github.com/microsoft/mssql-jdbc
[sonatype-oss-index-maven-plugin]: https://sonatype.github.io/ossindex-maven/maven-plugin/
[test-bd-builder]: https://github.com/exasol/test-db-builder-java
[versions-maven-plugin]: https://www.mojohaus.org/versions-maven-plugin/

[athena-dialect-doc]: doc/dialects/athena.md
[aurora-dialect-doc]: doc/dialects/aurora.md
[big-query-dialect-doc]: doc/dialects/bigquery.md
[db2-dialect-doc]: doc/dialects/db2.md
[exasol-dialect-doc]: https://github.com/exasol/exasol-virtual-schema/blob/master/doc/dialects/exasol.md
[hive-dialect-doc]: doc/dialects/hive.md
[impala-dialect-doc]: doc/dialects/impala.md
[mysql-dialect-doc]: https://github.com/exasol/mysql-virtual-schema/blob/main/doc/user_guide/mysql_user_guide.md
[oracle-dialect-doc]: doc/dialects/oracle.md
[postgresql-dialect-doc]: https://github.com/exasol/postgresql-virtual-schema/blob/main/doc/dialects/postgresql.md
[redshift-dialect-doc]: doc/dialects/redshift.md
[sap-hana-dialect-doc]: doc/dialects/saphana.md
[sql-server-dialect-doc]: doc/dialects/sql_server.md
[sybase-dialect-doc]: doc/dialects/sybase.md
[teradata-dialect-doc]: doc/dialects/teradata.md

[vs-api]: https://github.com/exasol/virtual-schema-common-java/blob/master/doc/development/api/virtual_schema_api.md