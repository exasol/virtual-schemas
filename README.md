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
* Supports [ElasticSearch][elasticsearch-dialect-doc] as a source
* Supports [Apache Hive][hive-dialect-doc] (Hadoop-based data warehouse) as source
* Supports access to file-based columnar storage (e.g. [Apache Parquet files](https://parquet.apache.org/documentation/latest/))
  via [Amazon AWS Redshift Spectrum](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html), 
  [Amazon AWS Athena][athena-dialect-doc], [AWS Aurora][aurora-dialect-doc] and [Google Big Query][big-query-dialect-doc].
* Pushes down queries to the remote source (some sources)
* Supports sources with no / one / multiple catalogs or schemas
* Allows limiting metadata mapping to selected catalogs and / or schemas
* Allows redirecting log output to a remote machine
* Allows remote debugging with the Java Debugger

## Supported Data Sources

| Data Source                                       |
|---------------------------------------------------|
| [IBM DB2][db2-dialect-doc]                        |
| [Exasol][exasol-dialect-doc]                      |
| [Generic JDBC-capable RDBMS][generic-dialect-doc] |
| [MySQL][mysql-dialect-doc]                        |
| [Oracle][oracle-dialect-doc]                      |
| [PostgreSQL][postgresql-dialect-doc]              |
| [Amazon AWS Redshift][redshift-dialect-doc]       |
| [Amazon AWS Redshift Spectrum][redshift-spectrum] |
| [SAP HANA][sap-hana-dialect-doc]                  |
| [Microsoft SQLServer][sql-server-dialect-doc]     |
| [Sybase][sybase-dialect-doc]                      |
| [Teradata][teradata-dialect-doc]                  |
| [Google Big Query][big-query-dialect-doc]         |
| [AWS Aurora][aurora-dialect-doc]                  |
| [Amazon AWS Athena][athena-dialect-doc]           |
| [ElasticSearch][elasticsearch-dialect-doc]        |
| [Apache Impala][impala-dialect-doc]               |
| [Apache Hive][hive-dialect-doc]                   |
| [Apache Parquet files][apache-parquet]            |

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

Running any Virtual Schema requires a Java Runtime version 9 or later.

The runtime dependencies are specified for each dialect in their own repository.

### Maven Plug-ins

| Plug-in                                                                             | Purpose                                                | License                          |
|-------------------------------------------------------------------------------------|--------------------------------------------------------|----------------------------------|
| [Maven Assembly Plugin][maven-assembly-plugin]                                      | Creating JAR                                           | Apache License 2.0               |
| [Maven Compiler Plugin](https://maven.apache.org/plugins/maven-compiler-plugin/)    | Setting required Java version                          | Apache License 2.0               |
| [Maven Enforcer Plugin][maven-enforcer-plugin]                                      | Controlling environment constants                      | Apache License 2.0               |
| [Maven Jacoco Plugin](https://www.eclemma.org/jacoco/trunk/doc/maven.html)          | Code coverage metering                                 | Eclipse Public License 2.0       |
| [Maven Surefire Plugin](https://maven.apache.org/surefire/maven-surefire-plugin/)   | Unit testing                                           | Apache License 2.0               |
| [Sonatype OSS Index Maven Plugin][sonatype-oss-index-maven-plugin]                  | Checking Dependencies Vulnerability                    | ASL2                             |
| [Versions Maven Plugin][versions-maven-plugin]                                      | Checking if dependencies updates are available         | Apache License 2.0               |
| [Artifact Reference Checker Plugin][artifact-reference-checker-plugin]              | Check if artifact is referenced with correct version   | MIT License                      |

[artifact-reference-checker-plugin]: https://github.com/exasol/artifact-reference-checker-maven-plugin
[maven-assembly-plugin]: https://maven.apache.org/plugins/maven-assembly-plugin/
[maven-enforcer-plugin]: http://maven.apache.org/enforcer/maven-enforcer-plugin/
[sonatype-oss-index-maven-plugin]: https://sonatype.github.io/ossindex-maven/maven-plugin/
[versions-maven-plugin]: https://www.mojohaus.org/versions-maven-plugin/

[athena-dialect-doc]: https://github.com/exasol/athena-virtual-schema/blob/main/doc/user_guide/athena_user_guide.md
[aurora-dialect-doc]: doc/dialects/aurora.md
[big-query-dialect-doc]: https://github.com/exasol/bigquery-virtual-schema/blob/main/doc/user_guide/bigquery_user_guide.md
[db2-dialect-doc]: https://github.com/exasol/mysql-virtual-schema/blob/main/doc/user_guide/db2_user_guide.md
[exasol-dialect-doc]: https://github.com/exasol/exasol-virtual-schema/blob/main/doc/dialects/exasol.md
[hive-dialect-doc]: https://github.com/exasol/hive-virtual-schema/blob/main/doc/user_guide/hive_user_guide.md
[impala-dialect-doc]: https://github.com/exasol/impala-virtual-schema/blob/main/doc/dialects/impala_user_guide.md
[mysql-dialect-doc]: https://github.com/exasol/mysql-virtual-schema/blob/main/doc/user_guide/mysql_user_guide.md
[oracle-dialect-doc]: https://github.com/exasol/oracle-virtual-schema/blob/main/doc/user_guide/oracle_user_guide.md
[postgresql-dialect-doc]: https://github.com/exasol/postgresql-virtual-schema/blob/main/doc/dialects/postgresql.md
[redshift-dialect-doc]: https://github.com/exasol/redshift-virtual-schema/blob/main/doc/dialects/redshift_user_guide.md
[sap-hana-dialect-doc]: https://github.com/exasol/hana-virtual-schema/blob/main/doc/user_guide/user_guide.md
[sql-server-dialect-doc]: https://github.com/exasol/sqlserver-virtual-schema/blob/main/doc/user_guide/sqlserver_user_guide.md
[sybase-dialect-doc]: https://github.com/exasol/sybase-virtual-schema/blob/main/doc/user_guide/sybase_user_guide.md
[teradata-dialect-doc]: https://github.com/exasol/teradata-virtual-schema/blob/main/doc/dialects/teradata.md
[elasticsearch-dialect-doc]: https://github.com/exasol/elasticsearch-virtual-schema/blob/main/doc/dialects/elasticsearch_sql_user_guide.md
[redshift-spectrum]: https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html
[apache-parquet]: https://parquet.apache.org/documentation/latest/
[generic-dialect-doc]: https://github.com/exasol/generic-virtual-schema/blob/main/doc/user_guide/generic_user_guide.md

[vs-api]: https://github.com/exasol/virtual-schema-common-java/blob/master/doc/development/api/virtual_schema_api.md