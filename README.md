# Virtual Schemas 

<img alt="virtual-schemas logo" src="doc/images/virtual-schemas_128x128.png" style="float:left; padding:0px 10px 10px 10px;"/>

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

The runtime dependencies are specified for each dialect in their own repository.

## Deprecation Warning

Please use the entry point

    com.exasol.adapter.RequestDispatcher

in all your `CREATE JAVA ADAPTER SCRIPT` statements. The old entry point was removed with the Version 2.0.0 of Exasol's Virtual Schema.

## Features

* Read only access to data on remote data sources (see the Supported Data Sources below)
* Data in those sources appears as tables inside Exasol and can be queried using regular SQL statements.
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

* The Virtual Schema adapter takes about 1s to start.
That means that queries that involve tables from Virtual Schema will take at least 1s. 

## Customer Support

This is an open source project which is officially supported by Exasol. For any question, you can contact our support team.

**NOTE: For reporting an issue for a specific dialect, please do it in the dialect repository (see the Supported Data Sources above)**

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
* [Developing and Testing an SQL Dialect][developing-dialect]
* [Step-by-step guide to writing your own SQL dialect][step-be-step-developing-guide]
* [Remote Debugging][remote-debugging]

[athena-dialect-doc]: https://github.com/exasol/athena-virtual-schema/blob/main/doc/user_guide/athena_user_guide.md
[aurora-dialect-doc]: doc/dialects/aurora.md
[big-query-dialect-doc]: https://github.com/exasol/bigquery-virtual-schema/blob/main/doc/user_guide/bigquery_user_guide.md
[db2-dialect-doc]: https://github.com/exasol/db2-virtual-schema/blob/main/doc/user_guide/db2_user_guide.md
[exasol-dialect-doc]: https://github.com/exasol/exasol-virtual-schema/blob/main/doc/dialects/exasol.md
[hive-dialect-doc]: https://github.com/exasol/hive-virtual-schema/blob/main/doc/user_guide/hive_user_guide.md
[impala-dialect-doc]: https://github.com/exasol/impala-virtual-schema/blob/main/doc/user_guide/impala_user_guide.md
[mysql-dialect-doc]: https://github.com/exasol/mysql-virtual-schema/blob/main/doc/user_guide/mysql_user_guide.md
[oracle-dialect-doc]: https://github.com/exasol/oracle-virtual-schema/blob/main/doc/user_guide/oracle_user_guide.md
[postgresql-dialect-doc]: https://github.com/exasol/postgresql-virtual-schema/blob/main/doc/dialects/postgresql.md
[redshift-dialect-doc]: https://github.com/exasol/redshift-virtual-schema/blob/main/doc/user_guide/redshift_user_guide.md
[sap-hana-dialect-doc]: https://github.com/exasol/hana-virtual-schema/blob/main/doc/user_guide/user_guide.md
[sql-server-dialect-doc]: https://github.com/exasol/sqlserver-virtual-schema/blob/main/doc/user_guide/sqlserver_user_guide.md
[sybase-dialect-doc]: https://github.com/exasol/sybase-virtual-schema/blob/main/doc/user_guide/sybase_user_guide.md
[teradata-dialect-doc]: https://github.com/exasol/teradata-virtual-schema/blob/main/doc/dialects/teradata.md
[elasticsearch-dialect-doc]: https://github.com/exasol/elasticsearch-virtual-schema/blob/main/doc/dialects/elasticsearch_sql_user_guide.md
[redshift-spectrum]: https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html
[apache-parquet]: https://parquet.apache.org/documentation/latest/
[generic-dialect-doc]: https://github.com/exasol/generic-virtual-schema

[vs-api]: https://github.com/exasol/virtual-schema-common-java/blob/master/doc/development/api/virtual_schema_api.md
[developing-dialect]: https://github.com/exasol/virtual-schema-common-jdbc/blob/main/doc/development/developing_a_dialect.md
[remote-debugging]: https://github.com/exasol/virtual-schema-common-jdbc/blob/main/doc/development/remote_debugging.md
[step-be-step-developing-guide]: https://github.com/exasol/virtual-schema-common-jdbc/blob/main/doc/development/step_by_step_guide_to_writing_your_own_dialect.md