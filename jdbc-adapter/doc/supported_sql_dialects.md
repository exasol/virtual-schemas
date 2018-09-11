# Supported Dialects

The purpose of this page is to provide detailed instructions for each of the supported dialects on how to get started. Typical questions are
* Which **JDBC driver** is used, which files have to be uploaded and included when creating the adapter script.
* How does the **CREATE VIRTUAL SCHEMA** statement look like, i.e. which properties are required.
* **Data source specific notes**, like authentication with Kerberos, supported capabilities or things to consider regarding the data type mapping.

As an entry point we recommend you follow the [step-by-step deployment guide](deploying_the_virtual_schema_adapter.md) which will link to this page whenever needed.

## Before you Start

Please note that the syntax for creating adapter scripts is not recognized by all SQL clients. [DBeaver](https://dbeaver.io/) for example. If you encounter such a problem, try a different client.

## List of Supported Dialects

1. [EXASOL](sql_dialects/exasol.md)
1. [Hive](sql_dialects/hive.md)
1. [Impala](sql_dialects/impala.md)
1. [DB2](sql_dialects/db2.md)
1. [Oracle](sql_dialects/oracle.md)
1. [Teradata](sql_dialects/teradata.md)
1. [Redshift](sql_dialects/redshift.md)
1. [SQL Server](sql_dialects/sql_server.md)
1. [Sybase ASE](sql_dialects/sybase.md)
1. [PostgresSQL](sql_dialects/postgresql.md)
1. Generic