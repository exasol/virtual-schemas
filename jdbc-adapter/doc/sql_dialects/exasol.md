# Exasol SQL Dialect

## Supported Capabilities

The Exasol SQL dialect supports all capabilities that are supported by the virtual schema framework.

## JDBC Driver

Connecting to an Exasol database is the simplest way to start with virtual schemas.
You don't have to install any JDBC driver, because it is already installed in the Exasol database and also included in the jar of the JDBC adapter.

## Adapter Script

After uploading the adapter jar, the adapter script can be created as follows:

```sql
CREATE SCHEMA adapter;
CREATE JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;
  %jar /buckets/your-bucket-fs/your-bucket/virtualschema-jdbc-adapter-dist-1.4.0.jar;
/
```

## Creating a Virtual Schema

```sql
CREATE CONNECTION exasol_conn TO 'jdbc:exa:exasol-host:1234' USER 'user' IDENTIFIED BY 'pwd';

CREATE VIRTUAL SCHEMA virtual_exasol USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'EXASOL'
  CONNECTION_NAME = 'EXASOL_CONN'
  SCHEMA_NAME     = 'default';
```

## Using IMPORT FROM EXA Instead of IMPORT FROM JDBC

Exasol provides the faster and parallel `IMPORT FROM EXA` command for loading data from Exasol. You can tell the adapter to use this command instead of `IMPORT FROM JDBC` by setting the `IMPORT_FROM_EXA` property. In this case you have to provide the additional `EXA_CONNECTION_STRING` which is the connection string used for the internally used `IMPORT FROM EXA` command (it also supports ranges like `192.168.6.11..14:8563`). Please note, that the `CONNECTION` object must still have the JDBC connection string in `AT`, because the Adapter Script uses a JDBC connection to obtain the metadata when a schema is created or refreshed. For the internally used `IMPORT FROM EXA` statement, the address from `EXA_CONNECTION_STRING` and the user name and password from the connection will be used.

```sql
CREATE CONNECTION exasol_conn TO 'jdbc:exa:exasol-host:1234' USER 'user' IDENTIFIED BY 'pwd';

CREATE VIRTUAL SCHEMA virtual_exasol USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'EXASOL'
  CONNECTION_NAME = 'EXASOL_CONN'
  SCHEMA_NAME     = 'default'
  IMPORT_FROM_EXA = 'true'
  EXA_CONNECTION_STRING = 'exasol-host:1234';
```