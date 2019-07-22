# Exasol SQL Dialect

## Supported Capabilities

The Exasol SQL dialect supports all capabilities that are supported by the virtual schema framework.

## JDBC Driver

Connecting to an Exasol database is the simplest way to start with virtual schemas.
You don't have to install any JDBC driver, because it is already installed in the Exasol database and also included in the jar of the JDBC adapter.

## Installing the Adapter Script

Create a schema to hold the adapter script.

Upload the [Virtual Schema JDBC Adapter JAR](https://github.com/exasol/virtual-schemas/releases/download/1.19.0/virtualschema-jdbc-adapter-dist-1.19.1.jar) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
CREATE JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/bfsdefault/jars/virtualschema-jdbc-adapter-dist-1.19.1.jar;
/
```

## Defining a Named Connection

Define the connection to the other Exasol instance as shown below.

```sql
CREATE CONNECTION EXASOL_CONNECTION TO 'jdbc:exa:<host>:<port>' USER '<user>' IDENTIFIED BY '<password>';
```

## Creating a Virtual Schema

```sql
CREATE VIRTUAL SCHEMA virtual_exasol USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'EXASOL'
  CONNECTION_NAME = 'EXASOL_CONNECTION'
  SCHEMA_NAME     = 'default';
```

## Using IMPORT FROM EXA Instead of IMPORT FROM JDBC

Exasol provides the faster and parallel `IMPORT FROM EXA` command for loading data from Exasol. You can tell the adapter to use this command instead of `IMPORT FROM JDBC` by setting the `IMPORT_FROM_EXA` property. In this case you have to provide the additional `EXA_CONNECTION_STRING` which is the connection string used for the internally used `IMPORT FROM EXA` command (it also supports ranges like `192.168.6.11..14:8563`). Please note, that the `CONNECTION` object must still have the JDBC connection string in `AT`, because the Adapter Script uses a JDBC connection to obtain the metadata when a schema is created or refreshed. For the internally used `IMPORT FROM EXA` statement, the address from `EXA_CONNECTION_STRING` and the user name and password from the connection will be used.

```sql
CREATE CONNECTION EXASOL_CONNECTION TO 'jdbc:exa:<host>:<port>' USER '<user>' IDENTIFIED BY '<password>';

CREATE VIRTUAL SCHEMA virtual_exasol USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'EXASOL'
  CONNECTION_NAME = 'EXASOL_CONNECTION'
  SCHEMA_NAME     = 'default'
  IMPORT_FROM_EXA = 'true'
  EXA_CONNECTION_STRING = '<host>:<port>';
```