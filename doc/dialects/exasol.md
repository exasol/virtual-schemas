# Exasol SQL Dialect

Connecting to an Exasol database is the simplest way to get started with Virtual Schemas.
You don't have to install any JDBC driver, because it is already installed in the Exasol database and also included in the JAR of the JDBC adapter.

## Installing the Adapter Script

Upload the latest available release of [Virtual Schema JDBC Adapter](https://github.com/exasol/virtual-schemas/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
CREATE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-2.2.0.jar;
/
```

## Using IMPORT FROM EXA Instead of IMPORT FROM JDBC

Exasol provides the faster and parallel `IMPORT FROM EXA` command for loading data from Exasol. You can tell the adapter to use this command instead of `IMPORT FROM JDBC` by setting the `IMPORT_FROM_EXA` property. 
In this case you have to provide the additional `EXA_CONNECTION_STRING` which is the connection string used for the internally used `IMPORT FROM EXA` command (it also supports ranges like `192.168.6.11..14:8563`). Please note, that the `CONNECTION` object must still have the JDBC connection string in `AT`, because the Adapter Script uses a JDBC connection to obtain the metadata when a schema is created or refreshed. 
For the internally used `IMPORT FROM EXA` statement, the address from `EXA_CONNECTION_STRING` and the user name and password from the connection will be used.

### Defining a Named Connection

```sql
CREATE CONNECTION EXASOL_CONNECTION 
TO 'jdbc:exa:<host>:<port>' 
USER '<user>' 
IDENTIFIED BY '<password>';
```

### Creating a Virtual Schema

```sql
CREATE VIRTUAL SCHEMA VIRTUAL_EXASOL 
    USING ADAPTER.JDBC_ADAPTER WITH
    SQL_DIALECT     = 'EXASOL'
    CONNECTION_NAME = 'EXASOL_CONNECTION'
    SCHEMA_NAME     = '<schema name>'
    IMPORT_FROM_EXA = 'true'
    EXA_CONNECTION_STRING = '<host>:<port>';
```

## Using IMPORT FROM JDBC

### Defining a Named Connection

Define the connection to the other Exasol cluster as shown below.

```sql
CREATE CONNECTION EXASOL_CONNECTION 
TO 'jdbc:exa:<host>:<port>' 
USER '<user>' 
IDENTIFIED BY '<password>';
```

### Creating a Virtual Schema

```sql
CREATE VIRTUAL SCHEMA <virtual schema name> 
    USING ADAPTER.JDBC_ADAPTER 
    WITH
    SQL_DIALECT     = 'EXASOL'
    CONNECTION_NAME = 'EXASOL_CONNECTION'
    SCHEMA_NAME     = '<schema name>';
```

## Supported Capabilities

The Exasol SQL dialect supports all capabilities that are supported by the virtual schema framework.