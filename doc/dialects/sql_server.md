# SQL Server SQL Dialect

[Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server/sql-server-2017) is a relational database management system developed by Microsoft. 

## Uploading the JDBC Driver to EXAOperation

First download the [SQL Server JDBC driver](https://sourceforge.net/projects/jtds/files/).

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
1. Upload the driver to BucketFS

## Installing the Adapter Script

Upload the last available release of [Virtual Schema JDBC Adapter](https://github.com/exasol/virtual-schemas/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.l

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.sql_server_jdbc_adapter AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-1.19.1.jar;
  %jar /buckets/<BFS service>/<bucket>/jtds.jar;
/
```

## Defining a Named Connection

Define the connection to Athena as shown below. We recommend using TLS to secure the connection.

```sql
CREATE OR REPLACE CONNECTION SQLSERVER_CONNECTION
TO 'jdbc:jtds:sqlserver://<server name>:<port>/<database name>'
USER '<access key ID>'
IDENTIFIED BY '<access key>';
```

## Creating a Virtual Schema

Below you see how an SQL Server Virtual Schema is created. Please note that you have to provide the name of the database in the property `SHEMA_NAME` since Athena simulates catalogs.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT = 'SQLSERVER'
    CONNECTION_NAME = 'SQLSERVER_CONNECTION'
    CATALOG_NAME   =  '<catalog name>'
    SCHEMA_NAME = '<database name>';
```

## Testing inforamtion

The SQL Server Dialect was tested with the jTDS 1.3.1 JDBC driver and SQL Server 2014.