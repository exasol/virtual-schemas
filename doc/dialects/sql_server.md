# SQL Server SQL Dialect

[Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server/sql-server-2017) is a Relational Database Management System (RDBMS) developed by Microsoft. 

## Uploading the JDBC Driver to EXAOperation

First download the [jTDS JDBC driver](https://sourceforge.net/projects/jtds/files/).

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
1. Upload the driver to BucketFS

## Installing the Adapter Script

Upload the latest available release of [Virtual Schema JDBC Adapter](https://github.com/exasol/virtual-schemas/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-3.0.2.jar;
  %jar /buckets/<BFS service>/<bucket>/jtds.jar;
/
```

## Defining a Named Connection

Define the connection to SQL Server as shown below. We recommend using TLS to secure the connection.

```sql
CREATE OR REPLACE CONNECTION SQLSERVER_CONNECTION
TO 'jdbc:jtds:sqlserver://<server name>:<port>/<database name>'
USER '<user>'
IDENTIFIED BY '<passsword>';
```

## Creating a Virtual Schema

Below you see how an SQL Server Virtual Schema is created.

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

## Troubleshooting 

- SQL SERVER jTDS JDBC driver contains a [bug](https://sourceforge.net/p/jtds/bugs/679/) in DATE type. 
The returned datatype for a SQLServer DATE type is a VARCHAR with a length of 10. If you want to avoid it you can use a newer driver, for example:[mssql-jdbc-7.2.2.jre8.jar](https://www.microsoft.com/en-us/download/details.aspx?id=57782).
Please, be aware that the new driver is not completely tested with Virtual Schemas. The driver's information for this dialect will be updated after we test the driver. 
