# MySQL SQL Dialect

[MySQL](https://www.mysql.com/) is an open-source relational database management system.

## Registering the JDBC Driver in EXAOperation

First download the [MySQL JDBC driver](https://dev.mysql.com/downloads/connector/j/).
Select Operating System -> Platform Independent -> Download.

Now register the driver in EXAOperation:

1. Click "Software"
1. Switch to tab "JDBC Drivers"
1. Click "Browse..."
1. Select JDBC driver file
1. Click "Upload"
1. Click "Add"
1. In dialog "Add EXACluster JDBC driver" configure the JDBC driver (see below)

You need to specify the following settings when adding the JDBC driver via EXAOperation.

| Parameter | Value                                               |
|-----------|-----------------------------------------------------|
| Name      | `MYSQL`                                             |
| Main      | `com.mysql.jdbc.Driver`                             |
| Prefix    | `jdbc:mysql:`                                       |
| Files     | `mysql-connector-java-<version>.jar`                |

IMPORTANT: Currently you have to **Disable Security Manager** for the driver if you want to connect to MySQL using Virtual Schemas.
It is necessary because JDBC driver requires a JAVA permission which we do not grant by default.  

## Uploading the JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
1. Upload the driver to BucketFS

This step is necessary since the UDF container the adapter runs in has no access to the JDBC drivers installed via EXAOperation but it can access BucketFS.

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
    %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-2.2.0.jar;
    %jar /buckets/<BFS service>/<bucket>/mysql-connector-java-<version>.jar;
/
;
```

## Defining a Named Connection

Define the connection to MySQL as shown below.

```sql
CREATE OR REPLACE CONNECTION MYSQL_CONNECTION
TO 'jdbc:mysql://<host>:<port>/'
USER '<user>'
IDENTIFIED BY '<password>';
```

## Creating a Virtual Schema

Below you see how a MySQL Virtual Schema is created. Use CATALOG_NAME property to select a database.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT = 'MYSQL'
    CONNECTION_NAME = 'MYSQL_CONNECTION'
    CATALOG_NAME = '<database name>';
```

## Data Types Mapping and Limitations

- `TIME` is casted to `TIMESTAMP` with a format `1970-01-01 hh:mm:ss`.   
- Unsupported data types: `BINARY`, `VARBINARY`, `BLOB`, `TINYBLOB`, `MEDIUMBLOB`, `LONGBLOB`.