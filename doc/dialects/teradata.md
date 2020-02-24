# Teradata SQL Dialect

Teradata is one of the Relational Database Management System based on a Massively Parallel Processing (MPP) Architecture.

## Registering the JDBC Driver in EXAOperation

First download the [Teradata JDBC driver](https://downloads.teradata.com/download/connectivity/jdbc-driver).

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
| Name      | `TERADATA`                                          |
| Main      | `com.teradata.jdbc.TeraDriver`                      |
| Prefix    | `jdbc:teradata:`                                    |
| Files     | `terajdbc4.jar`, `tdgssconfig.jar`                  |

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
  %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-3.1.1.jar;
  %jar /buckets/<BFS service>/<bucket>/terajdbc4.jar;
  %jar /buckets/<BFS service>/<bucket>/tdgssconfig.jar;
/
;
```

## Defining a Named Connection

Define the connection to Teradata as shown below. 

```sql
CREATE OR REPLACE CONNECTION TERADATA_CONNECTION
TO 'jdbc:teradata://<database server>/database=<database name>'
USER '<user>'
IDENTIFIED BY '<password>';
```

## Creating a Virtual Schema

Below you see how a Teradata Virtual Schema is created. 

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT = 'TERADATA'
    CONNECTION_NAME = 'TERADATA_CONNECTION'
    SCHEMA_NAME = '<schema name>';
```