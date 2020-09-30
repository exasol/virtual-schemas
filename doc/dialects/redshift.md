# Redshift SQL Dialect

The Redshift SQL Dialect supports Amazon's [AWS Redshift](https://aws.amazon.com/redshift/) managed data warehouse. Redshift is at its core a relational database based on PostgreSQL.

In addition to reading from the regular relational database, this SQL dialect adapter also supports reading from [Redshift Spectrum](https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum.html). This allows reading file based data from S3.

## Registering the JDBC Driver in EXAOperation

First download the [Redshift JDBC driver](https://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver).

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
| Name      | `REDSHIFT`                                          |
| Main      | `com.amazon.redshift.jdbc42.Driver`                 |
| Prefix    | `jdbc:redshift:`                                    |
| Files     | `RedshiftJDBC42-<JDBC driver version>.jar`          |

Please refer to the [documentation on configuring JDBC connections to Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html) for details.

## Upload JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) (recommended: `jdbc`)
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
    %jar /buckets/<BFS service>/<bucket>/virtual-schema-dist-6.0.0-bundle-4.0.4.jar;
    %jar /buckets/<BFS service>/<bucket>/RedshiftJDBC42-<JDBC driver version>.jar;
/
;
```

## Defining a Named Connection

Define the connection to Redshift as shown below. We recommend using TLS to secure the connection.

```sql
CREATE OR REPLACE CONNECTION REDSHIFT_CONNECTION
TO 'jdbc:redshift://<cluster>.<region>.redshift.amazonaws.com:<port>/<database>'
USER '<user>'
IDENTIFIED BY '<password>';
```

## Creating a Virtual Schema

Below you see how a Redshift Virtual Schema is created. Please note that you have to provide the name of the database in the property `CATALOG_NAME` since Redshift simulates catalogs.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT = 'REDSHIFT'
    CONNECTION_NAME = 'REDSHIFT_CONNECTION'
    CATALOG_NAME = '<database name>'
    SCHEMA_NAME = 'public';
```
