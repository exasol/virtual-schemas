# Snowflake SQL Dialect

The Snowflake's Software-as-a-Service data warehouse is an analytic data warehouse that uses an architecture designed for cloud.

## Registering the JDBC Driver in EXAOperation

First download the [Snowflake JDBC driver](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc).

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
| Name      | `SNOWFLAKE`                                            |
| Main      | `net.snowflake.client.jdbc.SnowflakeDriver`                     |
| Prefix    | `jdbc:snowflake:`                                   |
| Files     | `snowflake-jdbc-<JDBC driver version>.jar`            |

Please refer to the [documentation on configuring JDBC connections to Snowflake](https://docs.snowflake.com/en/user-guide/jdbc-configure.html) for details.

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
    %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-4.0.1.jar;
    %jar /buckets/<BFS service>/<bucket>/snowflake-jdbc-<JDBC driver version>.jar;
/
;
```

## Defining a Named Connection

Define the connection to Snowflake as shown below. We recommend using TLS to secure the connection.

```sql
CREATE OR REPLACE CONNECTION SNOWFLAKE_CONNECTION
TO 'jdbc:snowflake://<account_name>.snowflakecomputing.com/?<connection_params>'
USER '<access key ID>'
IDENTIFIED BY '<access key>';
```

## Creating a Virtual Schema

Below you see how an Snowflake Virtual Schema is created. Please note that you have to provide the name of the database in the property `SHEMA_NAME`.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT = 'SNOWFLAKE'
    CONNECTION_NAME = 'SNOWFLAKE_CONNECTION'
    SCHEMA_NAME = '<database name>';
```