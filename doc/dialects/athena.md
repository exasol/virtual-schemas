# Athena SQL Dialect

The Athena SQL Dialect supports Amazon's [AWS Athena](https://aws.amazon.com/athena/), a managed service that lets you read files on S3 as if they were part of a relational database.

>(i) Information for DbVisualizer users
>To tell DbVisualizer that a part of a script should be handled as a single statement, you can insert an SQL block begin identifier just >before the block and an end identifier after the block. The delimiter must be the only text on the line. The default value for the >Begin Identifier is --/ and for the End Identifier it is /.



## JDBC Driver

### Registering the JDBC Driver in EXAOperation

First download the [Athena JDBC driver](https://docs.aws.amazon.com/athena/latest/ug/connect-with-jdbc.html).

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
| Name      | `ATHENA`                                            |
| Main      | `com.amazon.athena.jdbc.Driver`                     |
| Prefix    | `jdbc:awsathena:`                                   |
| Files     | `AthenaJDBC42_<JDBC driver version>.jar`            |

Please refer to the [documentation on configuring JDBC connections to Athena](https://docs.aws.amazon.com/athena/latest/ug/connect-with-jdbc.html) for details.

### Upload JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) (recommended: `jdbc`)
1. Upload the driver to BucketFS

This step is necessary since the UDF container the adapter runs in has no access to the JDBC drivers installed via EXAOperation but it can access BucketFS.

## Adapter Script

You install the adapter script via the special SQL command `CREATE JAVA ADAPTER SCRIPT`. Please remember to replace the placeholders in pointy brackets (e.g. `<JDBC driver version>`) with their actual values.

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
    %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;
    %jar /buckets/bucketfs1/jdbc/virtualschema-jdbc-adapter-dist-1.18.1.jar;
    %jar /buckets/bucketfs1/jdbc/AthenaJDBC42-<JDBC driver version>.jar;
/
```

## Creating a Virtual Schema

Below you see how an Athena Virtual Schema is created. Please note that you have to provide the name of the database in the property `SHEMA_NAME` since Athena simulates catalogs.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT = 'ATHENA'
    CONNECTION_NAME = '<connection name>'
    SCHEMA_NAME = '<database name>';
```
