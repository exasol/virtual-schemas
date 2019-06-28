# Redshift SQL Dialect

The Redshift SQL Dialect supports Amazon's [AWS Redshift](https://aws.amazon.com/redshift/) managed data warehouse. Redshift is at its core a relational database based on PostgreSQL.

In addition to reading from the regular relational database, this SQL dialect adapter also supports reading from [Redshift Spectrum](https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum.html). This allows reading file based data from S3.

## JDBC Driver

### Registering the JDBC Driver in EXAOperation

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

### Upload JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) (recommended: `jdbc`)
1. Upload the driver to BucketFS

This step is necessary since the UDF container the adapter runs in has no access to the JDBC drivers installed via EXAOperation but it can access BucketFS.

## Adapter Script

You install the adapter script via the special SQL command `CREATE JAVA ADAPTER SCRIPT`. Please remember to replace the placeholders in pointy brackets (e.g. `<JDBC driver version>`) with their actual values.

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
    %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;
    %jar /buckets/bucketfs1/jdbc/virtualschema-jdbc-adapter-dist-1.19.1.jar;
    %jar /buckets/bucketfs1/jdbc/RedshiftJDBC42-<JDBC driver version>.jar;
/
```

## Creating a Virtual Schema

Below you see how a Redshift Virtual Schema is created. Please note that you have to provide the name of the database in the property `CATALOG_NAME` since Redshift simulates catalogs.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT = 'REDSHIFT'
    CONNECTION_NAME = '<connection name>'
    CATALOG_NAME = '<database name>'
    SCHEMA_NAME = 'public';
```