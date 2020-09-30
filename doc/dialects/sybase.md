# Sybase SQL Dialect

[SAP ASE](https://www.sap.com/products/sybase-ase.html), originally known as Sybase SQL Server is a relational model database server developed by Sybase Corporation, which later became part of SAP AG.

## Uploading the JDBC Driver to EXAOperation

While the jTDS driver is pre-installed in EXAOperation, you still need to upload `jdts.jar` to BucketFS.
You can check the Sybase version with the following SQL command:

```sql
SELECT @@version;
```

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
  %jar /buckets/<BFS service>/<bucket>/virtual-schema-dist-6.0.0-bundle-4.0.4.jar;
  %jar /buckets/<BFS service>/<bucket>/jtds-<version>.jar;
/
```

## Defining a Named Connection

Define the connection to Sybase as shown below. 

```sql
CREATE OR REPLACE CONNECTION SYBASE_CONNECTION
TO 'jdbc:jtds:sybase://<host>:<port>/<database name>'
USER '<user>'
IDENTIFIED BY '<password>';
```

## Creating a Virtual Schema

Below you see how a Sybase Virtual Schema is created.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
	SQL_DIALECT = 'SYBASE'
	CONNECTION_NAME = 'SYBASE_CONNECTION'
	CATALOG_NAME = '<catalog name>'
	SCHEMA_NAME = '<schema name>';
```

## Supported Data types

* `NUMERIC/DECIMAL(precision, scale)`: Sybase supports precision values up to 38, Exasol only up to 36 decimals. `NUMERIC/DECIMAL` with precision <= 36 are mapped to Exasol's `DECIMAL` type; greater precision values are mapped to a `VARCHAR` column.
* The Sybase data type `CHAR(n > 2000)` is mapped to Exasol's `VARCHAR(n)`. Exasol only supports `n <= 2000` for data type `CHAR`.
* The Sybase data types `TEXT` and `UNITEXT` are mapped to `VARCHAR(2000000) UTF8`. If the virtual schema is queried and a row of the text column is matched that contains a value that exceed Exasol's column size, an error is shown.
* The Sybase data types `BINARY`, `VARBINARY`, and `IMAGE` are not supported.

## Testing information

The Sybase dialect was tested with the [jTDS 1.3.1 JDBC driver](https://sourceforge.net/projects/jtds/files/jtds/1.3.1/) and Sybase 16.0.
