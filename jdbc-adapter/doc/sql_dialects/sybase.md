# Sybase SQL Dialect

## JDBC driver

The Sybase dialect was tested with the [jTDS 1.3.1 JDBC driver](https://sourceforge.net/projects/jtds/files/jtds/1.3.1/) and Sybase 16.0.
While the jTDS driver is pre-installed in EXAOperation, you still need to upload `jdts.jar` to BucketFS.

You can check the Sybase version with the following SQL command:

```sql
SELECT @@version;
```

## Adapter script

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter
  AS

  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;
  %jar /buckets/bucketfs1/virtualschema/virtualschema-jdbc-adapter-dist-1.4.0.jar;
  %jar /buckets/bucketfs1/virtualschema/jtds-1.3.1.jar;
/
```

## Installing the Test Data

Create and populate the test database using the [sybase-testdata.sql](../integration-test-data/sybase-testdata.sql) SQL script.

## Creating a Virtual Schema

```sql
CREATE OR REPLACE CONNECTION "conn_sybase"
	TO 'jdbc:jtds:sybase://172.17.0.1:5000/testdb'
	USER 'tester'
	IDENTIFIED BY 'pass'

CREATE VIRTUAL SCHEMA sybase USING adapter.jdbc_adapter WITH
	SQL_DIALECT = 'SYBASE'
	CONNECTION_NAME = 'CONN_SYBASE'
	CATALOG_NAME = 'testdb'
	SCHEMA_NAME = 'tester';
```

## Supported Data types

* `NUMERIC/DECIMAL(precision, scale)`: Sybase supports precision values up to 38, Exasol only up to 36 decimals. `NUMERIC/DECIMAL` with precision <= 36 are mapped to Exasol's `DECIMAL` type; greater precision values are mapped to a `VARCHAR` column.
* The Sybase data type `CHAR(n > 2000)` is mapped to Exasol's `VARCHAR(n)`. Exasol only supports `n <= 2000` for data type `CHAR`.
* The Sybase data types `TEXT` and `UNITEXT` are mapped to `VARCHAR(2000000) UTF8`. If the virtual schema is queried and a row of the text column is matched that contains a value that exceed Exasol's column size, an error is shown.
* The Sybase data types `BINARY`, `VARBINARY`, and `IMAGE` are not supported.