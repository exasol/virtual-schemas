# PostgreSQL SQL Dialect

[PostgreSQL](https://www.postgresql.org/) is an open-source  Relational Database Management System (RDBMS).

## Uploading the JDBC Driver to EXAOperation

First download the [PostgreSQL JDBC driver](https://jdbc.postgresql.org/).

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
  %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-2.2.1.jar;
  %jar /buckets/<BFS service>/<bucket>/postgresql-<version>.jar;
/
```

## Defining a Named Connection

Define the connection to PostgreSQL as shown below. We recommend using TLS to secure the connection.

```sql
CREATE OR REPLACE CONNECTION POSTGRESQL_CONNECTION
TO 'jdbc:postgresql://<host>:<port>/<database name>?ssl=true&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory'
USER '<user>'
IDENTIFIED BY '<password>';
```

## Creating a Virtual Schema

Below you see how a PostreSQL Virtual Schema is created.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
	USING ADAPTER.JDBC_ADAPTER 
	WITH
	SQL_DIALECT = 'POSTGRESQL'
	CATALOG_NAME = '<catalog name>'
	SCHEMA_NAME = '<schema name>'
	CONNECTION_NAME = 'POSTGRESQL_CONNECTION'
	;
```

## Postgres Identifiers

In contrast to Exasol, PostgreSQL does not treat identifiers as specified in the SQL standard. PostgreSQL folds unquoted identifiers to lower case instead of upper case. The adapter has two modes for handling this:

### Automatic Identifier conversion

This is the default mode for handling identifiers, but identifier conversion can also be set explicitly using the following property:

```sql
ALTER VIRTUAL SCHEMA <virtual schema name> SET POSTGRESQL_IDENTIFIER_MAPPING = 'CONVERT_TO_UPPER';
```

In this mode you do not have to care about identifier handling. Everything will work as expected out of the box as long as you **do not use quoted identifiers** (in the PostgreSQL Schema as well as in the Exasol Virtual Schema). More specifically everything will work as long as there are no identifiers in the PostgreSQL database that contain upper case characters. If that is the case an error is thrown when creating or refreshing the virtual schema.
Regardless of this, you can create or refresh the virtual schema by specifying the adapter to ignore this particular error as shown below:

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
	USING ADAPTER.JDBC_ADAPTER 
	WITH
	SQL_DIALECT = 'POSTGRESQL'
	CATALOG_NAME = '<catalog name>'
	SCHEMA_NAME = '<schema name>'
	CONNECTION_NAME = 'POSTGRESQL_CONNECTION'
	IGNORE_ERRORS = 'POSTGRESQL_UPPERCASE_TABLES'
;
```
You can also set this property to an existing virtual schema:

```sql
ALTER VIRTUAL SCHEMA postgres SET IGNORE_ERRORS = 'POSTGRESQL_UPPERCASE_TABLES';
```
However you **will not be able to query the identifier containing the upper case character**. An error is thrown when querying the virtual table.

A best practice for this mode is: **never quote identifiers** (in the PostgreSQL Schema as well as in the Exasol Virtual Schema). This way everything works without having to change your queries.
An alternative is to use the second mode for identifier handling (see below).

### PostgreSQL like identifier handling

If you use quotes on the PostgreSQL side and have identifiers with uppercase characters, then it is recommended to use this mode. The PostgreSQL like identifier handling does no conversions but mirrors the PostgreSQL metadata as is. A small example to make this clear:
```sql
--Postgres Schema
CREATE TABLE "MyTable"("Col1" VARCHAR(100));
CREATE TABLE MySecondTable(Col1 VARCHAR(100));
--Postgres Queries
SELECT "Col1" FROM "MyTable";
SELECT Col1 FROM MySecondTable;
```
```sql
--Create Virtual Schema on EXASOL side
CREATE VIRTUAL SCHEMA <virtual schema name>
	USING ADAPTER.JDBC_ADAPTER 
	WITH
	SQL_DIALECT = 'POSTGRESQL'
	CATALOG_NAME = '<catalog name>'
	SCHEMA_NAME = '<schema name>'
	CONNECTION_NAME = 'POSTGRESQL_CONNECTION'
	POSTGRESQL_IDENTIFIER_MAPPING = 'PRESERVE_ORIGINAL_CASE'
;
-- Open Schema and see what tables are there
open schema postgres;
select * from cat;
-- result -->
-- TABLE_NAME	TABLE_TYPE
-- ----------------------
-- MyTable       | TABLE
-- mysecondtable | TABLE
```
As you can see `MySecondTable` is displayed in lower case in the virtual schema catalog. This is exactly like it is on the PostgreSQL side, but since unquoted identifiers are folded differently in PostgreSQL you cannot query the table like you did in PostgreSQL:
```sql
--Querying the virtual schema
--> this works
SELECT "Col1" FROM postgres."MyTable";

--> this does not work
SELECT Col1 FROM postgres.MySecondTable;
--> Error:
--  [Code: 0, SQL State: 42000]  object "POSTGRES"."MYSECONDTABLE" not found [line 1, column 18]

--> this works
SELECT "col1" FROM postgres."mysecondtable";
```
Unquoted identifiers are converted to lowercase on the PostgreSQL side, and since there is no catalog conversion these identifiers are also lowercase in Exasol. To query a lowercase identifier you must use quotes in Exasol, because everything that is unquoted gets folded to uppercase.

A best practice for this mode is: **always quote identifiers** (in the PostgreSQL Schema as well as in the Exasol Virtual Schema). This way everything works without having to change your queries.

## Testing information

The PostgreSQL dialect was tested with JDBC drivers of version 42.0.0 and later and PostgreSQL 10 and 11.
Driver version 42.2.6 or later are recommended if you want to establish a TLS-secured connection. 