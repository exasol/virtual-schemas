# DB2 SQL Dialect

DB2 was tested with the IBM DB2 JCC Drivers that come with DB2 LUW V10.1 and V11. As these drivers didn't have any major changes in the past years any DB2 driver should work (back to V9.1). The driver comes with 2 different implementations `db2jcc.jar` and `db2jcc4.jar`. All tests were made with the `db2jcc4.jar`.

Additionally there are 2 files for the DB2 Driver.

* `db2jcc_license_cu.jar` - License File for DB2 on Linux Unix and Windows
* `db2jcc_license_cisuz.jar` - License File for DB2 on zOS (Mainframe)

Make sure that you upload the necessary license file for the target platform you want to connect to. 

## Supported Capabilities

The DB2 dialect handles some casts in regards of time data types and functions.

Casting of Data Types

* `TIMESTAMP` and `TIMESTAMP(x)` will be cast to `VARCHAR` to not lose precision.
* `VARCHAR` and `CHAR` for bit data will be cast to a hex string with double the original size
* `TIME` will be cast to `VARCHAR(8)`
* `XML` will be cast to `VARCHAR(DB2_MAX_LENGTH)`
* `BLOB` is not supported

Casting of Functions

* `LIMIT` will replaced by `FETCH FIRST x ROWS ONLY`
* `OFFSET` is currently not supported as only DB2 V11 support this natively
* `ADD_DAYS`, `ADD_WEEKS` ... will be replaced by `COLUMN + DAYS`, `COLUMN + ....`


## JDBC Driver

You have to specify the following settings when adding the JDBC driver via EXAOperation:

* Name: `DB2`
* Main: `com.ibm.db2.jcc.DB2Driver`
* Prefix: `jdbc:db2:`

## Installing the Adapter Script

Upload the [Virtual Schema JDBC Adapter JAR](https://github.com/exasol/virtual-schemas/releases/download/1.19.0/virtualschema-jdbc-adapter-dist-1.19.0.jar) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

### For Regular DB2 Servers

```sql
CREATE or replace JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-1.19.1.jar;
  %jar /buckets/<BFS service>/<bucket>/db2jcc4.jar;
  %jar /buckets/<BFS service>/<bucket>/db2jcc_license_cu.jar;
/
```

### For Mainframes

```sql
CREATE or replace JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-1.19.1.jar;
  %jar /buckets/<BFS service>/<bucket>/db2jcc4.jar;
  %jar /buckets/<BFS service>/<bucket>/db2jcc_license_cu.jar;
  %jar /buckets/<BFS service>/<bucket>/db2jcc_license_cisuz.jar;
/
```

## Creating a Virtual Schema

You can now create a virtual schema as follows:

```sql
create or replace connection DB2_CON to 'jdbc:db2://host:port/database' user 'db2-usr' identified by 'db2-pwd';

create  virtual schema db2 using adapter.jdbc_adapter with
	SQL_DIALECT = 'DB2'
	CONNECTION_NAME = 'DB2_CON'
	SCHEMA_NAME = '<schema>'
;
```

`<schema>` has to be replaced by the actual DB2 schema you want to connect to.

## Running the DB2 Integration Tests

A how to has been included in the [setup SQL file](../../jdbc-adapter/integration-test-data/db2-testdata.sql)