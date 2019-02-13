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
* `OFFSET` is currently not supported as only DB2 V11 support this nativly
* `ADD_DAYS`, `ADD_WEEKS` ... will be replaced by `COLUMN + DAYS`, `COLUMN + ....`


## JDBC Driver

You have to specify the following settings when adding the JDBC driver via EXAOperation:

* Name: `DB2`
* Main: `com.ibm.db2.jcc.DB2Driver`
* Prefix: `jdbc:db2:`

## Adapter script

```sql
CREATE or replace JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS

  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.4.0.jar;

  // DB2 Driver files
  %jar /buckets/bucketfs1/bucket1/db2jcc4.jar;
  %jar /buckets/bucketfs1/bucket1/db2jcc_license_cu.jar;
  // uncomment for mainframe connection and upload  db2jcc_license_cisuz.jar;
  //%jar /buckets/bucketfs1/bucket1/db2jcc_license_cisuz.jar;
/
```

## Creating a Virtual Schema

You can now create a virtual schema as follows:

```sql
create or replace connection DB2_CON to 'jdbc:db2://host:port/database' user 'db2-usr' identified by 'db2-pwd';

create  virtual schema db2 using adapter.jdbc_adapter with
	SQL_DIALECT = 'DB2'
	CONNECTION_NAME = 'DB2_CON'
	SCHEMA_NAME = '<schemaname>'
;
```

`<schemaname>` has to be replaced by the actual db2 schema you want to connect to.

## Running the DB2 Integration Tests

A how to has been included in the [setup sql file](../../integration-test-data/db2-testdata.sql)