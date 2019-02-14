# Teradata SQL Dialect

## JDBC Driver

You have to specify the following settings when adding the JDBC driver via EXAOperation:

* Name: `TERADATA`
* Main: `com.teradata.jdbc.TeraDriver`
* Prefix: `jdbc:teradata:`
* Files: `terajdbc4.jar`, `tdgssconfig.jar`

Please also upload the jar files to a bucket for the adapter script.

## Adapter script

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter 
  AS
  
  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.4.0.jar;
									 
  // You have to add all files of the data source jdbc driver here (e.g. MySQL or Hive)
  %jar /buckets/bucketfs1/bucket1/terajdbc4.jar;
  %jar /buckets/bucketfs1/bucket1/tdgssconfig.jar;

/
```

## Creating a Virtual Schema

```sql
CREATE VIRTUAL SCHEMA TERADATA_financial USING adapter.jdbc_adapter 
WITH
  SQL_DIALECT     = 'TERADATA'
  CONNECTION_NAME = 'TERADATA_CONNECTION'
  SCHEMA_NAME     = 'financial'
;
```