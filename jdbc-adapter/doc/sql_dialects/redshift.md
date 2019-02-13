# Redshift SQL Dialect

## JDBC Driver

You have to specify the following settings when adding the JDBC driver via EXAOperation:
* Name: `REDSHIFT`
* Main: `com.amazon.redshift.jdbc.Driver`
* Prefix: `jdbc:redshift:`
* Files: `RedshiftJDBC42-1.2.1.1001.jar`

Please also upload the driver jar into a bucket for the adapter script.

## Adapter Script

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter 
  AS
  
  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.4.0.jar;
									 
  // You have to add all files of the data source jdbc driver here (e.g. MySQL or Hive)

  %jar /buckets/bucketfs1/bucket1/RedshiftJDBC42-1.2.1.1001.jar;

/
```

## Creating a Virtual Schema

```sql
CREATE VIRTUAL SCHEMA redshift_tickit
	USING adapter.jdbc_adapter 
	WITH
	SQL_DIALECT = 'REDSHIFT'
	CONNECTION_NAME = 'REDSHIFT_CONNECTION'
	CATALOG_NAME = 'database_name'
	SCHEMA_NAME = 'public'
	;
```