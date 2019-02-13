# SQL Server SQL Dialect

## JDBC driver

The SQL Server Dialect was tested with the jTDS 1.3.1 JDBC driver and SQL Server 2014.
As the jTDS driver is already pre-installed for the `IMPORT` command itself you only need
to upload the `jtds.jar` to a bucket for the adapter script.

## Adapter Script

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.sql_server_jdbc_adapter 
  AS
  
  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.4.0.jar;
									 
  // You have to add all files of the data source jdbc driver here 
  %jar /buckets/bucketfs1/bucket1/jtds.jar;
/
```

## Creating a Virtual Schema

```sql
CREATE VIRTUAL SCHEMA VS_SQLSERVER USING adapter.sql_server_jdbc_adapter
WITH
  SQL_DIALECT     = 'SQLSERVER'
  CONNECTION_NAME = 'SQLSERVER_CONNECTION'
  CATALOG_NAME	  =  'MyDatabase'
  SCHEMA_NAME     = 'dbo'
;
```