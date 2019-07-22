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
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-1.19.1.jar;
  %jar /buckets/<BFS service>/<bucket>/terajdbc4.jar;
  %jar /buckets/<BFS service>/<bucket>/tdgssconfig.jar;
/
```

## Creating a Virtual Schema

```sql
CREATE VIRTUAL SCHEMA TERADATA_financial USING adapter.jdbc_adapter 
WITH
  SQL_DIALECT     = 'TERADATA'
  CONNECTION_NAME = 'TERADATA_CONNECTION'
  SCHEMA_NAME     = '<schema>'
;
```