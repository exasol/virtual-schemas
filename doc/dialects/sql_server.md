# SQL Server SQL Dialect

## JDBC driver

The SQL Server Dialect was tested with the jTDS 1.3.1 JDBC driver and SQL Server 2014.
As the jTDS driver is already pre-installed for the `IMPORT` command itself you only need
to upload the `jtds.jar` to a bucket for the adapter script.

## Adapter Script

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.sql_server_jdbc_adapter AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/bfsdefault/jars/virtualschema-jdbc-adapter-dist-1.19.1.jar;
  %jar /buckets/bfsdefault/jars/jtds.jar;
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