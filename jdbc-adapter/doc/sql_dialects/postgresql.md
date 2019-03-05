# PostgreSQL SQL Dialect

## JDBC Driver

The PostgreSQL dialect was tested with JDBC driver version 42.0.0 and PostgreSQL 9.6.2 .

## Adapter Script

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter 
  AS
  
  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.5.3.jar;
									 
  // You have to add all files of the data source jdbc driver here (e.g. MySQL or Hive)
  %jar /buckets/bucketfs1/bucket1/postgresql-42.0.0.jar;

/
```

## Creating a Virtual Schema

```sql
CREATE VIRTUAL SCHEMA postgres
	USING adapter.jdbc_adapter 
	WITH
	SQL_DIALECT = 'POSTGRESQL'
	CATALOG_NAME = 'postgres'
	SCHEMA_NAME = 'public'
	CONNECTION_NAME = 'POSTGRES_DOCKER'
	;
```

## Postgres identifiers

In contrast to EXASOL, PostgreSQL does not treat identifiers as specified in the SQL standard. PostgreSQL folds unquoted identifiers to lower case instead of upper case. The adapter can do the identifier conversion as long as there are no identifiers in the PostgreSQL database that contain upper case characters. If that is the case an error will be thrown when creating or refreshing the virtual schema.
In order to create or refresh the virtual schema regrardlessly, you can specifiy that the adapter should ignore this specific error:
```sql
CREATE VIRTUAL SCHEMA postgres
	USING adapter.jdbc_adapter 
	WITH
	SQL_DIALECT = 'POSTGRESQL'
	CATALOG_NAME = 'postgres'
	SCHEMA_NAME = 'public'
	CONNECTION_NAME = 'POSTGRES_DOCKER'
	IGNORE_ERRORS = 'POSTGRESQL_UPPERCASE_TABLES'
;
```
You can also set this property for an exitsing virtual schema:
```sql
ALTER VIRTUAL SCHEMA postgres SET IGNORE_ERRORS = 'POSTGRESQL_UPPERCASE_TABLES';
```
However you won't be able to query the identifier containing the upper case character, the name resolution will fail when querying the virtual table.