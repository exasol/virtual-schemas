# User Guide


The JDBC adapter for virtual schemas allows you to connect to JDBC data sources like Hive, Oracle, Teradata, Exasol or any other data source supporting JDBC.
It uses the well proven ```IMPORT FROM JDBC``` Exasol statement behind the scenes to obtain the requested data, when running a query on a virtual table. 
The JDBC adapter also serves as the reference adapter for the Exasol virtual schema framework.

You can find supported dialects in [the list of supported dialects](dialects.md).

The so called `GENERIC` dialect is designed to work with any JDBC driver. It derives the SQL dialect from the JDBC driver metadata. 
However, it does not support any capabilities and might fail if the data source has special syntax or data types, so it should only be used for evaluation purposes.

If you are interested in an introduction to virtual schemas please refer to the [Exasol Documentation Portal](https://docs.exasol.com/home.htm). 

## Before you Start 

Please note that the syntax for creating adapter scripts is not recognized by all SQL clients. 
See [SQL Client Specifics](sql_clients.md) for details

## Getting Started

This page contains common information applicable to all the dialects. You can also check more [detailed guides for each dialect](dialects.md) in the dialects' documentation.

The steps for creating virtual schema are:

* Deploy JDBC Driver Files
* Install the adapter script
* Define a named connection
* Create Virtual Schema


## Deploy JDBC Driver Files

You have to upload the JDBC driver files of your remote database **twice** (except for the Exasol and BigQuery dialects):

* Upload all files of the JDBC driver into a bucket of your choice, so that they can be accessed from the adapter script.
See [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) and [Upload the driver to BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/accessfiles.htm) for details.

* Upload all files of the JDBC driver as a JDBC driver in EXAOperation
  - In EXAOperation go to Software -> JDBC Drivers
  - Add the JDBC driver by specifying the JDBC main class and the prefix of the JDBC connection string
  - Upload all files (one by one) to the specific JDBC to the newly added JDBC driver.

Note that some JDBC drivers consist of several files and that you have to upload all of them. 
To find out which JAR you need, check the individual dialects' documentation pages.

## Install the adapter script

Create a schema to hold the adapter script.

```sql
CREATE SCHEMA SCHEMA_FOR_VS_SCRIPT;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
CREATE JAVA ADAPTER SCRIPT SCHEMA_FOR_VS_SCRIPT.JDBC_ADAPTER_SCRIPT AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/your-bucket-fs/your-bucket/virtual-schema-dist-5.0.3-bundle-4.0.3.jar;
  %jar /buckets/your-bucket-fs/your-bucket/<JDBC driver>.jar;
/
```


## Define a named connection.

First we create a connection and then a virtual schema.

```sql
CREATE CONNECTION JDBC_CONNECTION TO '<jdbc connection string>' USER 'usr' IDENTIFIED BY 'pwd';
```

## Create Virtual Schema

The adapter will retrieve the metadata via JDBC and map them to virtual tables. 
The metadata (virtual tables, columns and data types) are then cached in Exasol.

For a list of all properties supported by the JDBC adapter please refer to the [Virtual Schema's properties reference](virtual_schema_properties.md).

```sql
CREATE VIRTUAL SCHEMA VIRTUAL_SCHEMA_TEST USING SCHEMA_FOR_VS_SCRIPT.JDBC_ADAPTER_SCRIPT WITH
  SQL_DIALECT     = '<dialect name>'
  CONNECTION_NAME = 'JDBC_CONNECTION'
  SCHEMA_NAME     = 'default';
```

## Using the Adapter 

We can now explore the tables in the virtual schema, just like for a regular schema:

```sql
OPEN SCHEMA VIRTUAL_SCHEMA_TEST;
SELECT * FROM MY_TABLE;
```

And we can run arbitrary queries on the virtual tables:

```sql
SELECT count(*) FROM MY_TABLE;
SELECT DISTINCT USER_ID FROM MY_TABLE;
```

Behind the scenes the Exasol command `IMPORT FROM JDBC` will be executed to obtain the data needed from the data source to fulfil the query. 
The Exasol database interacts with the adapter to pushdown as much as possible to the data source (e.g. filters, aggregations or `ORDER BY` / `LIMIT`), 
while considering the capabilities of the data source.

Let's combine virtual and native tables in a query:

```sql
SELECT * FROM MY_TABLE JOIN NATIVE_SCHEMA.USERS on MY_TABLE.USERID = USERS.ID;
```

You can refresh the schemas metadata, e.g. if tables were added in the remote system:

```sql
ALTER VIRTUAL SCHEMA VIRTUAL_SCHEMA_TEST REFRESH;
ALTER VIRTUAL SCHEMA VIRTUAL_SCHEMA_TEST REFRESH TABLES T1 T2; -- refresh only these tables
```

Or set properties. Depending on the adapter and the property you set this might update the metadata or not. 
In our example the metadata are affected, because afterwards the virtual schema will only expose two virtual tables.

```sql
ALTER VIRTUAL SCHEMA VIRTUAL_SCHEMA_TEST SET TABLE_FILTER='MY_TABLE, CLICKS';
```

Finally, you can unset properties:

```sql
ALTER VIRTUAL SCHEMA VIRTUAL_SCHEMA_TEST SET TABLE_FILTER=null;
```

Or drop the virtual schema:

```sql
DROP VIRTUAL SCHEMA VIRTUAL_SCHEMA_TEST CASCADE;
```


## Unrecognized and Unsupported Data Types

Not all data types present in a source database have a matching equivalent in Exasol. Also software updates on the source database can introduce new data type that the Virtual schema does not recognize.

There are a few important things you need to know about those data types.

1. Columns of an unrecognized / unsupported data type are not mapped in a Virtual Schema. From Exasol's perspective those columns do not exist on a table. This is done so that tables containing those columns can still be mapped and do not have to be rejected as a whole.
2. You can't query columns of an unrecognized / unsupported data type. If the source table contains them, you have to *explicitly* exclude them from the query. You can for example not use the asterisk (`*`) on a table that contains one ore more of those columns. This will result in an error issued by the Virtual schema.
3. If you want to query all columns except unsupported, add `1` to the columns list. Otherwise you will see the same error as if you query with the asterisk (`*`).
    For example, a table contains 3 columns: `bool_column` BOOLEAN, `timestamp_column` TIMESTAMP, `blob_column` BLOB. The column BLOB is not supported. If you want to query two other columns, use: `SELECT "bool_column", "timestamp_column", 1 FROM table_name;` .
4. You can't use functions that result in an unsupported / unknown data type.  
