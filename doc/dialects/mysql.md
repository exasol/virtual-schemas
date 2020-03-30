# MySQL SQL Dialect

[MySQL](https://www.mysql.com/) is an open-source relational database management system.

## Registering the JDBC Driver in EXAOperation

First download the [MySQL JDBC driver](https://dev.mysql.com/downloads/connector/j/).
Select Operating System -> Platform Independent -> Download.

Now register the driver in EXAOperation:

1. Click "Software"
1. Switch to tab "JDBC Drivers"
1. Click "Browse..."
1. Select JDBC driver file
1. Click "Upload"
1. Click "Add"
1. In dialog "Add EXACluster JDBC driver" configure the JDBC driver (see below)

You need to specify the following settings when adding the JDBC driver via EXAOperation.

| Parameter | Value                                               |
|-----------|-----------------------------------------------------|
| Name      | `MYSQL`                                             |
| Main      | `com.mysql.jdbc.Driver`                             |
| Prefix    | `jdbc:mysql:`                                       |
| Files     | `mysql-connector-java-<version>.jar`                |

IMPORTANT: Currently you have to **Disable Security Manager** for the driver if you want to connect to MySQL using Virtual Schemas.
It is necessary because JDBC driver requires a JAVA permission which we do not grant by default.  

## Uploading the JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
1. Upload the driver to BucketFS

This step is necessary since the UDF container the adapter runs in has no access 
to the JDBC drivers installed via EXAOperation but it can access BucketFS.

## Installing the Adapter Script

Upload the latest available release of [Virtual Schema JDBC Adapter](https://github.com/exasol/virtual-schemas/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA SCHEMA_FOR_VS_SCRIPT;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT SCHEMA_FOR_VS_SCRIPT.ADAPTER_SCRIPT_MYSQL AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-3.1.2.jar;
    %jar /buckets/<BFS service>/<bucket>/mysql-connector-java-<version>.jar;
/
;
```

## Defining a Named Connection

Define the connection to MySQL as shown below.

```sql
CREATE OR REPLACE CONNECTION MYSQL_JDBC_CONNECTION
TO 'jdbc:mysql://<host>:<port>/'
USER '<user>'
IDENTIFIED BY '<password>';
```

## Creating a Virtual Schema

Below you see how a MySQL Virtual Schema is created. Use CATALOG_NAME property to select a database.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING SCHEMA_FOR_VS_SCRIPT.ADAPTER_SCRIPT_MYSQL
    WITH
    SQL_DIALECT = 'MYSQL'
    CONNECTION_NAME = 'MYSQL_JDBC_CONNECTION'
    CATALOG_NAME = '<database name>';
```

## Data Types Conversion

MySQL Data Type    | Supported | Converted Exasol Data Type| Known limitations
-------------------|-----------|---------------------------|-------------------
BOOLEAN            |  ✓        | BOOLEAN                   | 
BIGINT             |  ✓        | DECIMAL                   | 
BINARY             |  ×        |                           | 
BIT                |  ✓        | BOOLEAN                   | 
BLOB               |  ×        |                           | 
CHAR               |  ✓        | CHAR                      | 
DATE               |  ✓        | DATE                      | 
DATETIME           |  ✓        | TIMESTAMP                 | 
DECIMAL            |  ✓        | DECIMAL                   |  
DOUBLE             |  ✓        | DOUBLE PRECISION          | 
ENUM               |  ✓        | CHAR                      | 
FLOAT              |  ✓        | DOUBLE PRECISION          |  
INT                |  ✓        | DECIMAL                   | 
LONGBLOB           |  ×        |                           | 
LONGTEXT           |  ✓        | VARCHAR(2000000)          | 
MEDIUMBLOB         |  ×        |                           | 
MEDIUMINT          |  ✓        | DECIMAL                   | 
MEDIUMTEXT         |  ✓        | VARCHAR(2000000)          | 
SET                |  ✓        | CHAR                      | 
SMALLINT           |  ✓        | DECIMAL                   | 
TEXT               |  ✓        | VARCHAR(65535)            | The size of the column is always 65535.*
TINYBLOB           |  ×        |                           | 
TINYINT            |  ✓        | DECIMAL                   | 
TINYTEXT           |  ✓        | VARCHAR                   | 
TIME               |  ✓        | TIMESTAMP                 | Casted to `TIMESTAMP` with a format `1970-01-01 hh:mm:ss`.  
TIMESTAMP          |  ✓        | TIMESTAMP                 | 
VARBINARY          |  ×        |                           | 
VARCHAR            |  ✓        | VARCHAR                   | 
YEAR               |  ✓        | DATE                      |

* The tested versions of MySQL Connector JDBC Driver return the column's size depending on the charset and its collation. 
As the real data in a MySQL table can sometimes exceed the size that we get from the JDBC driver, we set the size for all TEXT columns to 65535 characters.  

If you need to use currently unsupported data types or find a way around known limitations, please, create a github issue in the [VS repository](https://github.com/exasol/virtual-schemas/issues).

## Testing information

In the following matrix you find combinations of JDBC driver and dialect version that we tested.

Virtual Schema Version| Big Query Version   | Driver Name              | Driver Version 
----------------------|---------------------|--------------------------|------------------------
 3.1.1                | MySQL 8.0.19        | MySQL Connector          |  8.0.17 