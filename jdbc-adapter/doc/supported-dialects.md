# Supported Dialects

The purpose of this page is to provide detailed instructions 

## Table of Contents

1. [EXASOL](#exasol)
2. [Hive](#hive)
3. [Impala](#impala)
5. [Oracle](#oracle)
6. [Teradata](#teradata)
7. [Redshirt](#redshift)
8. [Generic](#generic)

## EXASOL

**Supported Capabilities**
The EXASOL SQL dialect supports all capabilities that are supported by the virtual schema framework.

**JDBC Driver**
Connecting to an EXASOL database is the simplest way to start with virtual schemas.
You don't have to install any JDBC driver, because it is already installed in the EXASOL database and also included in the jar of the adapter.

**Get Started**
All you have to do is uploading the adapter jar to a bucket and create the adapter script:
```sql
CREATE SCHEMA adapter;
CREATE JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;
  %jar /buckets/your-bucket-fs/your-bucket/virtualschema-jdbc-adapter-dist-0.0.1-SNAPSHOT.jar;
/
```
Then you can create a virtual schema
```sql
CREATE CONNECTION exasol_conn TO 'jdbc:exa:localhost:5555' USER 'user' IDENTIFIED BY 'pwd';

CREATE VIRTUAL SCHEMA virtual_exasol USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'EXASOL'
  CONNECTION_NAME = 'EXASOL_CONN'
  SCHEMA_NAME     = 'default';
```

EXASOL provides the faster ```IMPORT FROM EXA``` command for loading data from EXASOL. You can tell the adapter to use this command instead of ```IMPORT FROM JDBC``` by setting the ```IMPORT_FROM_EXA``` property:
```sql
CREATE VIRTUAL SCHEMA virtual_exasol USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'EXASOL'
  CONNECTION_NAME = 'EXASOL_CONN'
  SCHEMA_NAME     = 'default'
  IMPORT_FROM_EXA = 'true'
  EXA_CONNECTION_STRING = 'exa-host:1234';
```

## Hive

**JDBC Driver**
The dialect was tested with the Cloudera Hive JDBC driver available on the [Cloudera downloads page](http://www.cloudera.com/downloads). The driver is also available directly from [Simba technologies](http://www.simba.com/), who developed the driver.

You have to specify the following settings when adding the JDBC driver via EXAOperation:
* Name: Hive
* Main: com.cloudera.hive.jdbc41.HS2Driver
* Prefix: ```jdbc:hive2:```

**Create Adapter script**
You have to add all files of the JDBC driver to the classpath using %jar as follows:
```sql
CREATE SCHEMA adapter;
CREATE  JAVA  ADAPTER SCRIPT jdbc_adapter AS
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-0.0.1-SNAPSHOT.jar;
  %jar /buckets/bucketfs1/bucket1/hive_metastore.jar;
  %jar /buckets/bucketfs1/bucket1/hive_service.jar;
  %jar /buckets/bucketfs1/bucket1/HiveJDBC41.jar;
  %jar /buckets/bucketfs1/bucket1/libfb303-0.9.0.jar;
  %jar /buckets/bucketfs1/bucket1/libthrift-0.9.0.jar;
  %jar /buckets/bucketfs1/bucket1/log4j-1.2.14.jar;
  %jar /buckets/bucketfs1/bucket1/ql.jar;
  %jar /buckets/bucketfs1/bucket1/slf4j-api-1.5.11.jar;
  %jar /buckets/bucketfs1/bucket1/slf4j-log4j12-1.5.11.jar;
  %jar /buckets/bucketfs1/bucket1/TCLIServiceClient.jar;
  %jar /buckets/bucketfs1/bucket1/zookeeper-3.4.6.jar;
/
```

**Create Virtual Schema**
```sql
CREATE CONNECTION hive_conn TO 'jdbc:hive2://localhost:10000/default' USER 'hive-usr' IDENTIFIED BY 'hive-pwd';

CREATE VIRTUAL SCHEMA hive USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'HIVE'
  CONNECTION_NAME = 'HIVE_CONN'
  SCHEMA_NAME     = 'default';
```

## Impala

## Oracle

## Teradata

## Redshift

## Generic
