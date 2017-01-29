# Supported Dialects

The purpose of this page is to provide detailed instructions on the individual dialects.

## Table of Contents

1. [EXASOL](#exasol)
2. [Hive](#hive)
    - [Connecting to a Kerberos secured Hadoop](#connecting-to-a-kerberos-secured-hadoop)
3. [Impala](#impala)
5. [Oracle](#oracle)
6. [Teradata](#teradata)
7. [Redshirt](#redshift)
8. [Generic](#generic)

## EXASOL

**Supported capabilities**:
The EXASOL SQL dialect supports all capabilities that are supported by the virtual schema framework.

**JDBC driver**:
Connecting to an EXASOL database is the simplest way to start with virtual schemas.
You don't have to install any JDBC driver, because it is already installed in the EXASOL database and also included in the jar of the adapter.

**Get started**:
All you have to do is uploading the adapter jar to a bucket and create the adapter script:
```sql
CREATE SCHEMA adapter;
CREATE JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;
  %jar /buckets/your-bucket-fs/your-bucket/virtualschema-jdbc-adapter-dist-0.0.1-SNAPSHOT.jar;
/
```
Then you can create a virtual schema:
```sql
CREATE CONNECTION exasol_conn TO 'jdbc:exa:exasol-host:1234' USER 'user' IDENTIFIED BY 'pwd';

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
  EXA_CONNECTION_STRING = 'exasol-host:1234';
```

## Hive

**JDBC driver**:
The dialect was tested with the Cloudera Hive JDBC driver available on the [Cloudera downloads page](http://www.cloudera.com/downloads). The driver is also available directly from [Simba technologies](http://www.simba.com/), who developed the driver.

When you unpack the JDBC driver archive you will see that there are two variants, JDBC 4.0 and 4.1. We tested with the JDBC 4.1 variant.

You have to specify the following settings when adding the JDBC driver via EXAOperation:
* Name: ```Hive```
* Main: ```com.cloudera.hive.jdbc41.HS2Driver```
* Prefix: ```jdbc:hive2:```

Make sure you upload **all files** of the JDBC driver (over 10 at the time of writing) in EXAOperation and to the bucket.

**Get started**:
You have to add all files of the JDBC driver to the classpath using %jar as follows (filenames may vary):
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
Then you can create a virtual schema:
```sql
CREATE CONNECTION hive_conn TO 'jdbc:hive2://hive-host:10000' USER 'hive-usr' IDENTIFIED BY 'hive-pwd';

CREATE VIRTUAL SCHEMA hive_default USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'HIVE'
  CONNECTION_NAME = 'HIVE_CONN'
  SCHEMA_NAME     = 'default';
```

### Connecting to a Kerberos secured Hadoop:
To Be Done.

## Impala

The Impala dialect is similar to the Hive dialect in most aspects. For this reason we only highlight the differences in this section.

**JDBC driver:**
The dialect was tested with the Cloudera Impala JDBC driver, which is comparable to the Hive driver in most aspects.

You have to specify the following settings when adding the JDBC driver via EXAOperation:
* Name: ```Hive```
* Main: ```com.cloudera.impala.jdbc41.Driver```
* Prefix: ```jdbc:impala:```

Make sure you upload **all files** of the JDBC driver (over 10 at the time of writing) in EXAOperation and to the bucket.

**Getting started**:
The adapter can be created similar to Hive, by adapting only the filenames of the JDBC driver.

You can create a virtual schema as follows:
```sql
CREATE CONNECTION impala_conn TO 'jdbc:impala://impala-host:21050' USER 'impala-usr' IDENTIFIED BY 'impala-pwd';

CREATE VIRTUAL SCHEMA impala_default USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'IMPALA'
  CONNECTION_NAME = 'IMPALA_CONN'
  SCHEMA_NAME     = 'default';
```


## Oracle

## Teradata

## Redshift

## Generic
