# Supported Dialects

## Table of Contents

1. [EXASOL](#exasol)
2. [Hive](#hive)
3. [Impala](#impala)
4. [Oracle](#oracle)
5. [Teradata](#teradata)
6. [Redshirt](#redshift)
7. [Generic](#generic)

## EXASOL

## Hive

**Which JDBC driver?**
The dialect was tested with the Cloudera Hive JDBC driver available on the [Cloudera downloads page](http://www.cloudera.com/downloads). The driver is also available from [Simba technologies](http://www.simba.com/), the developers of the JDBC driver.

**JDBC Driver Settings for EXAOperation**
* Name: Hive
* Main: com.cloudera.hive.jdbc41.HS2Driver
* Prefix: jdbc:hive2:

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
