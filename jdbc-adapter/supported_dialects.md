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

**JDBC Driver Settings**
* Name: Hive
* Main: com.cloudera.hive.jdbc41.HS2Driver
* Prefix: jdbc:hive2:

**Create Adapter script**
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


## Impala

## Oracle

## Teradata

## Redshift

## Generic
