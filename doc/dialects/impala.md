# Impala SQL Dialect

The Impala dialect is similar to the Hive dialect in most aspects. For this reason we only highlight the differences in this section.

## JDBC Driver

You have to specify the following settings when adding the JDBC driver via EXAOperation:

* Name: `Impala`
* Main: `com.cloudera.impala.jdbc41.Driver`
* Prefix: `jdbc:impala:`

Make sure you upload **all files** of the JDBC driver (over 10 at the time of writing) in EXAOperation and to the bucket.

## Adapter script

The adapter can be created similar to Hive:

```sql

CREATE SCHEMA adapter;
CREATE  JAVA  ADAPTER SCRIPT jdbc_adapter AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<buckets>/jars/virtualschema-jdbc-adapter-dist-1.19.1.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/hive_metastore.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/hive_service.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/ImpalaJDBC41.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/libfb303-0.9.0.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/libthrift-0.9.0.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/log4j-1.2.14.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/ql.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/slf4j-api-1.5.11.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/slf4j-log4j12-1.5.11.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/TCLIServiceClient.jar;
  %jar /buckets/<BFS service>/<buckets>/jars/zookeeper-3.4.6.jar;
/
```

## Creating a Virtual Schema

You can now create a virtual schema as follows:

```sql
CREATE CONNECTION impala_conn TO 'jdbc:impala://impala-host:21050' USER 'impala-usr' IDENTIFIED BY 'impala-pwd';

CREATE VIRTUAL SCHEMA impala_default USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'IMPALA'
  CONNECTION_NAME = 'IMPALA_CONN'
  SCHEMA_NAME     = 'default';
```

Connecting to a Kerberos secured Impala works similar as for Hive and is described in the section [Connecting To a Kerberos Secured Hadoop](hive.md#connecting-to-a-kerberos-secured-hadoop).