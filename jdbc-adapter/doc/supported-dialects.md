# Supported Dialects

The purpose of this page is to provide detailed instructions for each of the supported dialects on how to get started. Typical questions are
* Which **JDBC driver** is used, which files have to be uploaded and included when creating the adapter script.
* How does the **CREATE VIRTUAL SCHEMA** statement look like, i.e. which parameters are required.
* **Data source specific notes**, like authentication with Kerberos, supported capabilities or things to consider regarding the data type mapping.

As an entry point we recommend to follow the [step-by-step deployment guide](deploy-adapter.md) which will link to this page whenever needed.

## Table of Contents

1. [EXASOL](#exasol)
2. [Hive](#hive)
    - [Connecting To a Kerberos Secured Hadoop](#connecting-to-a-kerberos-secured-hadoop)
3. [Impala](#impala)
4. [DB2](#db2)
5. [Oracle](#oracle)
6. [Teradata](#teradata)
7. [Redshift](#redshift)
8. [SQL Server](#sql-server)
8. [PostgresSQL](#postgresql)
10. [Generic](#generic)

## EXASOL

**Supported capabilities**:
The EXASOL SQL dialect supports all capabilities that are supported by the virtual schema framework.

**JDBC driver**:
Connecting to an EXASOL database is the simplest way to start with virtual schemas.
You don't have to install any JDBC driver, because it is already installed in the EXASOL database and also included in the jar of the JDBC adapter.

**Adapter script**:
After uploading the adapter jar, the adapter script can be created as follows:
```sql
CREATE SCHEMA adapter;
CREATE JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;
  %jar /buckets/your-bucket-fs/your-bucket/virtualschema-jdbc-adapter-dist-1.0.1-SNAPSHOT.jar;
/
```
**Create a virtual schema:**

```sql
CREATE CONNECTION exasol_conn TO 'jdbc:exa:exasol-host:1234' USER 'user' IDENTIFIED BY 'pwd';

CREATE VIRTUAL SCHEMA virtual_exasol USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'EXASOL'
  CONNECTION_NAME = 'EXASOL_CONN'
  SCHEMA_NAME     = 'default';
```

**Use IMPORT FROM EXA instead of IMPORT FROM JDBC**

EXASOL provides the faster and parallel ```IMPORT FROM EXA``` command for loading data from EXASOL. You can tell the adapter to use this command instead of ```IMPORT FROM JDBC``` by setting the ```IMPORT_FROM_EXA``` property. In this case you have to provide the additional ```EXA_CONNECTION_STRING``` which is the connection string used for the internally used ```IMPORT FROM EXA``` command (it also supports ranges like ```192.168.6.11..14:8563```). Please note, that the ```CONNECTION``` object must still have the jdbc connection string in ```AT```, because the Adapter Script uses a JDBC connection to obtain the metadata when a schema is created or refreshed. For the internally used ```IMPORT FROM EXA``` statement, the address from ```EXA_CONNECTION_STRING``` and the username and password from the connection will be used.
```sql
CREATE CONNECTION exasol_conn TO 'jdbc:exa:exasol-host:1234' USER 'user' IDENTIFIED BY 'pwd';

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

Make sure you upload **all files** of the JDBC driver (over 10 at the time of writing) in EXAOperation **and** to the bucket.

**Adapter script**:
You have to add all files of the JDBC driver to the classpath using %jar as follows (filenames may vary):
```sql
CREATE SCHEMA adapter;
CREATE  JAVA  ADAPTER SCRIPT jdbc_adapter AS
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.0.1-SNAPSHOT.jar;

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
**Create a virtual schema:**
```sql
CREATE CONNECTION hive_conn TO 'jdbc:hive2://hive-host:10000' USER 'hive-usr' IDENTIFIED BY 'hive-pwd';

CREATE VIRTUAL SCHEMA hive_default USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'HIVE'
  CONNECTION_NAME = 'HIVE_CONN'
  SCHEMA_NAME     = 'default';
```

### Connecting To a Kerberos Secured Hadoop:

Connecting to a Kerberos secured Impala or Hive service only differs in one aspect: You have to a ```CONNECTION``` object which contains all the relevant information for the Kerberos authentication. This section describes how Kerberos authentication works and how to create such a ```CONNECTION```.

#### 0. Understand how it works (optional)
Both the adapter script and the internally used ```IMPORT FROM JDBC``` statement support Kerberos authentication. They detect, that the connection is a Kerberos connection by a special prefix in the ```IDENTIFIED BY``` field. In such case, the authentication will happen using a Kerberos keytab and Kerberos config file (using the JAAS Java API).

The ```CONNECTION``` object stores all relevant information and files in its fields:
* The ```TO``` field contains the JDBC connection string
* The ```USER``` field contains the Kerberos principal
* The ```IDENTIFIED BY``` field contains the Kerberos configuration file and keytab file (base64 encoded) along with an internal prefix ```ExaAuthType=Kerberos;``` to identify the CONNECTION as a Kerberos CONNECTION.

#### 1. Generate the CREATE CONNECTION statement
In order to simplify the creation of Kerberos CONNECTION objects, the [create_kerberos_conn.py](https://github.com/EXASOL/hadoop-etl-udfs/blob/master/tools/create_kerberos_conn.py) Python script has been provided. The script requires 5 arguments:
* CONNECTION name (arbitrary name for the new CONNECTION)
* Kerberos principal for Hadoop (i.e., Hadoop user)
* Kerberos configuration file path (e.g., krb5.conf)
* Kerberos keytab file path, which contains keys for the Kerberos principal
* JDBC connection string

Example command:
```
python tools/create_kerberos_conn.py krb_conn krbuser@EXAMPLE.COM /etc/krb5.conf ./krbuser.keytab \
  'jdbc:hive2://hive-host.example.com:10000;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=hive-host.example.com;KrbServiceName=hive'
```
Output:
```sql
CREATE CONNECTION krb_conn TO 'jdbc:hive2://hive-host.example.com:10000;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=hive-host.example.com;KrbServiceName=hive' USER 'krbuser@EXAMPLE.COM' IDENTIFIED BY 'ExaAuthType=Kerberos;enp6Cg==;YWFhCg=='
```

#### 2. Create the CONNECTION
You have to execute the generated CREATE CONNECTION statement directly in EXASOL to actually create the Kerberos CONNECTION object. For more detailed information about the script, use the help option:
```
python tools/create_kerberos_conn.py -h
```

#### 3. Use the connection when creating a virtual schema
You can now create a virtual schema using the Kerberos connection created before.
```sql
CREATE VIRTUAL SCHEMA hive_default USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'HIVE'
  CONNECTION_NAME = 'KRB_CONN'
  SCHEMA_NAME     = 'default';
```

## Impala

The Impala dialect is similar to the Hive dialect in most aspects. For this reason we only highlight the differences in this section.

**JDBC driver:**

You have to specify the following settings when adding the JDBC driver via EXAOperation:
* Name: ```Hive```
* Main: ```com.cloudera.impala.jdbc41.Driver```
* Prefix: ```jdbc:impala:```

Make sure you upload **all files** of the JDBC driver (over 10 at the time of writing) in EXAOperation and to the bucket.

**Adapter script**:
The adapter can be created similar to Hive:
```sql

CREATE SCHEMA adapter;
CREATE  JAVA  ADAPTER SCRIPT jdbc_adapter AS
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.0.1-SNAPSHOT.jar;

  %jar /buckets/bucketfs1/bucket1/hive_metastore.jar;
  %jar /buckets/bucketfs1/bucket1/hive_service.jar;
  %jar /buckets/bucketfs1/bucket1/ImpalaJDBC41.jar;
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

**Create a virtual schema:**
You can now create a virtual schema as follows:
```sql
CREATE CONNECTION impala_conn TO 'jdbc:impala://impala-host:21050' USER 'impala-usr' IDENTIFIED BY 'impala-pwd';

CREATE VIRTUAL SCHEMA impala_default USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'IMPALA'
  CONNECTION_NAME = 'IMPALA_CONN'
  SCHEMA_NAME     = 'default';
```

Connecting to a Kerberos secured Impala works similar as for Hive and is described in the section [Connecting To a Kerberos Secured Hadoop](#connecting-to-a-kerberos-secured-hadoop).

## DB2

DB2 was tested with the IBM DB2 JCC Drivers that come with DB2 LUW V10.1 and V11. As these drivers didn't have any major changes in the past years any DB2 driver should work (back to V9.1). The driver comes with 2 different implementations db2jcc.jar and db2jcc4.jar. All tests were made with the db2jcc4.jar.

Additionally there are 2 files for the DB2 Driver.
* db2jcc_license_cu.jar - License File for DB2 on Linux Unix and Windows
* db2jcc_license_cisuz.jar - License File for DB2 on zOS (Mainframe)

Make sure that you upload the necessary license file for the target platform you want to connect to. 

**Supported capabilities**:
The db2 dialect handles some casts in regards of time data types and functions.

Casting of Data Types
* TIMESTAMP and TIMESTAMP(x) will be cast to VARCHAR to not lose precision.
* VARCHAR and CHAR for bit data will be cast to a hex string with double the original size
* TIME will be cast to VARCHAR(8)
* XML will be cast to VARCHAR(DB2_MAX_LENGTH)
* BLOB is not supported

Casting of Functions
* LIMIT will replaced by FETCH FIRST x ROWS ONLY
* OFFESET is currently not supported as only DB2 V11 support this nativly
* ADD_DAYS, ADD_WEEKS ... will be replaced by COLUMN + DAYS, COLUMN + ....


**JDBC driver:**
You have to specify the following settings when adding the JDBC driver via EXAOperation:
* Name: ```DB2```
* Main: ```com.ibm.db2.jcc.DB2Driver```
* Prefix: ```jdbc:db2:```

**Adapter script**
```sql
CREATE or replace JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS

  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.0.1-SNAPSHOT.jar;

  // DB2 Driver files
  %jar /buckets/bucketfs1/bucket1/db2jcc4.jar;
  %jar /buckets/bucketfs1/bucket1/db2jcc_license_cu.jar;
  // uncomment for mainframe connection and upload  db2jcc_license_cisuz.jar;
  //%jar /buckets/bucketfs1/bucket1/db2jcc_license_cisuz.jar;
/
```

**Create a virtual schema**
You can now create a virtual schema as follows:
```sql
create or replace connection DB2_CON to 'jdbc:db2://host:port/database' user 'db2-usr' identified by 'db2-pwd';

create  virtual schema db2 using adapter.jdbc_adapter with
	SQL_DIALECT = 'DB2'
	CONNECTION_NAME = 'DB2_CON'
	SCHEMA_NAME = '<schemaname>'
;
```

```<schemaname>``` has to be replaced by the actual db2 schema you want to connect to.

**Running the DB2 integration tests**
A how to has been included in the [setup sql file](../integration-test-data/db2-testdata.sql)

## Oracle

## Teradata

**JDBC driver:**
You have to specify the following settings when adding the JDBC driver via EXAOperation:
* Name: ```TERADATA```
* Main: ```com.teradata.jdbc.TeraDriver```
* Prefix: ```jdbc:teradata:```
* Files: terajdbc4.jar, tdgssconfig.jar

Please also upload the jar files to a bucket for the adapter script.

**Adapter script**
```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter 
  AS
  
  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.0.1-SNAPSHOT.jar;
									 
  // You have to add all files of the data source jdbc driver here (e.g. MySQL or Hive)
  %jar /buckets/bucketfs1/bucket1/terajdbc4.jar;
  %jar /buckets/bucketfs1/bucket1/tdgssconfig.jar;

/
```

**Create a virtual schema**
```sql
CREATE VIRTUAL SCHEMA TERADATA_financial USING adapter.jdbc_adapter 
WITH
  SQL_DIALECT     = 'TERADATA'
  CONNECTION_NAME = 'TERADATA_CONNECTION'
  SCHEMA_NAME     = 'financial'
;
```

## Redshift

**JDBC driver:**

You have to specify the following settings when adding the JDBC driver via EXAOperation:
* Name: ```REDSHIFT```
* Main: ```com.amazon.redshift.jdbc.Driver```
* Prefix: ```jdbc:redshift:```
* Files: RedshiftJDBC42-1.2.1.1001.jar

Please also upload the driver jar into a bucket for the adapter script.

**Adapter script**
```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter 
  AS
  
  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.0.1-SNAPSHOT.jar;
									 
  // You have to add all files of the data source jdbc driver here (e.g. MySQL or Hive)

  %jar /buckets/bucketfs1/bucket1/RedshiftJDBC42-1.2.1.1001.jar;

/
```

**Create a virtual schema**
```sql
CREATE VIRTUAL SCHEMA redshift_tickit
	USING adapter.jdbc_adapter 
	WITH
	SQL_DIALECT = 'REDSHIFT'
	CONNECTION_NAME = 'REDSHIFT_CONNECTION'
	CATALOG_NAME = 'database_name'
	SCHEMA_NAME = 'public'
	;
```

## Sql Server

**JDBC driver:**
The Sql Server Dialect was tested with the jdts 1.3.1 JDBC driver and Sql Server 2014.
As the jdts driver is already preinstalled for the IMPORT command itself you only need
to upload the jdts.jar to a bucket for the adapter script.

**Adapter script**
```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.sql_server_jdbc_adapter 
  AS
  
  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.0.1-SNAPSHOT.jar;
									 
  // You have to add all files of the data source jdbc driver here 
  %jar /buckets/bucketfs1/bucket1/jtds.jar;
/
```

**Create a virtual schema**
```sql
CREATE VIRTUAL SCHEMA VS_SQLSERVER USING adapter.sql_server_jdbc_adapter
WITH
  SQL_DIALECT     = 'SQLSERVER'
  CONNECTION_NAME = 'SQLSERVER_CONNECTION'
  CATALOG_NAME	  =  'MyDatabase'
  SCHEMA_NAME     = 'dbo'
;
```

## PostgreSQL

**JDBC driver:**
The PostgreSQL dialect was tested with JDBC driver version 42.0.0 and PostgreSQL 9.6.2 .

**Adapter script**
```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter 
  AS
  
  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.0.1-SNAPSHOT.jar;
									 
  // You have to add all files of the data source jdbc driver here (e.g. MySQL or Hive)
  %jar /buckets/bucketfs1/bucket1/postgresql-42.0.0.jar;

/
```

**Create a virtual schema**
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

## Generic
