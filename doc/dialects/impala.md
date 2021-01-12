# Impala SQL Dialect

[Impala](https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala.html) is a MPP (Massive Parallel Processing) SQL query engine for processing data that is stored on a Hadoop cluster.  

## Registering the JDBC Driver in EXAOperation

First download the [Impala JDBC driver](https://www.cloudera.com/downloads/connectors/impala/jdbc/2-6-4.html).

Now register the driver in EXAOperation:

1. Click "Software"
1. Switch to tab "JDBC Drivers"
1. Click "Browse..."
1. Select JDBC driver file
1. Click "Upload"
1. Click "Add"
1. In dialog "Add EXACluster JDBC driver" configure the JDBC driver (see below)

You need to specify the following settings when adding the JDBC driver via EXAOperation.

| Parameter | Value                                                   |
|-----------|---------------------------------------------------------|
| Name      | `IMPALA`                                                |
| Main      | `com.cloudera.impala.jdbc41.Driver`                     |
| Prefix    | `jdbc:impala:`                                          |
| Files     | `ImpalaJDBC41.jar`                                      |

## Uploading the JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
1. Upload the driver to BucketFS

This step is necessary since the UDF container the adapter runs in has no access to the JDBC drivers installed via EXAOperation but it can access BucketFS.

## Installing the Adapter Script

Upload the latest available release of [Virtual Schema JDBC Adapter](https://github.com/exasol/virtual-schemas/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<bucket>/virtual-schema-dist-8.0.0-bundle-6.0.0.jar;
  %jar /buckets/<BFS service>/<bucket>/ImpalaJDBC41.jar;
/
;
```

## Defining a Named Connection

Define the connection to Impala as shown below. 

```sql
CREATE OR REPLACE CONNECTION IMPALA_CONNECTION 
TO 'jdbc:impala://<Impala host>:<port>' 
USER '<user>' 
IDENTIFIED BY '<password>';
```    

## Creating a Virtual Schema

Below you see how an Impala Virtual Schema is created. Please note that you have to provide the name of a schema.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER 
    WITH
    SQL_DIALECT     = 'IMPALA'
    CONNECTION_NAME = 'IMPALA_CONNECTION'
    SCHEMA_NAME     = '<schema name>';
```

## Connecting To a Kerberos Secured Hadoop

Connecting to a Kerberos secured Impala service only differs in one aspect: You have a `CONNECTION` object which contains all the relevant information for the Kerberos authentication.

### Understanding how it Works (Optional)

Both the adapter script and the internally used `IMPORT FROM JDBC` statement support Kerberos authentication. They detect, that the connection is a Kerberos connection by a special prefix in the `IDENTIFIED BY` field. In such case, the authentication will happen using a Kerberos keytab and Kerberos config file (using the JAAS Java API).

The `CONNECTION` object stores all relevant information and files in its fields:

* The `TO` field contains the JDBC connection string
* The `USER` field contains the Kerberos principal
* The `IDENTIFIED BY` field contains the Kerberos configuration file and keytab file (base64 encoded) along with an internal prefix `ExaAuthType=Kerberos;` to identify the `CONNECTION` as a Kerberos `CONNECTION`.

### Generating the CREATE CONNECTION Statement

In order to simplify the creation of Kerberos `CONNECTION` objects, the [`create_kerberos_conn.py`](https://github.com/EXASOL/hadoop-etl-udfs/blob/master/tools/create_kerberos_conn.py) Python script has been provided. The script requires 5 arguments:

* `CONNECTION` name (arbitrary name for the new `CONNECTION`)
* Kerberos principal for Hadoop (i.e., Hadoop user)
* Kerberos configuration file path (e.g., `krb5.conf`)
* Kerberos keytab file path, which contains keys for the Kerberos principal
* JDBC connection string

Example command:

```
python tools/create_kerberos_conn.py krb_conn krbuser@EXAMPLE.COM /etc/krb5.conf ./krbuser.keytab \
  'jdbc:impala://<Impala host>:<port>;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=host.example.com;KrbServiceName=impala'
```

Output:

```sql
CREATE CONNECTION krb_conn TO 'jdbc:impala://<Impala host>:<port>;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=host.example.com;KrbServiceName=impala' USER 'krbuser@EXAMPLE.COM' IDENTIFIED BY 'ExaAuthType=Kerberos;enp6Cg==;YWFhCg=='
```

### Creating the connection

You have to execute the generated `CREATE CONNECTION` statement directly in EXASOL to actually create the Kerberos `CONNECTION` object. For more detailed information about the script, use the help option:

```sh
python tools/create_kerberos_conn.py -h
```

### Using the Connection When Creating a Virtual Schema

You can now create a virtual schema using the Kerberos connection created before.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name> 
   USING ADAPTER.JDBC_ADAPTER
   WITH
   SQL_DIALECT     = 'IMPALA'
   CONNECTION_NAME = 'KRB_CONN'
   SCHEMA_NAME     = '<schema name>';
```
