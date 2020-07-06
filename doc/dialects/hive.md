# Hive SQL Dialect

[The Apache Hive](https://hive.apache.org/) data warehouse software facilitates reading, writing, and managing datasets residing in distributed storage using SQL.

## Registering the JDBC Driver in EXAOperation

First download the [Hive JDBC driver](https://www.cloudera.com/downloads/connectors/hive/jdbc/2-6-10.html).

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
| Name      | `HIVE`                                              |
| Main      | `com.cloudera.hive.jdbc41.HS2Driver`                |
| Prefix    | `jdbc:hive2:`                                       |
| Files     | `HiveJDBC41.jar`                                    |

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
  %jar /buckets/<BFS service>/<bucket>/jars/virtual-schema-dist-5.0.1-bundle-4.0.1.jar;
  %jar /buckets/<BFS service>/<bucket>/jars/HiveJDBC41.jar;
/
```

## Defining a Named Connection

Define the connection to Hive as shown below. 

```sql
CREATE OR REPLACE CONNECTION HIVE_CONNECTION 
TO 'jdbc:hive2://<Hive host>:<port>' 
USER '<user>' 
IDENTIFIED BY '<password>';
```

## Creating a Virtual Schema

Below you see how a Hive Virtual Schema is created. Please note that you have to provide the name of a schema.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name> 
    USING ADAPTER.JDBC_ADAPTER 
    WITH
    SQL_DIALECT     = 'HIVE'
    CONNECTION_NAME = 'HIVE_CONN'
    SCHEMA_NAME     = '<schema name>';
```

### Connecting To a Kerberos Secured Hadoop:

Connecting to a Kerberos secured Hive service only differs in one aspect: You have a `CONNECTION` object which contains all the relevant information for the Kerberos authentication. This section describes how Kerberos authentication works and how to create such a `CONNECTION`.

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
python tools/create_kerberos_conn.py krb_conn krbuser@EXAMPLE.COM /etc/krb5.conf ./krbuser.keytab
```

This outputs a create connection statement:

```sql
CREATE CONNECTION krb_conn TO '' USER 'krbuser@EXAMPLE.COM' IDENTIFIED BY 'ExaAuthType=Kerberos;enp6Cg==;YWFhCg=='
```

However, we should update it and add the JDBC connection URL to the `TO` part of the connection string.

#### Using Thrift protocol with Kerberos

Add the JDBC connection URL to the `TO` part of the connection string:

```sql
CREATE OR REPLACE CONNECTION krb_conn
TO 'jdbc:hive2://<Hive host>:<port>;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=hive-host.example.com;KrbServiceName=hive'
USER 'krbuser@EXAMPLE.COM'
IDENTIFIED BY 'ExaAuthType=Kerberos;enp6Cg==;YWFhCg=='
```

#### Using HTTP protocol with Kerberos

Similar to the thrift protocol, update the `TO` part of the connection string with HTTP enabled URL:

```sql
CREATE OR REPLACE CONNECTION krb_conn
TO 'jdbc:hive2://<Hive host>:<port>;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=hive-host.example.com;KrbServiceName=hive;transportMode=http;httpPath=cliservice'
USER 'krbuser@EXAMPLE.COM'
IDENTIFIED BY 'ExaAuthType=Kerberos;enp6Cg==;YWFhCg=='
```

### Creating the Connection

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
   SQL_DIALECT     = 'HIVE'
   CONNECTION_NAME = 'KRB_CONN'
   SCHEMA_NAME     = '<schema name>';
```

## Troubleshooting

### VARCHAR Columns Size Fixed at 255 Characters

Hive is operating on schemaless data. Virtual Schemas &mdash; as the name suggests &mdash; require a schema. In the case of string variables this creates the situation that the Hive JDBC driver cannot tell how long strings are when asked for schema data. To achieve this it would have to scan all data first, which is not an option.

Instead the Driver simply reports a configurable fixed value as the maximum string size. By default this is 255 characters. In the Virtual Schema this is mapped to VARCHAR(255) columns.

If you need larger strings, you have to override the default setting of the Hive JDBC driver by adding the following parameter to the JDBC connection string when creating the Virtual Schema:

```
DefaultStringColumnLength=<length>
```

So an example for a connection string would look like:

```
jdbc:hive2://localhost:10000;DefaultStringColumnLength=32767;
```

Please also note that 32KiB are the maximum string size the driver accepts.

See also:

* [Cloudera JDBC driver for Apache Hive Install Guide](https://docs.cloudera.com/documentation/other/connectors/hive-jdbc/latest/Cloudera-JDBC-Driver-for-Apache-Hive-Install-Guide.pdf)

### No metadata is returned

If no Virtual Schema is created and see warning message in the log output:

```
2020-06-12 06:53:12.309 WARNING [c.e.a.j.BaseTableMetadataReader] Table scan did not find any tables. This can mean that either a) the source does not contain tables (yet), b) the table type is not supported or c) the table scan filter criteria is incorrect. Please check that the source actually contains tables.  Also check the spelling and exact case of any catalog or schema name you provided.
```

Please ensure that the user specified in the connection string has correct Hive access privileges in addition to above suggestions. Otherwise, the Virtual Schema may not return any metadata.

### Kerberos Exception: Invalid status error

If you see the error message similar to below:

```
2020-05-19 13:56:54.573 FINE    [c.e.a.RequestDispatcher] Stack trace:
        com.exasol.adapter.jdbc.RemoteMetadataReaderException: Unable to create Hive remote metadata reader.
        at com.exasol.adapter.dialects.hive.HiveSqlDialect.createRemoteMetadataReader(HiveSqlDialect.java:126)
        at com.exasol.adapter.dialects.AbstractSqlDialect.readSchemaMetadata(AbstractSqlDialect.java:138)
        at com.exasol.adapter.jdbc.JdbcAdapter.readMetadata(JdbcAdapter.java:56)
        at com.exasol.adapter.jdbc.JdbcAdapter.createVirtualSchema(JdbcAdapter.java:33)
        at com.exasol.adapter.RequestDispatcher.dispatchCreateVirtualSchemaRequestToAdapter(RequestDispatcher.java:110)
        at com.exasol.adapter.RequestDispatcher.processRequest(RequestDispatcher.java:70)
        at com.exasol.adapter.RequestDispatcher.executeAdapterCall(RequestDispatcher.java:52)
        at com.exasol.adapter.RequestDispatcher.adapterCall(RequestDispatcher.java:41)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.base/java.lang.reflect.Method.invoke(Method.java:566)
        at com.exasol.ExaWrapper.runSingleCall(ExaWrapper.java:95)
Caused by: java.sql.SQLException: Could not open client transport with JDBC Uri: <JDBC_URL>: Invalid status 72
        at org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:333)
        at org.apache.hive.jdbc.HiveDriver.connect(HiveDriver.java:107)
        at java.sql/java.sql.DriverManager.getConnection(DriverManager.java:677)
        at java.sql/java.sql.DriverManager.getConnection(DriverManager.java:189)
        ... 12 more
Caused by: org.apache.hive.org.apache.thrift.transport.TTransportException: Invalid status 72
        at org.apache.hive.org.apache.thrift.transport.TSaslTransport.sendAndThrowMessage(TSaslTransport.java:232)
        at org.apache.hive.org.apache.thrift.transport.TSaslTransport.receiveSaslMessage(TSaslTransport.java:184)
        at org.apache.hive.org.apache.thrift.transport.TSaslTransport.open(TSaslTransport.java:307)
        at org.apache.hive.org.apache.thrift.transport.TSaslClientTransport.open(TSaslClientTransport.java:37)
        at org.apache.hive.jdbc.HiveConnection.openTransport(HiveConnection.java:420)
        at org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:301)
        ... 19 more
```

It means you have specified the Thrift protocol, and it cannot establish connection using Thrift.

Check if the Hive cluster accepts a thrift connection. Or if the HTTP protocol enabled, then change the JDBC connection string as specified for HTTP protocol.

### Kerberos Exception: GSS initiate failed

The GSS initiate failure exception as below:

```
2020-05-25 14:44:02.707 FINE [c.e.a.RequestDispatcher] Stack trace:
com.exasol.adapter.jdbc.RemoteMetadataReaderException: Unable to create Hive remote metadata reader.
at com.exasol.adapter.dialects.hive.HiveSqlDialect.createRemoteMetadataReader(HiveSqlDialect.java:126)
at com.exasol.adapter.dialects.AbstractSqlDialect.readSchemaMetadata(AbstractSqlDialect.java:138)
at com.exasol.adapter.jdbc.JdbcAdapter.readMetadata(JdbcAdapter.java:56)
at com.exasol.adapter.jdbc.JdbcAdapter.createVirtualSchema(JdbcAdapter.java:33)
at com.exasol.adapter.RequestDispatcher.dispatchCreateVirtualSchemaRequestToAdapter(RequestDispatcher.java:110)
at com.exasol.adapter.RequestDispatcher.processRequest(RequestDispatcher.java:70)
at com.exasol.adapter.RequestDispatcher.executeAdapterCall(RequestDispatcher.java:52)
at com.exasol.adapter.RequestDispatcher.adapterCall(RequestDispatcher.java:41)
at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
at java.base/java.lang.reflect.Method.invoke(Method.java:566)
at com.exasol.ExaWrapper.runSingleCall(ExaWrapper.java:95)
Caused by: java.sql.SQLException: [Cloudera][HiveJDBCDriver](500164) Error initialized or created transport for authentication: [Cloudera][HiveJDBCDriver](500169) Unable to connect to server: GSS initiate failed.
at com.cloudera.hiveserver2.hivecommon.api.HiveServer2ClientFactory.createTransport(Unknown Source)
at com.cloudera.hiveserver2.hivecommon.api.ServiceDiscoveryFactory.createClient(Unknown Source)
at com.cloudera.hiveserver2.hivecommon.core.HiveJDBCCommonConnection.establishConnection(Unknown Source)
at com.cloudera.hiveserver2.jdbc.core.LoginTimeoutConnection.connect(Unknown Source)
at com.cloudera.hiveserver2.jdbc.common.BaseConnectionFactory.doConnect(Unknown Source)
at com.cloudera.hiveserver2.jdbc.common.AbstractDriver.connect(Unknown Source)
at java.sql/java.sql.DriverManager.getConnection(DriverManager.java:677)
at java.sql/java.sql.DriverManager.getConnection(DriverManager.java:189)
at com.exasol.adapter.jdbc.RemoteConnectionFactory.establishConnectionWithKerberos(RemoteConnectionFactory.java:74)
at com.exasol.adapter.jdbc.RemoteConnectionFactory.createConnection(RemoteConnectionFactory.java:55)
at com.exasol.adapter.jdbc.RemoteConnectionFactory.getConnection(RemoteConnectionFactory.java:38)
at com.exasol.adapter.dialects.hive.HiveSqlDialect.createRemoteMetadataReader(HiveSqlDialect.java:124)
at com.exasol.adapter.dialects.AbstractSqlDialect.readSchemaMetadata(AbstractSqlDialect.java:138)
at com.exasol.adapter.jdbc.JdbcAdapter.readMetadata(JdbcAdapter.java:56)
at com.exasol.adapter.jdbc.JdbcAdapter.createVirtualSchema(JdbcAdapter.java:33)
at com.exasol.adapter.RequestDispatcher.dispatchCreateVirtualSchemaRequestToAdapter(RequestDispatcher.java:110)
at com.exasol.adapter.RequestDispatcher.processRequest(RequestDispatcher.java:70)
at com.exasol.adapter.RequestDispatcher.executeAdapterCall(RequestDispatcher.java:52)
at com.exasol.adapter.RequestDispatcher.adapterCall(RequestDispatcher.java:41)
at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
at java.base/java.lang.reflect.Method.invoke(Method.java:566)
... 23 more
```

This error happens when the internal Hive host is unreachable. Please check and ensure that the Hive host specified using `KrbHostFQDN` parameter is correct.

## Testing information

The dialect was tested with the Cloudera Hive JDBC driver available on the [Cloudera downloads page](http://www.cloudera.com/downloads). The driver is also available directly from [Simba technologies](http://www.simba.com/), who developed the driver.
We tested with the JDBC 4.1.

## Type Mappings

-`DECIMAL with precision > 36` is casted to `VARCHAR` to prevent a loss of precision. 

    If you want to return a DECIMAL type you can set the property HIVE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE: 
    
    `HIVE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE='36,20'` 
    
    This will cast DECIMAL with precision > 36, DECIMAL without precision to DECIMAL(36,20).
    Keep in mind that this will yield errors if the data in the Hive database does not fit into the specified DECIMAL type.
