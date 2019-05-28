# Hive SQL Dialect

## JDBC Driver

The dialect was tested with the Cloudera Hive JDBC driver available on the [Cloudera downloads page](http://www.cloudera.com/downloads). The driver is also available directly from [Simba technologies](http://www.simba.com/), who developed the driver.

When you unpack the JDBC driver archive you will see that there are two variants, JDBC 4.0 and 4.1. We tested with the JDBC 4.1 variant.

You have to specify the following settings when adding the JDBC driver via EXAOperation:

* Name: `Hive`
* Main: `com.cloudera.hive.jdbc41.HS2Driver`
* Prefix: `jdbc:hive2:`

Make sure you upload **all files** of the JDBC driver (one at the time of writing) in EXAOperation **and** to the bucket.

## Adapter Script

You have to add all files of the JDBC driver to the classpath using `%jar` as follows (filenames may vary):

```sql
CREATE SCHEMA adapter;
CREATE  JAVA  ADAPTER SCRIPT jdbc_adapter AS
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.16.3.jar;

  %jar /buckets/bucketfs1/bucket1/HiveJDBC41.jar;
/
```

### Creating a Virtual Schema

```sql
CREATE CONNECTION hive_conn TO 'jdbc:hive2://hive-host:10000' USER 'hive-usr' IDENTIFIED BY 'hive-pwd';

CREATE VIRTUAL SCHEMA hive_default USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'HIVE'
  CONNECTION_NAME = 'HIVE_CONN'
  SCHEMA_NAME     = 'default';
```

### Connecting To a Kerberos Secured Hadoop:

Connecting to a Kerberos secured Impala or Hive service only differs in one aspect: You have to a `CONNECTION` object which contains all the relevant information for the Kerberos authentication. This section describes how Kerberos authentication works and how to create such a `CONNECTION`.

#### Understanding how it Works (Optional)

Both the adapter script and the internally used `IMPORT FROM JDBC` statement support Kerberos authentication. They detect, that the connection is a Kerberos connection by a special prefix in the `IDENTIFIED BY` field. In such case, the authentication will happen using a Kerberos keytab and Kerberos config file (using the JAAS Java API).

The `CONNECTION` object stores all relevant information and files in its fields:

* The `TO` field contains the JDBC connection string
* The `USER` field contains the Kerberos principal
* The `IDENTIFIED BY` field contains the Kerberos configuration file and keytab file (base64 encoded) along with an internal prefix `ExaAuthType=Kerberos;` to identify the `CONNECTION` as a Kerberos `CONNECTION`.

#### Generating the CREATE CONNECTION Statement

In order to simplify the creation of Kerberos `CONNECTION` objects, the [`create_kerberos_conn.py`](https://github.com/EXASOL/hadoop-etl-udfs/blob/master/tools/create_kerberos_conn.py) Python script has been provided. The script requires 5 arguments:

* `CONNECTION` name (arbitrary name for the new `CONNECTION`)
* Kerberos principal for Hadoop (i.e., Hadoop user)
* Kerberos configuration file path (e.g., `krb5.conf`)
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

#### Creating the CONNECTION
You have to execute the generated `CREATE CONNECTION` statement directly in EXASOL to actually create the Kerberos `CONNECTION` object. For more detailed information about the script, use the help option:

```sh
python tools/create_kerberos_conn.py -h
```

#### Using the Connection When Creating a Virtual Schema

You can now create a virtual schema using the Kerberos connection created before.

```sql
CREATE VIRTUAL SCHEMA hive_default USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'HIVE'
  CONNECTION_NAME = 'KRB_CONN'
  SCHEMA_NAME     = 'default';
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

* [Cloudera JDBC driver for Apache Hive Install Guide](https://www.cloudera.com/documentation/other/connectors/hive-jdbc/2-5-4/Cloudera-JDBC-Driver-for-Apache-Hive-Install-Guide-2-5-4.pdf)