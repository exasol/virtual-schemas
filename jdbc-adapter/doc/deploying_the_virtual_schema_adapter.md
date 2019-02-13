# Deploying the Adapter Step By Step

Run the following steps to deploy your adapter:

## Prerequisites

* Exasol Version 6.0 or later
* Advanced edition (which includes the ability to execute adapter scripts), or Free Small Business Edition
* Exasol must be able to connect to the host and port specified in the JDBC connection string. In case of problems you can use a [UDF to test the connectivity](https://www.exasol.com/support/browse/SOL-307).
* If the JDBC driver requires Kerberos authentication (e.g. for Hive or Impala), the Exasol database will authenticate using a keytab file. Each Exasol node needs access to port 88 of the the Kerberos KDC (key distribution center).

## Obtaining JAR Archives

First you have to obtain the so called fat JAR (including all dependencies).

The easiest way is to download the JAR from the last [Release](https://github.com/Exasol/virtual-schemas/releases).

Alternatively you can clone the repository and build the JAR as follows:

```bash
git clone https://github.com/Exasol/virtual-schemas.git
cd virtual-schemas/jdbc-adapter/
mvn clean -DskipTests package
```

The resulting fat JAR is stored in `virtualschema-jdbc-adapter-dist/target/virtualschema-jdbc-adapter-dist-1.4.0.jar`.

## Uploading the Adapter JAR Archive

You have to upload the JAR of the adapter to a bucket of your choice in the Exasol bucket file system (BucketFS). This will allow using the jar in the adapter script.

Following steps are required to upload a file to a bucket:

1. Make sure you have a bucket file system (BucketFS) and you know the port for either HTTP or HTTPS.

   This can be done in EXAOperation under "EXABuckets". E.g. the id could be `bucketfs1` and the HTTP port 2580.
  
1. Check if you have a bucket in the BucketFS. Simply click on the name of the BucketFS in EXAOperation and add a bucket there, e.g. `bucket1`.

   Also make sure you know the write password. For simplicity we assume that the bucket is defined as a public bucket, i.e. it can be read by any script.
  
1. Now upload the file into this bucket, e.g. using curl (adapt the hostname, BucketFS port, bucket name and bucket write password).

```bash
curl -X PUT -T virtualschema-jdbc-adapter-dist/target/virtualschema-jdbc-adapter-dist-1.4.0.jar \
 http://w:write-password@your.exasol.host.com:2580/bucket1/virtualschema-jdbc-adapter-dist-1.4.0.jar
```

See chapter 3.6.4. "The synchronous cluster file system BucketFS" in the EXASolution User Manual for more details about BucketFS.

## Deploying JDBC Driver Files

You have to upload the JDBC driver files of your remote database **twice**:

* Upload all files of the JDBC driver into a bucket of your choice, so that they can be accessed from the adapter script.
  This happens the same way as described above for the adapter JAR. You can use the same bucket.
* Upload all files of the JDBC driver as a JDBC driver in EXAOperation
  - In EXAOperation go to Software -> JDBC Drivers
  - Add the JDBC driver by specifying the JDBC main class and the prefix of the JDBC connection string
  - Upload all files (one by one) to the specific JDBC to the newly added JDBC driver.

Note that some JDBC drivers consist of several files and that you have to upload all of them. To find out which JAR you need, consult the [supported dialects page](supported_sql_dialects.md).

## Deploying the Adapter Script

Then run the following SQL commands to deploy the adapter in the database:

```sql
-- The adapter is simply a script. It has to be stored in any regular schema.
CREATE SCHEMA adapter;
CREATE JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS

  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/your-bucket-fs/your-bucket/virtualschema-jdbc-adapter-dist-1.4.0.jar;

  // You have to add all files of the data source jdbc driver here (e.g. Hive JDBC driver files)
  %jar /buckets/your-bucket-fs/your-bucket/name-of-data-source-jdbc-driver.jar;
/
```

The [supported dialects page](supported-dialects.md) has example statements for the individual dialects.
