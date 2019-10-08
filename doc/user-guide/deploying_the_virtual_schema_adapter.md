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

The resulting fat JAR is stored in `virtualschema-jdbc-adapter-dist/target/virtualschema-jdbc-adapter-dist-2.2.0.jar`.

## Uploading the Adapter JAR Archive

You have to upload the JAR of the adapter to a bucket of your choice in the Exasol bucket file system (BucketFS). This will allow using the jar in the adapter script.

Following steps are required to upload a file to a bucket:

1. Make sure you have a bucket file system (BucketFS) and you know the port for either HTTP or HTTPS.

   This can be done in EXAOperation under "EXABuckets". E.g. the ID could be `bucketfs1`. The recommended default HTTP port is 2580, but any unused TCP port will work. As a best practice choose an unprivileged port (1024 or higher).

1. Create a bucket in the BucketFS: 

    1. Click on the name of the BucketFS in EXAOperation and add a bucket there, e.g. `bucket1`.
    
    1. Set the write password.
    
    1. To keep this example simple we assume that the bucket is defined publicly readable.

1. Now upload the file into this bucket, e.g. using curl (adapt the hostname, BucketFS port, bucket name and bucket write password).

```bash
curl -X PUT -T virtualschema-jdbc-adapter-dist/target/virtualschema-jdbc-adapter-dist-2.2.0.jar \
 http://w:write-password@your.exasol.host.com:2580/bucket1/virtualschema-jdbc-adapter-dist-2.2.0.jar
```

If you later need to change the bucket passwords, select the bucket and click "Edit".

See chapter 3.6.4. "The synchronous cluster file system BucketFS" in the EXASolution User Manual for more details about BucketFS.

Check out Exasol's [BucketFS Explorer](https://github.com/exasol/bucketfs-explorer) as an alternative means of uploading your JAR archive.

## Deploying JDBC Driver Files

You have to upload the JDBC driver files of your remote database **twice**:

* Upload all files of the JDBC driver into a bucket of your choice, so that they can be accessed from the adapter script.
  This happens the same way as described above for the adapter JAR. You can use the same bucket.
* Upload all files of the JDBC driver as a JDBC driver in EXAOperation
  - In EXAOperation go to Software -> JDBC Drivers
  - Add the JDBC driver by specifying the JDBC main class and the prefix of the JDBC connection string
  - Upload all files (one by one) to the specific JDBC to the newly added JDBC driver.

Note that some JDBC drivers consist of several files and that you have to upload all of them. To find out which JAR you need, consult the [user guide page](user_guide.md).

## Deploying the Adapter Script

Create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
CREATE SCHEMA ADAPTER;

CREATE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/your-bucket-fs/your-bucket/virtualschema-jdbc-adapter-dist-2.2.0.jar;
  %jar /buckets/your-bucket-fs/your-bucket/<JDBC driver>.jar;
/
```

The [user guide page](user_guide.md) has example statements for the individual dialects.