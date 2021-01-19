# Aurora SQL Dialect

Amazon's [AWS Aurora](https://aws.amazon.com/rds/aurora/) is a relational database offered as a managed service.

Its client is compatible with

1. [MariaDB](https://mariadb.org/) / [MySQL](https://www.mysql.com/)
1. [PostgreSQL](https://www.postgresql.org/)

Please note that you need to decide, which compatibility you prefer at the point when you create your RDS instance of Aurora.

## Using the PostgreSQL SQL Dialect to Connect to Aurora

In this section we are discussing how to used the PostgreSQL dialect of the Virtual Schemas to connect to an Aurora service.

### Registering the JDBC Driver in EXAOperation

First download the [Postgres JDBC driver](https://jdbc.postgresql.org/download.html).

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
| Name      | `POSTGRESQL`                                        |
| Main      | `org.postgresql.Driver`                             |
| Prefix    | `jdbc:postgresql:`                                  |
| Files     | `postgresql-<JDBC driver version>.jar`              |

Please refer to the [documentation on configuring JDBC connections to Aurora (PostgreSQL)](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Connecting.html#Aurora.Connecting.AuroraPostgreSQL) for details.

For TLS secured connections use the JDBC driver version **42.2.6 or later**.

### Uploading the JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
1. Upload the driver to BucketFS

This step is necessary since the UDF container the adapter runs in has no access to the JDBC drivers installed via EXAOperation but it can access BucketFS.

### Installing the Adapter Script

Upload the latest available release of [PostgreSQL Virtual Schema](https://github.com/exasol/postgresql-virtual-schema/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/<BFS service>/<bucket>/virtual-schema-dist-8.0.0-posgresql-1.0.0.jar;
    %jar /buckets/<BFS service>/<bucket>/postgresql-<JDBC driver version>.jar;
/
```

### Defining a Named Connection

Define the connection to the Aurora cluster as shown below. We recommend using TLS to secure the connection.

```sql
CREATE OR REPLACE CONNECTION AURORA_CONNECTION
TO 'jdbc:postgresql://<cluster>.<region>.rds.amazonaws.com/<database>?ssl=true&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory'
USER '<user>'
IDENTIFIED BY '<password>';
```

The parameters `user` and `password` are regular database credentials.

You need to set the property `ssl=true` to switch SSL on, including full certificate chain checking. To use the default Java certificate store, set the property `sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory`. Both settings are essential if you want to successfully establish the secured connection in a way that also works with Virtual Schemas.

You can find out the connection URL including the `cluster` and `region` part in the RDS console.

1. Open the RDS console
1. Select your database instance in the list
1. Switch to the tab "Connectivity & security"
1. Copy the hostname under "Endpoint"

### Installing the RDS Certificate Bundle

In order to successfully establish a TLS connection to Aurora, you also need to install the RDS certificates bundle on each data node in the cluster.

The following script installs that bundle on an Exasol node. You need log into the node as `root` via SSH to execute it.

```bash
#!/bin/bash
cd /tmp
wget https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem
openssl x509 -outform der -in rds-combined-ca-bundle.pem -out rds-combined-ca-bundle.der
keytool -keystore /etc/pki/ca-trust/extracted/java/cacerts -import -file rds-combined-ca-bundle.der -alias awsrds
```

The script first switches to the `/tmp` directory, then downloads the drivers from the Internet. After that it converts the certificates from `PEM` to `DER` format in order to be able to install them into the Java truststore.

If your cluster does not have an Internet connection, you have to download the certificate bundle to your machine, upload it to the cluster node via SSH and then install it in the truststore.

### Creating a Virtual Schema

Below you see the SQL statement that creates a Virtual Schema in order to connect to the remote data source. 

```sql
CREATE VIRTUAL SCHEMA AURORA
    USING ADAPTER.JDBC_ADAPTER 
    WITH
    SQL_DIALECT = 'POSTGRESQL'
    SCHEMA_NAME = '<schema>'
    CONNECTION_NAME = 'AURORA_CONNECTION'
    ;
```

The default schema is called `public`.

### PostgreSQL-specifics

Since this method of connecting uses the PostgreSQL database driver and SQL dialect, please check the [documentation of the PostgreSQL SQL dialect](postgresql.md) for details on type conversion and other PostgreSQL-specifics.
