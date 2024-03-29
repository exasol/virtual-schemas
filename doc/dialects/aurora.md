# Aurora SQL Dialect

Amazon's [AWS Aurora](https://aws.amazon.com/rds/aurora/) is a relational database offered as a managed service.

Its client is compatible with

1. [MariaDB](https://mariadb.org/) / [MySQL](https://www.mysql.com/)
1. [PostgreSQL](https://www.postgresql.org/)

Please note that you need to decide, which compatibility you prefer at the point when you create your RDS instance of Aurora.

## Using the PostgreSQL SQL Dialect to Connect to Aurora

In this section we are discussing how to use the PostgreSQL dialect of the Virtual Schemas to connect to an Aurora service.

### Uploading the JDBC Driver to BucketFS

First download the [Postgres JDBC driver](https://jdbc.postgresql.org/download/).

For TLS secured connections use the JDBC driver version **42.2.6 or later**.

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
1. Upload the driver to BucketFS

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
  %jar /buckets/<BFS service>/<bucket>/virtual-schema-dist-<version>-postgresql-<version>.jar;
  %jar /buckets/<BFS service>/<bucket>/postgresql-<postgresql-driver-version>.jar;
/
```

### Defining a Named Connection

Define the connection to the Aurora cluster as shown below. We recommend using [TLS](#creating-tls-connection) to secure the connection.

```sql
CREATE OR REPLACE CONNECTION AURORA_CONNECTION
TO 'jdbc:postgresql://<cluster>.<region>.rds.amazonaws.com/<database>'
USER '<user>'
IDENTIFIED BY '<password>';
```

The parameters `user` and `password` are regular database credentials.

You can find out the connection URL including the `cluster` and `region` part in the RDS console.

1. Open the RDS console
1. Select your database instance in the list
1. Switch to the tab "Connectivity & security"
1. Copy the hostname under "Endpoint"

### Creating TLS Connection

You need to set the property `ssl=true` to switch SSL on, including full certificate chain checking.
To use the default Java certificate store, set the property `sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory`.
Both settings are essential if you want to successfully establish the secured connection in a way that also works with Virtual Schemas.

```sql
CREATE OR REPLACE CONNECTION AURORA_CONNECTION
TO 'jdbc:postgresql://<cluster>.<region>.rds.amazonaws.com/<database>?ssl=true&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory'
USER '<user>'
IDENTIFIED BY '<password>';
```

Please refer to our [tutorial](https://community.exasol.com/t5/tech-blog/aurora-virtualis-using-aws-aurora-with-exasol-s-virtual-schema/ba-p/321) to install Virtual Schema with TLS connection.

### Creating a Virtual Schema

Below you see the SQL statement that creates a Virtual Schema in order to connect to the remote data source. 

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
	USING ADAPTER.JDBC_ADAPTER 
	WITH
	SCHEMA_NAME = '<schema name>'
	CONNECTION_NAME = 'POSTGRESQL_CONNECTION'
	;
```

### PostgreSQL-specifics

Since this method of connecting uses the PostgreSQL database driver and SQL dialect, please check the [documentation of the PostgreSQL SQL dialect](https://github.com/exasol/postgresql-virtual-schema/blob/main/doc/user_guide/postgresql_user_guide.md) for details on type conversion and other PostgreSQL-specifics.

### MySQL-specifics

If you decided to use MySQL-compatible Aurora, please refer to the MySQL dialect [documentation](https://github.com/exasol/mysql-virtual-schema/blob/main/doc/user_guide/mysql_user_guide.md).