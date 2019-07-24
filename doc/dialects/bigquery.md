# Big Query SQL Dialect

The Big Query SQL dialect allows you connect to the [Google Big Query](https://cloud.google.com/bigquery/), Google's serverless, enterprise data warehouse.

## JDBC Driver

Download the [Simba JDBC Driver for Google BigQuery](https://cloud.google.com/bigquery/providers/simba-drivers/).

## Uploading the JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) 
1. Upload the driver to BucketFS

## Installing the Adapter Script

Upload the last available release of [Virtual Schema JDBC Adapter](https://github.com/exasol/virtual-schemas/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

Please remember to check the versions of your JAR files after downloading driver. They can differ from the list below.

```sql
CREATE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-1.19.1.jar;
    %jar /buckets/<BFS service>/<bucket>/avro-1.8.2.jar;
    %jar /buckets/<BFS service>/<bucket>/gax-1.40.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-api-client-1.28.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-api-services-bigquery-v2-rev426-1.25.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-auth-library-credentials-0.13.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-auth-library-oauth2-http-0.13.0.jar;
    %jar /buckets/<BFS service>/<bucket>/GoogleBigQueryJDBC42.jar;
    %jar /buckets/<BFS service>/<bucket>/google-http-client-1.28.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-http-client-jackson2-1.28.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-oauth-client-1.28.0.jar;
    %jar /buckets/<BFS service>/<bucket>/grpc-context-1.18.0.jar;
    %jar /buckets/<BFS service>/<bucket>/jackson-core-2.9.6.jar;
    %jar /buckets/<BFS service>/<bucket>/joda-time-2.10.1.jar;
    %jar /buckets/<BFS service>/<bucket>/opencensus-api-0.18.0.jar;
    %jar /buckets/<BFS service>/<bucket>/opencensus-contrib-http-util-0.18.0.jar;
/
;
```

## Defining a Named Connection

Please follow the [Authenticating to a Cloud API Service article](https://cloud.google.com/video-intelligence/docs/common/auth]) to get Google service account credentials.

Upload the key to BucketFS, then create a named connection:

```sql
CREATE OR REPLACE CONNECTION BIGQUERY_CONNECTION
TO 'jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=<your project id>;OAuthType=0;OAuthServiceAcctEmail=<service account email>;OAuthPvtKeyPath=/<path to the bucket>/<name of the key file>';
```    
You can find additional information about the [JDBC connection string in the Big Query JDBC installation guide](https://www.simba.com/products/BigQuery/doc/JDBC_InstallGuide/content/jdbc/bq/authenticating/serviceaccount.htm]);

## Creating a Virtual Schema

Below you see how a Big Query Virtual Schema is created. Please note that you have to provide the name of a catalog and the name of a schema.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT = 'BIGQUERY'
    CONNECTION_NAME = 'BIGQUERY_CONNECTION'
    CATALOG_NAME = '<catalog name>'
    SCHEMA_NAME = '<schema name>';
```

## How to improve performance?

Please be aware that the current implementation of the dialect can only handle result sets with limited size (a few thousand rows).
If you need to proceed a large amount of data, please, contact our support team and create a support ticket. We can provide another implementation of the dialect the with a speed improvement that is not available officially on our github yet due to the hard installation process.