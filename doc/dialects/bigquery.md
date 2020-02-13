# Big Query SQL Dialect

The Big Query SQL dialect allows you connect to the [Google Big Query](https://cloud.google.com/bigquery/), Google's serverless, enterprise data warehouse.

## JDBC Driver

Download the [Simba JDBC Driver for Google BigQuery](https://cloud.google.com/bigquery/providers/simba-drivers/).

## Uploading the JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) 
1. Upload the driver to BucketFS

Hint: Magnitude Simba driver contains a lot of jar files, but you can upload all of them together as an archive (`.tar.gz`, for example).
The archive will be unpacked automatically in the bucket and you can access the files using the following path pattern '<your bucket>/<archive's name without extension>/<name of a file form the archive>.jar'

## Installing the Adapter Script

Upload the latest available release of [Virtual Schema JDBC Adapter](https://github.com/exasol/virtual-schemas/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA SCHEMA_FOR_VS_SCRIPT;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

Please remember to check the versions of your JAR files after downloading driver. They can differ from the list below.

```sql
CREATE JAVA ADAPTER SCRIPT SCHEMA_FOR_VS_SCRIPT.ADAPTER_SCRIPT_BIGQUERY AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-3.0.2.jar;
    %jar /buckets/<BFS service>/<bucket>/avro-1.8.2.jar;
    %jar /buckets/<BFS service>/<bucket>/gax-1.42.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-api-client-1.28.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-api-services-bigquery-v2-rev426-1.25.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-auth-library-credentials-0.15.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-auth-library-oauth2-http-0.13.0.jar;
    %jar /buckets/<BFS service>/<bucket>/GoogleBigQueryJDBC42.jar;
    %jar /buckets/<BFS service>/<bucket>/google-http-client-1.29.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-http-client-jackson2-1.28.0.jar;
    %jar /buckets/<BFS service>/<bucket>/google-oauth-client-1.28.0.jar;
    %jar /buckets/<BFS service>/<bucket>/grpc-context-1.18.0.jar;
    %jar /buckets/<BFS service>/<bucket>/guava-26.0-android.jar
    %jar /buckets/<BFS service>/<bucket>/jackson-core-2.9.6.jar;
    %jar /buckets/<BFS service>/<bucket>/joda-time-2.10.1.jar;
    %jar /buckets/<BFS service>/<bucket>/opencensus-api-0.18.0.jar;
    %jar /buckets/<BFS service>/<bucket>/opencensus-contrib-http-util-0.18.0.jar;
/
;
```

## Defining a Named Connection

Please follow the [Authenticating to a Cloud API Service article](https://cloud.google.com/docs/authentication/) to get Google service account credentials.

Upload the key to BucketFS, then create a named connection:

```sql
CREATE OR REPLACE CONNECTION BIGQUERY_JDBC_CONNECTION
TO 'jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=<your project id>;OAuthType=0;OAuthServiceAcctEmail=<service account email>;OAuthPvtKeyPath=/<path to the bucket>/<name of the key file>';
```    
You can find additional information about the [JDBC connection string in the Big Query JDBC installation guide](https://www.simba.com/products/BigQuery/doc/JDBC_InstallGuide/content/jdbc/using/intro.htm);

## Creating a Virtual Schema

Below you see how a Big Query Virtual Schema is created. Please note that you have to provide the name of a catalog and the name of a schema.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING SCHEMA_FOR_VS_SCRIPT.ADAPTER_SCRIPT_BIGQUERY
    WITH
    SQL_DIALECT = 'BIGQUERY'
    CONNECTION_NAME = 'BIGQUERY_JDBC_CONNECTION'
    CATALOG_NAME = '<catalog name>'
    SCHEMA_NAME = '<schema name>';
```


## Data Types Conversion

BigQuery Data Type | Supported | Converted Exasol Data Type| Known limitations
-------------------|-----------|---------------------------|-------------------
BOOLEAN            |  ✓        | BOOLEAN                   | 
BYTES              |  ×        |                           | 
DATE               |  ✓        | DATE                      | 
DATETIME           |  ✓        | TIMESTAMP                 | 
FLOAT              |  ✓        | DOUBLE                    | Expected range for correct mapping: -99999999.99999999 .. 99999999.99999999. 
GEOGRAPHY          |  ✓        | VARCHAR(65535)            |
INTEGER            |  ✓        | DECIMAL(19,0)             | 
NUMERIC            |  ✓        | VARCHAR(2000000)          | 
RECORD/STRUCT      |  ×        |                           | 
STRING             |  ✓        | VARCHAR(65535)            | 
TIME               |  ✓        | VARCHAR(16)               | 
TIMESTAMP          |  ✓        | TIMESTAMP                 | Expected range for correct mapping: 1582-10-15 00:00:01 .. 9999-12-31 23:59:59.9999. JDBC driver maps dates before 1582-10-15 00:00:01 incorrectly.  Example of incorrect mapping: 1582-10-14 22:00:01 -> 1582-10-04 22:00:01

If you need to use currently unsupported data types or find a way around known limitations, please, create a github issue in the [VS repository](https://github.com/exasol/virtual-schemas/issues).

## Performance

Please be aware that the current implementation of the dialect can only handle result sets with limited size (a few thousand rows).

## Testing information

In the following matrix you find combinations of JDBC driver and dialect version that we tested.

Virtual Schema Version| Big Query Version   | Driver Name                                 | Driver Version 
----------------------|---------------------|---------------------------------------------|------------------------
 3.0.2                | Google BigQuery 2.0 |  Magnitude Simba JDBC driver for BigQuery   | 1.2.2.1004