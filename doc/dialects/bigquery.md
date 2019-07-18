# Big Query SQL Dialect

The Big Query SQL dialect allows you connect to the [Google Big Query](https://cloud.google.com/bigquery/), Google's serverless, enterprise data warehouse.

## JDBC Driver

Download the [Simba JDBC Driver for Google BigQuery](https://cloud.google.com/bigquery/providers/simba-drivers/).

### Upload JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) 
1. Upload the driver to BucketFS

## Connecting to Big Query

1. Create schema:
    ```sql
    CREATE SCHEMA ADAPTER;
    ```
2. Create Adapter Script

    You install the adapter script via the special SQL command `CREATE JAVA ADAPTER SCRIPT`. 
    Please remember to check the versions of your JAR files after downloading driver. They can differ from the list below.

    ```sql
    --/
    CREATE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/bfsdefault/jars/virtualschema-jdbc-adapter-dist-1.19.7.jar;
    %jar /buckets/bfsdefault/jars/avro-1.8.2.jar;
    %jar /buckets/bfsdefault/jars/gax-1.40.0.jar;
    %jar /buckets/bfsdefault/jars/google-api-client-1.28.0.jar;
    %jar /buckets/bfsdefault/jars/google-api-services-bigquery-v2-rev426-1.25.0.jar;
    %jar /buckets/bfsdefault/jars/google-auth-library-credentials-0.13.0.jar;
    %jar /buckets/bfsdefault/jars/google-auth-library-oauth2-http-0.13.0.jar;
    %jar /buckets/bfsdefault/jars/GoogleBigQueryJDBC42.jar;
    %jar /buckets/bfsdefault/jars/google-http-client-1.28.0.jar;
    %jar /buckets/bfsdefault/jars/google-http-client-jackson2-1.28.0.jar;
    %jar /buckets/bfsdefault/jars/google-oauth-client-1.28.0.jar;
    %jar /buckets/bfsdefault/jars/grpc-context-1.18.0.jar;
    %jar /buckets/bfsdefault/jars/jackson-core-2.9.6.jar;
    %jar /buckets/bfsdefault/jars/joda-time-2.10.1.jar;
    %jar /buckets/bfsdefault/jars/opencensus-api-0.18.0.jar;
    %jar /buckets/bfsdefault/jars/opencensus-contrib-http-util-0.18.0.jar;
    /
    ;
    ```

3. Create a connection

    Please follow the [Authenticating to a Cloud API Service article](https://cloud.google.com/video-intelligence/docs/common/auth]) to get Google service account credentials.

    Upload the key to EXAOperation, then create a named connection:

    ```sql
    CREATE OR REPLACE CONNECTION BIGQUERY_CONNECTION
    TO 'jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=<your_project_id>;OAuthType=0;OAuthServiceAcctEmail=<your_service_account_email>;OAuthPvtKeyPath=/<path_to_your_bucket>/<name_of your_key_file>';
    ```
    You can find additional information about the [JDBC connection string in the Big Query JDBC installation guide](https://www.simba.com/products/BigQuery/doc/JDBC_InstallGuide/content/jdbc/bq/authenticating/serviceaccount.htm]);

4. Creating a Virtual Schema

    ```sql
    CREATE VIRTUAL SCHEMA "bigquerytest"
    USING ADAPTER.JDBC_ADAPTER
    WITH
        SQL_DIALECT = 'BIGQUERY'
        CONNECTION_NAME = 'BIGQUERY_CONNECTION'
        CATALOG_NAME = 'virtualschematest'
        SCHEMA_NAME = 'testdataset';
    ```