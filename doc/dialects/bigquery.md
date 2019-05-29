# Big Query SQL Dialect

The Big Query SQL dialect allows you connect to the [Google Big Query](https://cloud.google.com/bigquery/), Google's serverless, enterprise data warehouse.

## JDBC Driver

Download the [Simba JDBC Driver for Google BigQuery](https://cloud.google.com/bigquery/providers/simba-drivers/).

### Registering the JDBC Driver in EXAOperation

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
| Name      | `SimbaBigQuery`                                     |
| Main      | `com.simba.googlebigquery.jdbc42.Driver`            |
| Prefix    | `jdbc:bigquery:`                                    |
| Files     | `avro-1.8.2.jar`                                    |
|           | `gax-1.40.0.jar`                                    |
|           | `google-api-client-1.28.0.jar`                      |
|           | `google-api-services-bigquery-v2-rev426-1.25.0.jar` |
|           | `google-auth-library-credentials-0.13.0.jar`        |
|           | `google-auth-library-oauth2-http-0.13.0.jar`        |
|           | `GoogleBigQueryJDBC41.jar`                          |
|           | `google-http-client-1.28.0.jar`                     |
|           | `google-http-client-jackson2-1.28.0.jar`            |
|           | `google-oauth-client-1.28.0.jar`                    |
|           | `grpc-context-1.18.0.jar`                           |
|           | `jackson-core-2.9.6.jar`                            |
|           | `joda-time-2.10.1.jar`                              |
|           | `opencensus-api-0.18.0.jar`                         |
|           | `opencensus-contrib-http-util-0.18.0.jar`           |

Versions can differ from the list above.

### Upload JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) 
1. Upload the driver to BucketFS


## Connecting to Big Query

1. Create schema:
    ```sql
    CREATE SCHEMA adapter;
    ```
2. Create Adapter Script

    You install the adapter script via the special SQL command `CREATE JAVA ADAPTER SCRIPT`. 
    Please remember to check the versions of your jar-files after downloading driver. They can differ from the list below.

    ```sql
    --/
    CREATE JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/bucketfs1/jdbc/virtualschema-jdbc-adapter-dist-1.17.0.jar;
    %jar /buckets/bfsdefault/jars/avro-1.8.2.jar;
    %jar /buckets/bfsdefault/jars/gax-1.40.0.jar;
    %jar /buckets/bfsdefault/jars/google-api-client-1.28.0.jar;
    %jar /buckets/bfsdefault/jars/google-api-services-bigquery-v2-rev426-1.25.0.jar;
    %jar /buckets/bfsdefault/jars/google-auth-library-credentials-0.13.0.jar;
    %jar /buckets/bfsdefault/jars/google-auth-library-oauth2-http-0.13.0.jar;
    %jar /buckets/bfsdefault/jars/GoogleBigQueryJDBC41.jar;
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

    Please follow [this article](https://cloud.google.com/video-intelligence/docs/common/auth]) to get Google service account credentials.

    Upload the key to EXAOperation, then create connection:

    ```sql
    CREATE OR REPLACE CONNECTION BIGQUERY_CONNECTION
    TO 'jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=<your_project_id>;OAuthType=0;OAuthServiceAcctEmail=<your_service_account_email>;OAuthPvtKeyPath=/<path_to_your_bucket>/<name_of your_key_file>';
    ```
    Additional information about connection string you can find [here](https://www.simba.com/products/BigQuery/doc/JDBC_InstallGuide/content/jdbc/bq/authenticating/serviceaccount.htm]);

4. Creating a Virtual Schema

    ```sql
    CREATE VIRTUAL SCHEMA "bigquerytest"
    USING ADAPTER.jdbc_adapter
    WITH
        SQL_DIALECT = 'BIGQUERY'
        CONNECTION_NAME = 'BIGQUERY_CONNECTION'
        CATALOG_NAME = 'virtualschematest'
        SCHEMA_NAME = 'testdataset'
        DEBUG_ADDRESS = '192.168.122.1:3000'
        LOG_LEVEL = 'FINE';
    ```