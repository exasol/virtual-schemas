# SAP HANA SQL Dialect

The SAP HANA SQL dialect allows you to access [HANA](https://www.sap.com/products/hana.html) databases via Virtual Schemas.

## JDBC Driver

Download the latest version of the [SAP HANA JDBC driver](https://search.maven.org/search?q=g:com.sap.cloud.db.jdbc%20AND%20a:ngdbc&core=gav).

### Upload JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) 
1. Upload the driver to BucketFS

## Connecting to SAP HANA

1. Create schema
    ```sql
    CREATE SCHEMA ADAPTER;
    ```
2. Create Adapter Script

    You can install the adapter script via the special SQL command `CREATE JAVA ADAPTER SCRIPT`
    
    ```sql
     --/
        CREATE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/bfsdefault/saphana/virtualschema-jdbc-adapter-dist-1.19.6.jar;
    %jar /buckets/bfsdefault/saphana/ngdbc-2.4.56.jar;
    /
    ;
    ```

3. Create a connection
    
    ```sql
    CREATE OR REPLACE CONNECTION SAPHANA_CONNECTION 
    TO 'jdbc:sap://write.ip.address.here:port' 
    USER 'username' 
    IDENTIFIED BY 'yourpassword';
    ```

4. Create a Virtual Schema

    ```sql
    CREATE VIRTUAL SCHEMA SAPHANATEST
    USING adapter_sap_hana.jdbc_adapter_sap_hana_script
    WITH
        SQL_DIALECT = 'SAPHANA'
        CONNECTION_NAME = 'SAPHANA_CONNECTION'
        SCHEMA_NAME = 'TESTSAPHANASCHEMA';
    ```
    If you want to use [logging](../development/remote_logging.md), please, add additional parameters:
    
    ```sql
        DEBUG_ADDRESS = 'write.ip.address.here:port'
        LOG_LEVEL = 'FINE' 
    ``` 
    
## Know Issues

### Unsupported Column Types

The following column types are not supported by this dialect:

* `ARRAY`
* `BLOB`
* `NCLOB`
* `ST_GEOMETRY`
* `ST_POINT`
* `TEXT`
* `VARBINARY`

### Unparameterized Column Type `DECIMAL`

In HANA you are allowed to create columns of type `DECIMAL` without parameterizing them. I.e. you can skip the part in the brackets.

What the Virtual Schemas get from the HANA JDBC driver as column metadata is a column of precision 34 and scale 0. So in theory this columns values should behave like a 34-digit integer number. Tests that we conducted with a SQL editor though show that the values can have fractional digits. In fact values of this column type behave like floating point numbers.

Unfortunately we can't tell the metadata of columns defined with `DECIMAL` and `DECIMAL(34,0)` appart even though they behave differently.

To fix this, don't define any columns that you plan to use via a Virtual Schema with unparameterized type `DECIMAL`.

### Column Type `SMALLDECIMAL`

The type `SMALLDECIMAL` exhibits the same behavior as the [unparameterized Column Type `DECIMAL`](#unparameterized-column-type-decimal).

Also here the only solution is to not use it in conjunction with a Virtual Schema.

### Column Type `TIME`

The type `TIME` always comes to Virtual Schema as a  `TIMESTAMP` data type therefore it has not only time, but also date.
For now it is always a current date. Example: 10:30:25 will be 27.06.2019 10:30:25.0 where date is a current date. 
