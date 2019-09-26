# Oracle SQL Dialect

[Oracle Database](https://www.oracle.com/database/) is a proprietary multi-model database management system produced and marketed by Oracle Corporation. It is a database commonly used for running online transaction processing (OLTP), data warehousing (DW) and mixed (OLTP & DW) database workloads.

## Registering the JDBC Driver in EXAOperation

First download the [Oracle JDBC driver](https://www.oracle.com/technetwork/database/application-development/jdbc/downloads/index.html).

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
| Name      | `ORACLE`                                            |
| Main      | `oracle.jdbc.driver.OracleDriver`                   |
| Prefix    | `jdbc:oracle:thin:`                                 |
| Files     | `ojdbc<JDBC driver version>.jar`                                        |


## Uploading the JDBC Driver to EXAOperation

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
1. Upload the driver to BucketFS

This step is necessary since the UDF container the adapter runs in has no access to the JDBC drivers installed via EXAOperation but it can access BucketFS.

## Installing the Adapter Script

Upload the latest available release of [Virtual Schema JDBC Adapter](https://github.com/exasol/virtual-schemas/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
CREATE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<bucket>/virtualschema-jdbc-adapter-dist-2.1.2.jar;
  %jar /buckets/<BFS service>/<bucket>/ojdbc<JDBC driver version>.jar;
/
;
```

## Defining a Named Connection

Define the connection to Oracle as shown below.

```sql
CREATE OR REPLACE CONNECTION ORACLE_JDBC_CONNECTION
  TO 'jdbc:oracle:thin:@//<host>:<port>/<service name>'
  USER '<user>'
  IDENTIFIED BY '<password>';
```

A quick option to test the `ORACLE_JDBC_CONNECTION` connection is to run an `IMPORT FROM JDBC` query. The connection works, if `42` is returned.

```sql
IMPORT FROM JDBC AT jdbc_oracle
  STATEMENT 'SELECT 42 FROM DUAL';
```

## Creating a Virtual Schema

Below you see how an Oracle Virtual Schema is created. 

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT     = 'ORACLE'
    CONNECTION_NAME = 'ORACLE_JDBC_CONNECTION'
    SCHEMA_NAME     = '<schema name>';
```

## Using IMPORT FROM ORA Instead of IMPORT FROM JDBC

Exasol provides the `IMPORT FROM ORA` command for loading data from Oracle. It is possible to create a virtual schema that uses `IMPORT FROM ORA` instead of JDBC to communicate with Oracle. Both options are indented to support the same features. `IMPORT FROM ORA` almost always offers better performance since it is implemented natively.

This behavior is toggled by the Boolean `IMPORT_FROM_ORA` variable. Note that a JDBC connection to Oracle is still required to fetch metadata. In addition, a "direct" connection to the Oracle database is needed.

### Deploying the Oracle Instant Client

To be able to communicate with Oracle, you first need to supply Exasol with the Oracle Instant Client, which can be obtained [directly from Oracle](http://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html). Open EXAoperation, visit Software -> "Upload Oracle Instant Client" and select the downloaded package. The latest version of Oracle Instant Client we tested is `instantclient-basic-linux.x64-12.1.0.2.0`.

### Creating an Oracle Connection

Having deployed the Oracle Instant Client, a connection to your Oracle database can be set up.

```sql
CREATE CONNECTION ORA_CONNECTION
  TO '(DESCRIPTION =
		(ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)
                                   (HOST = <host>)
                                   (PORT = <port>)))
		(CONNECT_DATA = (SERVER = DEDICATED)
                        (SERVICE_NAME = <service_name>)))'
	USER '<username>'
	IDENTIFIED BY '<password>';
```

This connection can be tested using, e.g., the following SQL expression.

```sql
IMPORT FROM ORA at ORA_CONNECTION
  STATEMENT 'SELECT 42 FROM DUAL';
```

### Creating a Virtual Schema USING an ORA CONNECTION

Assuming you already setup the JDBC connection `ORACLE_JDBC_CONNECTION` as shown in the previous section, you can continue with creating the virtual schema.

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
    USING ADAPTER.JDBC_ADAPTER
    WITH
    SQL_DIALECT     = 'ORACLE'
    CONNECTION_NAME = 'ORACLE_JDBC_CONNECTION'
    SCHEMA_NAME     = '<schema name>'
    IMPORT_FROM_ORA = 'true'
    ORA_CONNECTION_NAME = 'ORA_CONNECTION';
```

## Supported capabilities

The Oracle dialect does not support all capabilities. A complete list can be found in [OracleSqlDialect.getCapabilities()](../../jdbc-adapter/virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/dialects/oracle/OracleSqlDialect.java).

## Type Mappings and Limitations

Oracle data types are mapped to their equivalents in Exasol. The following exceptions apply:

- `NUMBER`, `NUMBER with precision > 36` and `LONG` are casted to `VARCHAR` to prevent a loss of precision. 

    If you want to return a DECIMAL type for these types you can set the property ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE: 
    
    `ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE='36,20'` 
    
    This will cast NUMBER with precision > 36, NUMBER without precision and LONG to DECIMAL(36,20).
    Keep in mind that this will yield errors if the data in the Oracle database does not fit into the specified DECIMAL type.

- `DATE` is casted to `TIMESTAMP`. This data type is only supported for positive year values, i.e., years > 0001.
- `TIMESTAMP WITH [LOCAL] TIME ZONE` is casted to `TIMESTAMP`. 
- `INTERVAL` is casted to `VARCHAR`.
- `CLOB` and `NCLOB` values are supported up to the maximum size of an Exasol [`VARCHAR`](https://docs.exasol.com/sql_references/data_types/datatypedetails.htm#StringDataType).

    By default an error will be thrown if you try to import a value that is larger. If you want to import from tables which contain bigger values, you need to truncate the values.
    
- `BLOB`, `RAW` and `LONG RAW` are not supported.