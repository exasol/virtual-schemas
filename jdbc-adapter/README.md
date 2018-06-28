# JDBC Adapter for Virtual Schemas

[![Build Status](https://travis-ci.org/EXASOL/virtual-schemas.svg?branch=master)](https://travis-ci.org/EXASOL/virtual-schemas)

## Overview
The JDBC adapter for virtual schemas allows you to connect to JDBC data sources like Hive, Oracle, Teradata, Exasol or any other data source supporting JDBC. It uses the well proven ```IMPORT FROM JDBC``` Exasol statement behind the scenes to obtain the requested data, when running a query on a virtual table. The JDBC adapter also serves as the reference adapter for the Exasol virtual schema framework.

The JDBC adapter currently supports the following SQL dialects and data sources. This list will be continuously extended based on the feedback from our users:
* Exasol
* Hive
* Impala
* Oracle
* Teradata
* Redshift
* DB2
* SQL Server
* PostgreSQL

Each such implementation of a dialect handles three major aspects:
* How to **map the tables** in the source systems to virtual tables in Exasol, including how to **map the data types** to Exasol data types.
* How is the **SQL syntax** of the data source, including identifier quoting, case-sensitivity, function names, or special syntax like LIMIT/TOP.
* Which **capabilities** are supported by the data source. E.g. is it supported to run filters, to specify select list expressions, to run aggregation or scalar functions or to order or limit the result.

In addition to the aforementioned dialects there is the so called ```GENERIC``` dialect, which is designed to work with any JDBC driver. It derives the SQL dialect from the JDBC driver metadata. However, it does not support any capabilities and might fail if the data source has special syntax or data types, so it should only be used for evaluation purposes.

If you are interested in a introduction to virtual schemas please refer to the Exasol user manual. You can find it in the [download area of the Exasol user portal](https://www.exasol.com/portal/display/DOWNLOAD/6.0).


## Getting Started

Before you can start using the JDBC adapter for virtual schemas you have to deploy the adapter and the JDBC driver of your data source in your Exasol database.
Please follow the [step-by-step deployment guide](doc/deploy-adapter.md).


## Using the Adapter
The following statements demonstrate how you can use virtual schemas with the JDBC adapter to connect to a Hive system. Please scroll down to see a list of all properties supported by the JDBC adapter.

First we create a virtual schema using the JDBC adapter. The adapter will retrieve the metadata via JDBC and map them to virtual tables. The metadata (virtual tables, columns and data types) are then cached in Exasol.
```sql
CREATE CONNECTION hive_conn TO 'jdbc:hive2://localhost:10000/default' USER 'hive-usr' IDENTIFIED BY 'hive-pwd';

CREATE VIRTUAL SCHEMA hive USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'HIVE'
  CONNECTION_NAME = 'HIVE_CONN'
  SCHEMA_NAME     = 'default';
```

We can now explore the tables in the virtual schema, just like for a regular schema:
```sql
OPEN SCHEMA hive;
SELECT * FROM cat;
DESCRIBE clicks;
```

And we can run arbitrary queries on the virtual tables:
```sql
SELECT count(*) FROM clicks;
SELECT DISTINCT USER_ID FROM clicks;
```

Behind the scenes the Exasol command ```IMPORT FROM JDBC``` will be executed to obtain the data needed from the data source to fulfil the query. The Exasol database interacts with the adapter to pushdown as much as possible to the data source (e.g. filters, aggregations or order by/limit), while considering the capabilities of the data source.

Let's combine a virtual and a native tables in a query:
```
SELECT * from clicks JOIN native_schema.users on clicks.userid = users.id;
```

You can refresh the schemas metadata, e.g. if tables were added in the remote system:
```sql
ALTER VIRTUAL SCHEMA hive REFRESH;
ALTER VIRTUAL SCHEMA hive REFRESH TABLES t1 t2; -- refresh only these tables
```

Or set properties. Depending on the adapter and the property you set this might update the metadata or not. In our example the metadata are affected, because afterwards the virtual schema will only expose two virtul tables.
```sql
ALTER VIRTUAL SCHEMA hive SET TABLE_FILTER='CUSTOMERS, CLICKS';
```

Finally you can unset properties:
```sql
ALTER VIRTUAL SCHEMA hive SET TABLE_FILTER=null;
```

Or drop the virtual schema:
```sql
DROP VIRTUAL SCHEMA hive CASCADE;
```


### Adapter Properties
The following properties can be used to control the behavior of the JDBC adapter. As you see above, these properties can be defined in `CREATE VIRTUAL SCHEMA` or changed afterwards via `ALTER VIRTUAL SCHEMA SET`. Note that properties are always strings, like `TABLE_FILTER='T1,T2'`.

**Mandatory Properties:**

Property                    | Value
--------------------------- | -----------
**SQL_DIALECT**             | Name of the SQL dialect: EXASOL, HIVE, IMPALA, ORACLE, TERADATA, REDSHIFT or GENERIC (case insensitive). If you try generating a virtual schema without specifying this property you will see all available dialects in the error message.

**Mandatory Connection Specification:**
Either specify `CONNECTION_NAME` OR provide `CONNECTION_STRING`,String `USERNAME` and `PASSWORD`.

Property                    | Value
--------------------------- | -----------
**CONNECTION_NAME**         | Name of the connection created with `CREATE CONNECTION` which contains the JDBC connection string, the username and password. If you defined this property then it is not allowed to set CONNECTION_STRING, USERNAME and PASSWORD. We recommend using this property to ensure that the password will not be shown in the logfiles.
**CONNECTION_STRING**       | The JDBC connection string. Only required if CONNECTION_NAME is not set.
**USERNAME**                | Username for authentication. Only required if CONNECTION_NAME is not set.
**PASSWORD**                | Password for authentication. Only required if CONNECTION_NAME is not set.


**Common Optional Properties:**

Property                    | Value
--------------------------- | -----------
**CATALOG_NAME**            | The name of the remote JDBC catalog. This is usually case-sensitive, depending on the dialect. It depends on the dialect whether you have to specify this or not. Usually you have to specify it if the data source JDBC driver supports the concepts of catalogs.
**SCHEMA_NAME**             | The name of the remote JDBC schema. This is usually case-sensitive, depending on the dialect. It depends on the dialect whether you have to specify this or not. Usually you have to specify it if the data source JDBC driver supports the concepts of schemas.
**TABLE_FILTER**            | A comma-separated list of table names (case sensitive). Only these tables will be available as virtual tables, other tables are ignored. Use this if you don't want to have all remote tables in your virtual schema.

**Advanced Optional Properties:**

Property                    | Value
--------------------------- | -----------
**IMPORT_FROM_EXA**         | Only relevant if your data source is EXASOL. Either 'TRUE' or 'FALSE' (default). If true, IMPORT FROM EXA will be used for the pushdown instead of IMPORT FROM JDBC. You have to define EXA_CONNECTION_STRING if this property is true.
**EXA_CONNECTION_STRING**   | The connection string used for IMPORT FROM EXA in the format 'hostname:port'.
**IMPORT_FROM_ORA**         | Similar to IMPORT_FROM_EXA but for an Oracle data source. If enabled, the more performant `IMPORT FROM ORA` operation will be used in place of `IMPORT FROM JDBC`. You also need to define ORA_CONNECTION_NAME if this property is set to 'TRUE'.
**ORA_CONNECTION_NAME**     | Name of the connection to an Oracle database created with `CREATE CONNECTION`. Used by `IMPORT FROM ORA`.
**IS_LOCAL**                | Only relevant if your data source is the same Exasol database where you create the virtual schema. Either 'TRUE' or 'FALSE' (default). If true, you are connecting to the local Exasol database (e.g. for testing purposes). In this case, the adapter can avoid the IMPORT FROM JDBC overhead.
**EXCEPTION_HANDLING**      | Activates or deactivates different exception handling modes. Supported values: 'IGNORE_INVALID_VIEWS', 'NONE' (default). Currently this property only affects the Teradata dialect.


## Debugging
To see all communication between the database and the adapter you can use the python script udf_debug.py located in the [tools](tools) directory.

First, start the udf_debug.py script, which will listen on the specified address and print all incoming text.
```
python tools/udf_debug.py -s myhost -p 3000
```

Then run following SQL statement in your session to redirect all stdout and stderr from the adapter script to the udf_debug.py script we started before.
```sql
ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS='host-where-udf-debug-script-runs:3000'
```

You have to make sure that Exasol can connect to the host running the udf_debug.py script.


## Frequent Issues
* **Error: No suitable driver found for JDBC...**: The JDBC driver class was not discovered automatically. Either you have to add a META-INF/services/java.sql.Driver file with the classname to your jar, or you have to load the driver manually (see JdbcMetadataReader.readRemoteMetadata()).
See https://docs.oracle.com/javase/7/docs/api/java/sql/DriverManager.html
* **Very slow execution of queries with SCRIPT_OUTPUT_ADDRESS**: If `SCRIPT_OUTPUT_ADDRESS` is set as explained in the [debugging section](#debugging), verify that a service is actually listening at that address. Otherwise, if Exasol can not establish a connection, repeated connection attempts can be the cause for slowdowns.
* **Very slow execution of queries**: Depending on which JDK version Exasol uses to execute Java user-defined functions, a blocking randomness source may be used by default. Especially cryptographic operations do not complete until the operating system has collected a sufficient amount of entropy. This problem seems to occur most often when Exasol is run in an isolated environment, e.g., a virtual machine or a container. A solution is to use a non-blocking randomness source. 
  To do so, log in to EXAOperation and shutdown the database. Append `-etlJdbcJavaEnv -Djava.security.egd=/dev/./urandom` to the "Extra Database Parameters" input field and power the database on again.

## Developing New Dialects

If you want to contribute a new dialect please visit the guide [how to develop and test a dialect](doc/develop-dialect.md).
