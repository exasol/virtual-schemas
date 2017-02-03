# JDBC Adapter for Virtual Schemas

## Overview
The JDBC adapter for virtual schemas allows you to connect to JDBC data sources like Hive, Oracle, Teradata, EXASOL or any other data source supporting JDBC. It uses the well proven ```IMPORT FROM JDBC``` EXASOL statement behind the scenes to obtain the requested data, when running a query on a virtual table. The JDBC adapter also serves as the reference adapter for the EXASOL virtual schema framework.

The JDBC adapter currently supports the following SQL dialects and data sources. This list will be continuously extended based on the feedback from our users:
* EXASOL
* Hive
* Impala
* Oracle
* Teradata
* Redshift

Each such implementation of a dialect handles three major aspects:
* How to **map the tables** in the source systems to virtual tables in EXASOL, including how to **map the data types** to EXASOL data types.
* How is the **SQL syntax** of the data source, including identifier quoting, case-sensitivity, function names, or special syntax like LIMIT/TOP.
* Which **capabilities** are supported by the data source. E.g. is it supported to run filters, to specify select list expressions, to run aggregation or scalar functions or to order or limit the result.

In addition to the aforementioned dialects there is the so called ```GENERIC``` dialect, which is designed to work with any JDBC driver. It derives the SQL dialect from the JDBC driver metadata. However, it does not support any capabilities and might fail if the data source has special syntax or data types, so it should only be used for evaluation purposes.

If you are interested in a introduction to virtual schemas please refer to the EXASOL user manual. You can find it in the [download area of the EXASOL user portal](https://www.exasol.com/portal/display/DOWNLOAD/6.0).


## Getting Started

Before you can start using the JDBC adapter for virtual schemas you have to deploy the adapter and the JDBC driver of your data source in your EXASOL database.
Please follow the [step-by-step deployment guide](doc/deploy-adapter.md).


## Using the Adapter
The following statements demonstrate how you can use virtual schemas with the JDBC adapter to connect to a Hive system. Please scroll down to see a list of all properties supported by the JDBC adapter.

First we create a virtual schema using the JDBC adapter. The adapter will retrieve the metadata via JDBC and map them to virtual tables. The metadata (virtual tables, columns and data types) are then cached in EXASOL.
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

Behind the scenes the EXASOL command ```IMPORT FROM JDBC``` will be executed to obtain the data needed from the data source to fulfil the query. The EXASOL database interacts with the adapter to pushdown as much as possible to the data source (e.g. filters, aggregations or order by/limit), while considering the capabilities of the data source.

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
The following properties can be used to control the behavior of the JDBC adapter. As you see above, these properties can be defined in ```CREATE VIRTUAL SCHEMA``` or changed afterwards via ```ALTER VIRTUAL SCHEMA SET```. Note that properties are always strings, like `TABLE_FILTER='T1,T2'`.

**Mandatory Properties:**

Parameter                   | Value
--------------------------- | -----------
**SQL_DIALECT**             | Name of the SQL dialect: EXASOL, HIVE, IMPALA, ORACLE, TERADATA, REDSHIFT or GENERIC (case insensitive). If you try generating a virtual schema without specifying this property you will see all available dialects in the error message.
**CONNECTION_NAME**         | Name of the connection created with ```CREATE CONNECTION``` which contains the jdbc connection string, the username and password. If you defined this property then it is not allowed to set CONNECTION_STRING, USERNAME and PASSWORD. We recommend using this property to ensure that the password will not be shown in the logfiles.
**CONNECTION_STRING**       | The jdbc connection string. Only required if CONNECTION_NAME is not set.


**Typical Optional Parameters:**

Parameter                   | Value
--------------------------- | -----------
**CATALOG_NAME**            | The name of the remote jdbc catalog. This is usually case-sensitive, depending on the dialect. It depends on the dialect whether you have to specify this or not. Usually you have to specify it if the data source JDBC driver supports the concepts of catalogs.
**SCHEMA_NAME**             | The name of the remote jdbc schema. This is usually case-sensitive, depending on the dialect. It depends on the dialect whether you have to specify this or not. Usually you have to specify it if the data source JDBC driver supports the concepts of schemas.
**USERNAME**                | Username for authentication. Can only be set if CONNECTION_NAME is not set.
**PASSWORD**                | Password for authentication. Can only be set if CONNECTION_NAME is not set.
**TABLE_FILTER**            | A comma-separated list of table names (case sensitive). Only these tables will be available as virtual tables, other tables are ignored. Use this if you don't want to have all remote tables in your virtual schema.


**Advanced Optional Properties:**

Parameter                   | Value
--------------------------- | -----------
**DEBUG_ADDRESS**           | The IP address/hostname and port of the UDF debugging service, e.g. 'myhost:3000'. Debug output from the UDFs will be sent to this address. See the section on debugging below.
**IMPORT_FROM_EXA**         | Only relevant if your data source is EXASOL. Either 'TRUE' or 'FALSE' (default). If true, IMPORT FROM EXA will be used for the pushdown instead of IMPORT FROM JDBC. You have to define EXA_CONNECTION_STRING if this property is true.
**EXA_CONNECTION_STRING**   | The connection string used for IMPORT FROM EXA in the format 'hostname:port'.
**IS_LOCAL**                | Only relevant if your data source is the same EXASOL database where you create the virtual schema. Either 'TRUE' or 'FALSE' (default). If true, you are connecting to the local EXASOL database (e.g. for testing purposes). In this case, the adapter can avoid the IMPORT FROM JDBC overhead.



## Debugging
To see all communication between the database and the adapter you can use the python script udf_debug.py.

First, start the udf_debug.py script, which will listen on the specified address and print all incoming text.
```
python tools/udf_debug.py -s myhost -p 3000
```
And set the DEBUG_ADDRESS properties so that the adapter will send debug output to the specified address.
```sql
ALTER VIRTUAL SCHEMA vs SET DEBUG_ADDRESS='host-where-udf-debug-script-runs:3000'
```

You have to make sure that EXASOL can connect to the host running the udf_debug.py script.


## Frequent Issues
* **Error: No suitable driver found for jdbc...**: The jdbc driver class was not discovered automatically. Either you have to add a META-INF/services/java.sql.Driver file with the classname to your jar, or you have to load the driver manually (see JdbcMetadataReader.readRemoteMetadata()).
See https://docs.oracle.com/javase/7/docs/api/java/sql/DriverManager.html


## Developing New Dialects

If you want to contribute a new dialect please visit the guide [how to develop and test a dialect](doc/develop-dialect.md).
