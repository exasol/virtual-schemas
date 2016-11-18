# JDBC Adapter for Virtual Schemas

## Overview
This is an adapter for virtual schemas to connect to JDBC data sources, like Hive or Oracle or any other. It serves as the reference adapter for the virtual schema framework.

If you are interested in a introduction to virtual schemas please refer to our [virtual schemas documentation](../doc).


## Deploying the Adapter

Run the following steps to deploy your adapter:

### Prerequisites:
* EXASOL >= 6.0
* Advanced edition (which includes the ability to execute adapter scripts)

### Build:

To build a fat jar (including all dependencies) run:
```
mvn clean -DskipTests package
```

The resulting fat jar is stored in ```virtualschema-jdbc-adapter-dist/target/virtualschema-jdbc-adapter-dist-0.0.1-SNAPSHOT.jar```.

### Upload Adapter jar

You have to upload the jar of the adapter to a bucket of your choice. This will allow using the jar in the adapter script. See chapter 3.6.4. "The synchronous cluster file system BucketFS" in the EXASolution User Manual for how to use BucketFS.

### Upload JDBC Driver files

You have to upload the JDBC driver files of your remote database two times: First into a bucket of your choice, so that they can be accessed from the adapter script. Second you have to upload the files as a JDBC driver in EXAOperation (under Software -> JDBC Drivers).

### Deploy Adapter Script
Then run the following SQL commands to deploy the adapter in the database:
```sql
-- The adapter is simply a script. It has to be stored in any regular schema.
CREATE SCHEMA adapter;
CREATE JAVA ADAPTER SCRIPT adapter.jdbc_adapter AS

  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/your-bucket-fs/your-bucket/virtualschema-jdbc-adapter-dist-0.0.1-SNAPSHOT.jar;

  // You have to add all files of the data source jdbc driver here (e.g. MySQL or Hive)
  %jar /buckets/your-bucket-fs/your-bucket/name-of-data-source-jdbc-driver.jar;
/
```


## Using the Adapter
The following statements demonstrate how you can use the JDBC adapter and virtual schemas. Please scroll down to see a list of all properties supported by the JDBC adapter. Please also consult the user manual for a in-depth introduction to virtual schemas.

Create a virtual schema using the JDBC adapter. This will retrieve and cache the metadata via JDBC.
```sql
CREATE CONNECTION hive_conn TO 'jdbc:hive2://localhost:10000/default' USER 'hive-usr' IDENTIFIED BY 'hive-pwd';

CREATE VIRTUAL SCHEMA hive USING adapter.jdbc_adapter WITH
  SQL_DIALECT     = 'HIVE'
  CONNECTION_NAME = 'HIVE_CONN'
  SCHEMA_NAME     = 'default';
```

Explore the tables in the virtual schema:
```sql
OPEN SCHEMA hive;
SELECT * FROM cat;
DESCRIBE clicks;
```

Run queries on the virtual tables:
```sql
SELECT count(*) FROM clicks;
SELECT DISTINCT USER_ID FROM clicks;
```

Combine virtual and native tables in a query:
```
SELECT * from clicks JOIN native_schema.users on clicks.userid = users.id;
```

You can refresh the schemas metadata, e.g. if tables were added in the remote system:
```sql
ALTER VIRTUAL SCHEMA hive REFRESH;
ALTER VIRTUAL SCHEMA hive REFRESH TABLES t1 t2; -- refresh only these tables
```

Or set properties. This might update the metadata (if you change the remote database) or not.
```sql
ALTER VIRTUAL SCHEMA hive SET TABLE_FILTER='CUSTOMERS, CLICKS';
```

Or unset properties:
```sql
ALTER VIRTUAL SCHEMA hive SET TABLE_FILTER=null;
```

Or drop the schema
```sql
DROP VIRTUAL SCHEMA hive CASCADE;
```



### Adapter Properties
Note that properties are always strings, like `TABLE_FILTER='T1,T2'`.

**Mandatory Properties:**

Parameter                   | Value
--------------------------- | -----------
**SQL_DIALECT**             | Name of the SQL dialect, e.g. EXASOL, IMPALA, ORACLE or GENERIC (case insensitive). For some SQL dialects we have presets which are used for the pushdown SQL query generation. If you try to generate a virtual schema without specifying this property you will see all available dialects in the error message.
**CONNECTION_NAME**         | Name of the connection created with ```CREATE CONNECTION``` which contains the jdbc connection string, the username and password. You don't need to set CONNECTION_STRING, USERNAME and PASSWORD if you define this property. We recommend this to ensure that passwords are not shown in the logfiles.
**CONNECTION_STRING**       | The jdbc connection string. Only required if CONNECTION_NAME is not set.


**Typical Optional Parameters:**

Parameter                   | Value
--------------------------- | -----------
**CATALOG_NAME**            | The name of the remote jdbc catalog. This is usually case-sensitive, depending on the dialect. It depends on the dialect whether you have to specify this or not. Usually you have to specify it if the data source JDBC driver supports the concepts of catalogs.
**SCHEMA_NAME**             | The name of the remote jdbc schema. This is usually case-sensitive, depending on the dialect.  It depends on the dialect whether you have to specify this or not.  Usually you have to specify it if the data source JDBC driver supports the concepts of schemas.
**USERNAME**                | Username for authentication. Can only be set if CONNECTION_NAME is not set.
**PASSWORD**                | Password for authentication. Can only be set if CONNECTION_NAME is not set.


**Advanced Optional Properties:**

Parameter                   | Value
--------------------------- | -----------
**TABLE_FILTER**            | A comma-separated list of tablenames (case sensitive). Only these tables will be available, other tables are ignored. Use this if you don't want to have all remote tables in your virtual schema.
**IMPORT_FROM_EXA**         | Either 'TRUE' or 'FALSE' (default). If true, IMPORT FROM EXA will be used for the pushdown instead of IMPORT FROM JDBC. You have to define EXA_CONNECTION_STRING if this property is true.
**EXA_CONNECTION_STRING**   | The connection string used for IMPORT FROM EXA in the format 'hostname:port'.
**DEBUG_ADDRESS**           | The IP address/hostname and port of the UDF debugging service, e.g. 'myhost:3000'. Debug output from the UDFs will be sent to this address. See the section on debugging below.
**IS_LOCAL**                | Either 'TRUE' or 'FALSE' (default). If true, you are connecting to the local EXASOL database (e.g. for testing purposes). In this case, the adapter can avoid the IMPORT FROM JDBC overhead.



## Debugging
To see all communication between the database and the adapter you can use the python script udf_debug.py.

First, start the udf_debug.py script, which will listen on the specified address and print all incoming text.
```
python tools/udf_debug.py -s myhost -p 3000
```
And set the DEBUG_ADDRESS properties so that the adapter will send debug output to the specified address.
```sql
ALTER VIRTUAL SCHEMA vs SET DEBUG_ADDRESS='myhost:3000'
```




## Frequent Issues
* **Error: No suitable driver found for jdbc...**: The jdbc driver class was not discovered automatically. Either you have to add a META-INF/services/java.sql.Driver file with the classname to your jar, or you have to load the driver manually (see JdbcMetadataReader.readRemoteMetadata()).
See https://docs.oracle.com/javase/7/docs/api/java/sql/DriverManager.html
