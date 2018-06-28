# How To Develop and Test a Dialect
This page describes how you can develop and semi-automatically test a dialect for the JDBC adapter. The framework for testing a dialect is still work in progress.

# Content
* [How To Develop a Dialect](#how-to-develop-a-dialect)
* [How To Start Integration Tests](#how-to-start-integration-tests)

## How To Develop a Dialect
You can implement a dialect by implementing the interface ```com.exasol.adapter.dialects.SqlDialect```.
We recommend to look at the following ressources to get started:
* First have a look at the [SqlDialect interface source code](../virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/dialects/SqlDialect.java). You can start with the comments of the interface and have a look at the methods you can override.
* Second you can review the source code of one of the [dialect implementations](../virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/dialects/impl) as an inspiration. Ideally you should look at the dialect which is closest to your data source.

To implement a full dialect for a typical data source you have to run all of the following steps. We recommend to follow the order proposed here.

### Setup Data Source
* Setup and start the database
* Testdata: Create a test schema with a simple table (simple data types)

### Setup EXASOL
* Setup and start an EXASOL database with virtual schemas feature
* Upload the JDBC drivers of the data source via EXAOperation
* Manual test: query data from the data source via IMPORT FROM JDBC

### Catalog, Schema & Table Mapping
* Override the SqlDialect methods for catalog, schema and table mapping
* Manual test: create a virtual schema by specifying the catalog and/or schema.

### Data Type Mapping
* Testdata: Create a table with all data types and at least one row of data
* Override the SqlDialect method for data type mapping
* Automatic test: sys tables show virtual table and columns with correctly mapped type
* Automatic test: running SELECT on the virtual table returns the expected result

### Identifier Case Handling & Quoting
* Testdata: Create a schema/table/column with mixed case (if supported)
* Automatic test: sys tables correct
* Automatic test: SELECT works as expected

### Projection Capability
* Add capability
* Automatic test: pushed down & correct result (incl. EXPLAIN VIRTUAL). Also test with mixed case columns.

### Predicates and Literal Capabilities
* Add capabilities for supported literals and predicates (e.g. c1='foo')
* Automatic test: pushed down & correct result (incl. EXPLAIN VIRTUAL) for all predicates & literals

### Aggregation & Set Function Capabilities
* Add capabilities for aggregations and aggregation functions
* Automatic test: pushed down & correct result (incl. EXPLAIN VIRTUAL) for all set functions

### Order By / Limit Capabilities
* Testdata: Create a table with null values and non-null values, to check null collation.
* Add capabilities for order by and/or limit
* Automatic test: pushed down & correct result (incl. EXPLAIN VIRTUAL)
* Automatic test: default null collation, explicit NULLS FIRST/LAST

### Scalar Function Capabilities
* Add capabilities for scalar functions
* Automatic test: pushed down & correct result (incl. EXPLAIN VIRTUAL)

### Views
* Testdata: Create a simple view, e.g. joining two existing tables
* Automatic test: Query the view, optionally e.g. with a filter.


## How To Start Integration Tests
We assume that you have a running EXASOL and data source database with all required test tables.

We use following Maven phases for our integration tests:
* pre-integration-test phase is used to automatically deploy the latest jdbc adapter jar (based on your latest code modifications)
* integration-test phase is used to execute the actual integration tests

Note that to check whether the integration-tests were successful, you have to run the verify Maven phase.

You can start the integration tests as follows:
```
mvn clean package && mvn verify -Pit -Dintegrationtest.configfile=/path/to/your/integration-test-config.yaml
```

This will run all integration tests, i.e. all junit tests with the suffix "IT" in the filename. The yaml configuration file stores the information for your test environment like jdbc connection strings, paths and credentials.

## Java Remote Debugging of Adapter script

When developing a new dialect it's sometimes really helpful to debug the deployed adapter script inside the database.
In a one node EXASOL environment setting up remote debugging is straight forward.
First define the following env directive in your adapter script:

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter 
  AS
  
  %env JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=y";

  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-1.0.2-SNAPSHOT.jar;
									 
  // You have to add all files of the data source jdbc driver here (e.g. MySQL or Hive)

  %jar /buckets/bucketfs1/bucket1/RedshiftJDBC42-1.2.1.1001.jar;

/
```

In eclipse (or any other Java IDE) you can then attach remotely to the Java Adapter using the IP of your one node EXASOL environment and the port 8000.
With suspend=y the Java-process will wait until the debugger connects to the Java UDF.

