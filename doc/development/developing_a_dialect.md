# How To Develop and Test a SQL Dialect Adapter

This article describes how you can develop and test an SQL dialect adapter based on the Virtual Schema JDBC adapter.

## Content

* [Developing a Dialect](#developing-a-dialect)
* [Integration Testing with Containers](#integration-testing-with-containers)

## Introduction

Before you start writing your own SQL adapter that integrates Virtual Schemas with the SQL dialect a specific data source uses, we first need to briefly discuss how Virtual Schemas are structured in general and the JDBC adapter in particular.

[Adapters](https://www.gofpatterns.com/structural-design-patterns/structural-patterns/adapter-pattern.php) (also known as wrappers) are a piece of code that enable interaction between two previously incompatible objects by planting an adapter layer in between that serves as a translator. In our case a Virtual Schema adapter implements an API defined by Exasol Virtual Schemas and translates all data accesses and type conversions between the adapted source and the Exasol database.

In the case of the JDBC adapter there are _two_ different adapter layers in between Exasol and the source. The first one from Exasol's perspective is the JDBC adapter which contains the common part of the translation between Exasol and a source for which a JDBC driver exists. The second layer is a SQL dialect adapter, that evens out the specialties of the source databases.

The name SQL dialect adapter is derived from the non-standard implementation parts of SQL databases which are often referred to as "dialects" of the SQL language.

As an example, PostgreSQL handles some of the data types subtly different from Exasol and the SQL dialect adapter needs to deal with those differences by implementing conversion functions.

    .-------------------------.
    |        PostgreSQL       |   External data source
    |-------------------------|
    |   SQL Dialect Adapter   |   Even out specifics of the source database
    |-------------------------|
    |     JDBC  Adapter       |   Common JDBC functions
    |-------------------------|
    |    Virtual Schema API   |
    |-------------------------|
    |         Exasol          |
    '-------------------------'

For more information about the structure of the Virtual Schemas check the UML diagrams provided in the directory [model/diagrams](model/diagrams). You either need [PlantUML](http://plantuml.com/) to render them or an editor that has PlamtUML preview built in.

## Developing a Dialect

If you want to write an SQL dialect, you need to start by implementing the dialect adapter interfaces.

### Project Structure

This repository contains Maven sub-projects that are structured as follows. 

    jdbc-adapter                               Parent project and integration test framework
      |
      |-- virtualschema-jdbc-adapter           The actual implementation files
      |
      '-- virtualschema-jdbc-adapter-dist      Environment for creating the all-in-one adapter JAR

### Package Structure

The Java package structure of the `virtualschema-jdbc-adapter` reflects the separation into dialect-independent and dialect-specific parts. 

    com.exasol.adapter
      |
      |-- dialects                             Common code for all dialect adapters
      |     |
      |     |-- db2                            IBM DB2-specific dialect adapter implementation
      |     |
      |     |-- exasol                         Exasol-specific dialect adapter implementation
      |     |
      |     |-- hive                           Apache-Hive-specific dialect adapter implementation
      |     |
      |     '-- ...
      |
      '-- jdbc                                 Base implementation for getting metadata from JDBC

### Interfaces

* [`com.exasol.adapter.dialects.SqlDialect`](jdbc-adapter/virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/dialects/SqlDialect.java) (implementation mandatory)
   * Define capabilities
   * Define which kind of support the dialect has for catalogs and schemas
* [`com.exasol.adapter.jdbc.RemoteMetadataReader`](jdbc-adapter/virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/jdbc/RemoteMetadataReader.java) (optional depending on dialect)
   * Read top-level metadata
   * Locate tables
* [`com.exasol.adapter.jdbc.TableMetadataReader`](jdbc-adapter/virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/jdbc/TableMetadataReader.java) (optional depending on dialect)
   * Decide which tables should be mapped
   * Map data on table level
* [`com.exasol.adapter.jdbc.ColumnMetadataReader`](jdbc-adapter/virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/jdbc/ColumnMetadataReader.java) (optional depending on dialect)
   * Map data on column level

### Registering the Dialect

The Virtual Schema adapter creates an instance of an SQL dialect on demand. You can pick any dialect that is listed in the `SqlDialects` registry.

To register your new dialect add it to the list in [sql_dialects.properties](../virtualschema-jdbc-adapter/src/main/resources/sql_dialects.properties).

```properties
com.exasol.adapter.dialects.supported=\
...
com.exasol.adapter.dialects.myawesomedialect.MyAweSomeSqlDialect
```

For tests or in case you want to exclude existing dialects in certain scenarios you can override the contents of this file 
by setting the system property `com.exasol.adapter.dialects.supported`.

Please also remember to [list the supported dialect in the documentation](../README.md). <!-- FIXME -->

### Setup Data Source

* Setup and start the database
* Testdata: Create a test schema with a simple table (simple data types)

### Setup Exasol

* Setup and start an Exasol database with virtual schemas feature
* Upload the JDBC drivers of the data source via EXAOperation
* Manual test: query data from the data source via `IMPORT FROM JDBC`

### Catalog, Schema & Table Mapping

* Override the `SqlDialect` methods for catalog, schema and table mapping
* Manual test: create a virtual schema by specifying the catalog and/or schema.

### Data Type Mapping

* Testdata: Create a table with all data types and at least one row of data
* Override the `SqlDialect` method for data type mapping
* Automatic test: sys tables show virtual table and columns with correctly mapped type
* Automatic test: running `SELECT` on the virtual table returns the expected result

### Identifier Case Handling & Quoting

* Testdata: Create a schema/table/column with mixed case (if supported)
* Automatic test: sys tables correct
* Automatic test: `SELECT` works as expected

### Projection Capability

* Add capability
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`). Also test with mixed case columns.

### Predicates and Literal Capabilities

* Add capabilities for supported literals and predicates (e.g. `c1='foo'`)
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`) for all predicates & literals

### Aggregation & Set Function Capabilities

* Add capabilities for aggregations and aggregation functions
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`) for all set functions

### Order By / Limit Capabilities

* Testdata: Create a table with null values and non-null values, to check null collation.
* Add capabilities for order by and/or limit
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`)
* Automatic test: default null collation, explicit `NULLS FIRST/LAST`

### Scalar Function Capabilities

* Add capabilities for scalar functions
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`)

### Views

* Testdata: Create a simple view, e.g. joining two existing tables
* Automatic test: Query the view, optionally e.g. with a filter.

## Integration Testing with Containers

This integration test works fully automated, there are no manual steps in setting up the source database and the connection to EXASOL.

### Overview

The idea of the container based tests is:
* Run the EXASOL and source databases in containers
* Prepare the test schema in the source database
* Create a virtual schema for the source database
* Run the tests on the virtual schmema

![Integration test overview](images/integrationtest_overview.png)


### Prerequisites

What you need is, for each source database:

* A docker image with user you can connect to
* A JDBC driver for the database

### Preparing Integration Test

1. In order to run the integration test automated, edit the  [Travis CI integration test configuration file](../integration-test-data/integration-test-travis.yaml) and add your new database.
2. Provide a JDBC driver JAR for the source database.
3. Add a new Integration Test class for you database

#### Add Your Database to the Test Configuration
Set the following properties for your database:

| configuration property | explanation |
|------------------------|-------------|
| runIntegrationTests |    enable/disable your test (e.g. true)|
| jdbcDriverPath |         path to the jdbc driver in bucketFS (e.g /buckets/bfsdefault/default/drivers/jdbc/POSTGRESQL/postgresql-42.2.5.jar;)|
| connectionString |       connection string to connect to the source database from the integration test system, so use the exposed port (e.g. jdbc:postgresql://localhost:45432/postgres)|
| user |                   the database user|
| password |               password for the database user|
| dockerImage |            name of the docker image for the source db|
| dockerImageVersion |     version of the used docker image (eg. latest)|
| dockerPortMapping |      docker port mapping external_db_port:internal_db_port (e.g. 45432:5432)|
| dockerName |             name for the docker container (e.g. testpg)|
| dockerConnectionString | connection string to connect to the source db from the EXASOL docker container. Use the constant DBHOST as hostname, this will be set to the actual internal docker network IP by the integration test skript during runtime (e.g. jdbc:postgresql://DBHOST:5432/postgres)|

#### Provide JDBC drivers for the Source Database

The JDBC drivers are automatically deployed during the test. You have to create a directory for the jdbc driver under integration-test-data/drivers. The folder contains the driver jar file(s) and a config file. See the [PostgreSQL config](../integration-test-data/drivers/POSTGRESQL/settings.cfg) for an example.

In order to connect to the source database from your integration test you also have to add the jdbc driver dependency to the [POM](../virtualschema-jdbc-adapter/pom.xml) scope verify.

#### Add a new Integration Test Class

Add a new class that derives from [AbstractIntegrationTest](../virtualschema-jdbc-adapter/src/test/java/com/exasol/adapter/dialects/AbstractIntegrationTest.java). This class has to:
* Create the test schema in the source database
* Create the virtual schema
* Execute the tests on the virtual schema
See [PostgreSQLDialectIT](../virtualschema-jdbc-adapter/src/test/java/com/exasol/adapter/dialects/postgresql/PostgreSQLDialectIT.java) for an example.

### Executing Integration Tests

Executing the integration test is easy, just run the [integration test bash script](../integration-test-data/run_integration_tests.sh)

## Integration Testing against a local database

If you don't have a container for the source database, you can test against a local database

### Security Considerations

Please note that in the course of the integration tests you need to provide the test framework with access rights and credentials to the source database. 

In order not to create security issues:

* Make sure the data in the source database is not confidential (demo data only)
* Don't reuse credentials
* Don't check in credentials

### Prerequisites

* Exasol running
* Exasol accessible from within integration test environment
* Source database running
* Source database accessible from within integration test environment
* Test data loaded into source database
* [BucketFS HTTP port listening and reachable](https://www.exasol.com/support/browse/SOL-503?src=confmacro) (e.g. on port 2580)

  ![BucketFS on port 2580](images/Screenshot_BucketFS_default_service.png)
  
* Bucket on BucketFS prepared for holding JDBC drivers and virtual schema adapter

  ![Integration test bucket](images/Screenshot_bucket_for_JARs.png)

* JDBC driver JAR archives available for databases against which to run integration tests

If BucketFS is new to you, there are nice [training videos on BucketFS](https://www.exasol.com/portal/display/TRAINING/BucketFS) available.

### Preparing Integration Test

1. Create a dedicated user in the source database that has the necessary access privileges 
2. Create credentials for the user under which the integration tests run at the source
3. Make a local copy of the [sample integration test configuration file](../integration-test-data/integration-test-sample.yaml) in a place where you don't accidentally check this file in.
4. Edit the credentials information
5. [Deploy the JDBC driver(s)](deploying_the_virtual_schema_adapter.md#deploying-jdbc-driver-files) to the prepared bucket in Exasol's BucketFS       

#### Creating Your own Integration Test Configuration

Directories called `local` are ignored by Git, so you can place your configuration there and avoid having it checked in.

In the root directory of the adapter sources execute the following commands:

```bash
mkdir jdbc-adapter/local
cp jdbc-adapter/integration-test-data/integration-test-sample.yaml jdbc-adapter/local/integration-test-config.yaml
```

Now edit the file `jdbc-adapter/local/integration-test-config.yaml` to adapt the settings to your local installation.

### Executing Integration Tests

We use following [Maven lifecycle phases](https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html) for our integration tests:

* `pre-integration-test` phase is used to **automatically deploy the latest [JDBC](https://www.exasol.com/support/secure/attachment/66315/EXASOL_JDBC-6.1.rc1.tar.gz) adapter JAR** (based on your latest code modifications)
* `integration-test` phase is used to execute the actual integration tests

Note that to check whether the integration-tests were successful, you have to run the verify Maven phase.

You can start the integration tests as follows:

```bash
mvn clean package && mvn verify -Pit -Dintegrationtest.configfile=/path/to/your/integration-test-config.yaml
```

This will run all integration tests, i.e. all JUnit tests with the suffix `IT` in the filename.

The YAML configuration file stores the information for your test environment like JDBC connection strings, paths and credentials.

## Java Remote Debugging of Adapter script

When developing a new dialect it is sometimes really helpful to debug the deployed adapter script inside the database.
In a one node Exasol environment setting up remote debugging is straight forward.
First define the following `env` directive in your adapter script:

```sql
CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter.jdbc_adapter 
  AS
  
  %env JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=y";

  // This is the class implementing the callback method of the adapter script
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // This will add the adapter jar to the classpath so that it can be used inside the adapter script
  // Replace the names of the bucketfs and the bucket with the ones you used.
  %jar /buckets/bucketfs1/bucket1/virtualschema-jdbc-adapter-dist-1.10.0.jar;
									 
  // You have to add all files of the data source jdbc driver here (e.g. MySQL or Hive)

  %jar /buckets/bucketfs1/bucket1/RedshiftJDBC42-1.2.1.1001.jar;

/
```

In eclipse (or any other Java IDE) you can then attach remotely to the Java Adapter using the IP of your one node Exasol environment and the port 8000.

The switch `suspend=y` tells the Java-process to wait until the debugger connects to the Java UDF.

## Version Management

All dialects have the same version as the master project. In the master `pom.xml` file a property called `product-version` is set. Use this in as the artifact version number in the JDBC adapter and all dialects.

Run the script

```bash
jdbc-adapter/tools/version.sh verify
```

To check that all documentation and templates reference the same version number. This script is also used as a build breaker in the continuous integration script.

To update documentation files run

```bash
jdbc-adapter/tools/version.sh unify
```

Note that the script must be run from the root directory of the virtual schema project.
