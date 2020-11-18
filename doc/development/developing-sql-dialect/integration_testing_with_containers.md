# Integration Testing with Containers

Virtual Schema integration tests use the `exasol-testcontainers` framework, which requires docker privileged mode to be available to run the tests.

## Overview

The idea of the container based tests is:
* Run the EXASOL and source databases in containers
* Prepare the test schema in the source database
* Create a virtual schema for the source database
* Run the tests on the virtual schema

![Integration test overview](../../images/integrationtest_overview.png)

## Prerequisites

What you need is, for each source database:

* A docker image with user you can connect to
* A JDBC driver for the database

## Preparing Integration Test

1. In order to run the automated integration test, add the test to the includes list of the `maven-failsafe-plugin` in the [pom file](../../../pom.xml).
2. Provide a JDBC driver JAR for the source database.
3. Add a new Integration Test class for you database

### Provide JDBC drivers for the Source Database

The JDBC drivers are automatically deployed during the test. You have to create a directory for the JDBC driver under `src/test/resources/integration/driver`. 
The folder contains the driver jar file(s) and a `settings.cfg` file (for any integration test except Exasol and Postgres).
In order to connect to the source database from your integration test you also have to add the jdbc driver dependency to the [POM](../../../pom.xml) scope verify.

### Add a new Integration Test Class

Add a new class that has to:
* Create the test schema in the source database
* Create the virtual schema
* Execute the tests on the virtual schema
See [PostgreSQLDialectIT](../../../src/test/java/com/exasol/adapter/dialects/postgresql/PostgreSQLSqlDialectIT.java) for an example.

## Security Considerations

In order not to create security issues make sure the data in the source database is not confidential (demo data only).

## Executing Enabled Integration Tests

We use following [Maven life cycle phases](https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html) for our integration tests:

* `pre-integration-test` phase is used to **automatically deploy the latest [JDBC](https://www.exasol.com/support/secure/attachment/66315/EXASOL_JDBC-6.1.rc1.tar.gz) adapter JAR** (based on your latest code modifications)
* `integration-test` phase is used to execute the actual integration tests

Note that to check whether the integration-tests were successful, you have to run the verify Maven phase.

You can start the integration tests as follows:

```bash
mvn clean package && mvn verify
```

This will run all tests included in the `maven-failsafe-plugin` integration test configuration.

Another way to run integration tests:

* Create a package of Virtual Schemas using `mvn package` command and run integration tests inside your IDE in the same way as unit tests.

List of enabled integration tests:

* ExasolSqlDialectIT (in exasol-virtual-schema repository)
* PostgreSQLSqlDialectIT
* SqlServerSqlDialectIT

## Executing Disabled Integration Tests

Some integration tests are not running automatically, but it is possible to execute them locally. 
The reason for the tests being disabled is we can only deliver drivers where the license allows redistribution.
Therefore we cannot include some jdbc drivers to the projects and you need to download them manually for local integration testing.

List of disabled integration tests:

* HiveSqlDialectIT
* MySqlSqlDialectIT
* OracleSqlDialectIT

### Starting Disabled Integration Test Locally

1. Download a JDBC driver and other necessary files: 
 - Hive [`HiveJDBC41.jar`](https://www.cloudera.com/downloads/connectors/hive/jdbc/2-5-4.html)
 - MySQL [`mysql-connector-java-8.0.20.jar`](https://dev.mysql.com/downloads/connector/j/)
 - Oracle [`ojdbc8.jar`](https://www.oracle.com/database/technologies/appdev/jdbc-ucp-19c-downloads.html) and oracle instant client [`instantclient-basic-linux.x64-12.1.0.2.0.zip`](https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html). Please be aware that Exasol currently supports only mentioned version of the Oracle instant client.  
2. Temporarily put the files into `src/test/resources/integration/driver/<dialect lowercase name>` directory. Existing dialect directories: `hive`, `mysql`, `oracle`.

3. If the files' names are different (you renamed the file, or it has a different version number, for example) from the mentioned above, edit `src/test/resources/integration/driver/<dialect lowercase name>/<dialect lowercase name>.properties` and `settings.cfg` files.
4. Run the tests from an IDE or temporarily add the integration test name into the `maven-failsafe-plugin`'s includes a section and execute `mvn verify` command.
5. Remove the driver after the test. Do not upload it to the GitHub repository.

## See also

* [Developing an SQL dialect](developing_a_dialect.md)
* [Remote debugging](../remote_debugging.md)
* [Versioning](../versioning.md)
