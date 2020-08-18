# Exasol Virtual Schemas 4.0.2, released 2020-07-06

## Features / Enhancements

* #350: Improved Hive Kerberos documentation with HTTP protocol usage and troubleshooting findings.

## Refactoring

* #352: Updated dependencies.

## Dependency updates

* Updated `com.exasol:exasol-testcontainers` from 2.0.2 to 2.0.3
* Updated `com.exasol:hamcrest-resultset-matcher` from 1.1.0 to 1.1.1
* Updated `com.exasol:test-db-builder-java` from 1.0.0 to 1.0.1
* Updated `com.exasol:virtual-schema-common-jdbc` from 5.0.1 to 5.0.2
* Updated `org.testcontainers:junit-jupiter` from 1.14.2 to 1.14.3
* Updated `org.testcontainers:mysql` from 1.14.2 to 1.14.3
* Updated `org.testcontainers:oracle-xe` from 1.14.2 to 1.14.3
* Updated `org.testcontainers:postgresql` from 1.14.2 to 1.14.3
* Removed `hive-jdbc` transitive dependency `org.apache.logging.log4j:log4j-core`
* Removed `hbase-server` transitive dependency `com.nimbusds:nimbus-jose-jwt`
