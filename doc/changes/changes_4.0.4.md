# Exasol Virtual Schemas 4.0.4, released 2020-11-13

Code name: Important bugfixes

## Summary

In this release we fixed a few bugs, including a security issue. Please update your adapters as soon as possible.

## Documentation

* #371: Documented data types conversion for Hive dialect.
* #377: Improved Scalar Functions API documentation.	
* #384: Turned embedded JSON into key-value encoding in Adapter Notes API examples.	
* #386: Remove the documentation that was moved to the portal, added links instead.
* #394: Described 'No suitable driver found', added a note that Hive 1.1.0 has problems with its driver.
* #391: Removed the API documentation from this repository and added a link to it.

## Refactoring

* #263: Removed SybaseMetadataReader class as it was not used by the dialect.
* #381: Migrated from version.sh to artifact-reference-checker-maven-plugin.
* #389: Improved connection error handling.
* #396: Updated to the `virtual-schema-common-jdbc:6.0.0`
* #401: Updated to the `virtual-schema-common-jdbc:7.0.0`

## Dependency updates

* Added com.exasol:artifact-reference-checker-maven-plugin:0.3.1
* Updated com.exasol:virtual-schema-common-jdbc:5.0.4 to 7.0.0
* Updated org.apache.hbase:hbase-server:2.3.0 to 2.3.3
* Updated org.junit.jupiter:junit-jupiter:5.6.2 to 5.7.0
* Updated org.mockito:mockito-junit-jupiter:3.4.6 to 3.6.0
* Updated com.exasol:exasol-jdbc:6.2.5 to 7.0.3
* Updated com.exasol:exasol-testcontainers:2.1.0 to 3.3.0
* Updated org.postgresql:postgresql:42.2.14 to 42.2.18
* Updated org.apache.hbase:hbase-server:2.3.1 to 2.3.2
* Updated com.microsoft.sqlserver:mssql-jdbc:8.4.0.jre11 to 8.4.1.jre11
* Updated com.exasol:test-db-builder-java:1.0.1 to 1.1.0
* Updated com.exasol:hamcrest-resultset-matcher:1.1.1 to 1.2.1
* Updated nl.jqno.equalsverifier:equalsverifier:3.4.3 to 3.5
* Updated mysql:mysql-connector-java:8.0.21 to 8.0.22
* Updated org.testcontainers:junit-jupiter:1.14.3 to 1.15.0
* Updated org.testcontainers:mssqlserver:1.14.3 to 1.15.0
* Updated org.testcontainers:mysql:1.14.3 to 1.15.0
* Updated org.testcontainers:oracle-xe:1.14.3 to 1.15.0
* Updated org.testcontainers:postgresql:1.14.3 to 1.15.0