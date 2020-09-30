# Exasol Virtual Schemas 4.0.4, released 2020-??-??

Code name:

## Documentation

* #371: Documented data types conversion for Hive dialect.
* #377: Improved Scalar Functions API documentation.	
* #384: Turned embedded JSON into key-value encoding in Adapter Notes API examples.	
* #386: Remove the documentation that was moved to the portal, added links instead.
* #394: Described 'No  suitable driver found', added a note that Hive 1.1.0 has problems with its driver.

## Refactoring

* #263: Removed SybaseMetadataReader class as it was not used by the dialect.
* #381: Migrated from version.sh to artifact-reference-checker-maven-plugin.
* #389: Improved connection error handling.
* #396: Updated to the `virtual-schema-common-java:6.0.0`

## Dependency updates

* Added com.exasol:artifact-reference-checker-maven-plugin:0.3.1
* Updated com.exasol:virtual-schema-common-java:jar:5.0.4 to version 6.0.0
* Updated org.apache.hbase:hbase-server:jar:2.3.0 to version 2.3.1
* Updated org.junit.jupiter:junit-jupiter:jar:5.6.2 to version 5.7.0
* Updated org.mockito:mockito-junit-jupiter:jar:3.4.6 to version 3.5.13
* Updated com.exasol:exasol-jdbc:jar:6.2.5 to version 7.0.0
* Updated com.exasol:exasol-testcontainers:jar:2.1.0 to version 3.1.0
* Updated org.postgresql:postgresql:jar:42.2.14 to version 42.2.16
* Updated org.apache.hbase:hbase-server:jar:2.3.1 to version 2.3.2
* Updated com.microsoft.sqlserver:mssql-jdbc:jar:8.4.0.jre11 to version 8.4.1.jre11
* Updated com.exasol:test-db-builder-java:jar:1.0.1 to version 1.1.0
* Updated com.exasol:hamcrest-resultset-matcher:jar:1.1.1 to version 1.2.1