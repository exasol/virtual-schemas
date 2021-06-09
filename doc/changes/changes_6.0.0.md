# Exasol Virtual Schemas 6.0.0, released 2021-01-18

Code name: Dialects migration.

## Summary

The following dialect implementations have been migrate to their own repositories:

- Oracle, moved to https://github.com/exasol/oracle-virtual-schema.
- DB2, moved to https://github.com/exasol/db2-virtual-schema.
- SQL Server, moved to https://github.com/exasol/sqlserver-virtual-schema.
- Athena, moved to https://github.com/exasol/athena-virtual-schema.
- Hive, moved to https://github.com/exasol/hive-virtual-schema.
- Hana, moved to https://github.com/exasol/hana-virtual-schema.
- Big Query, moved to https://github.com/exasol/bigquery-virtual-schema.
- Sybase, moved to https://github.com/exasol/sybase-virtual-schema.
- Redshift, moved to https://github.com/exasol/redshift-virtual-schema.
- Impala, moved to https://github.com/exasol/impala-virtual-schema.
- Generic, moved to https://github.com/exasol/generic-virtual-schema.

Please we aware you can not create Virtual Schemas of the mentioned above dialects using this JAR anymore.

## Refactoring

* #428: Removed Teradata, Hive and Hana dialects.
* #438: Removed Oracle dialect.
* #440: Removed DB2 dialect.
* #442: Removed SQL Server dialect.
* #444: Removed Athena dialect.
* #446: Removed Big Query, Sybase, Impala and Redshift dialects.
* #451: Removed Generic dialect.


## Documentation

* #458: Added FAQ entry for outdated views

## Dependency updates

* Removed `org.testcontainers:oracle-xe:1.15.0`
* Removed `com.oracle.ojdbc:ojdbc8:19.3.0.0`
* Removed `org.testcontainers:mssqlserver:1.15.0`
* Removed `com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre11`
* Removed `com.exasol:db-fundamentals-java:0.1.1`
* Removed `nl.jqno.equalsverifier:equalsverifier:3.5`
* Removed `com.exasol:exasol-jdbc:7.0.3`
* Removed `com.exasol:exasol-testcontainers:3.3.1`
* Removed `org.testcontainers:junit-jupiter:1.15.0`
* Removed `org.apache.hive:hive-jdbc:3.1.2`
* Removed `org.apache.thrift:libthrift:0.13.0`
* Removed `org.apache.hbase:hbase-server:2.3.3`
* Removed `com.exasol:test-db-builder-java:1.1.0`
* Removed `com.exasol:hamcrest-resultset-matcher:1.2.1`
* Removed `com.exasol:virtual-schema-common-jdbc:8.0.0`
* Removed `org.hamcrest:hamcrest:2.2`
* Removed `org.junit.jupiter:junit-jupiter:5.7.0`
* Removed `org.mockito:mockito-junit-jupiter:3.6.0`
* Removed `junit:junit:4.13.1`
* Removed `org.apache.maven.plugins:maven-surefire-plugin:3.0.0-M4`
* Removed `org.jacoco:jacoco-maven-plugin:0.8.5`
* Removed `org.apache.maven.plugins:maven-compiler-plugin:3.8.1`
* Removed `org.apache.maven.plugins:maven-assembly-plugin:3.3.0`
* Removed `org.apache.maven.plugins:maven-failsafe-plugin:3.0.0-M4`
* Removed `org.codehaus.mojo:versions-maven-plugin:2.8.1`
* Removed `org.sonatype.ossindex.maven:ossindex-maven-plugin:3.1.0`
* Removed `org.apache.maven.plugins:maven-enforcer-plugin:3.0.0-M3`
* Removed `com.exasol:artifact-reference-checker-maven-plugin:0.3.1`