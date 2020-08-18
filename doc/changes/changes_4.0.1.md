# Exasol Virtual Schemas 4.0.1, released 25.06.2020

## Summary 

In this release we introduced the security risks check with `ossindex-maven-plugin`. We removed the reported security risks by updating or excluding third-party transient test dependencies. Those were used in the integration tests of the Virtual Schema for Hive.

We also adjusted the naming for the attached JAR file:

Old naming pattern: `virtualschema-jdbc-adapter-dist-<dialects version>.jar`

New naming pattern: `virtual-schema-dist-<virtual schema common jdbc version>-bundle-<dialects version>.jar`

The new naming pattern helps users to distinguish minor dialect changes from major adapter changes affecting all dialects.

## Bug Fixes
 
* #336: Fixed broken links in the documentation.
* #343: Added ossindex-maven-plugin plugin and removed found security risks in transient dependencies.

## Features / Enhancements
 
* #340: Added MySQL dialect integration test.
* #346: Added `changelog.md` file to the documentation for keeping tracking of changes outside of the github.
* #348: Added Version Maven Plugin for tracking new available versions of dependencies and plugins, updated dependencies

## Dependency updates
 
* Added `org.apache.thrift:libthrift:0.13.0`
* Added `org.sonatype.ossindex.maven:ossindex-maven-plugin:3.1.0`
* Added `org.testcontainers:mysql:1.13.0`
* Added `mysql:mysql-connector-java:8.0.20`
* Added `com:exasol:test-db-builder-java:1.0.0`
* Added `com.exasol:hamcrest-resultset-matcher:1.1.0`
* Added `org.codehaus.mojo:versions-maven-plugin:2.7`
* Added `org.apache.maven.plugins:maven-enforcer-plugin:3.0.0-M3`
* Updated `org.apache.hbase:hbase-server` from 2.2.4 to 2.2.5
* Updated `com.exasol:virtual-schema-common-jdbc` from 5.0.0 to 5.0.1
* Updated `com.exasol:exasol-testcontainers` from 2.0.0 to 2.0.2
* Updated `maven-assembly-plugin` from 3.2.0 to 3.3.0
* Updated `org.postgresql:postgresql` from 42.2.12 to 42.2.14
* Updated `org.junit.jupiter:junit-jupiter-engine` from 5.6.1 to 5.6.2
* Updated `org.junit.jupiter:junit-jupiter-params` from 5.6.1 to 5.6.2
* Updated `org.testcontainers:junit-jupiter` from 1.13.0 to 1.14.2
* Updated `org.testcontainers:postgresql` from 1.13.0 to 1.14.2
* Updated `org.testcontainers:oracle-xe` from 1.13.0 to 1.14.2
* Updated `org.testcontainers:mysql` from 1.13.0 to 1.14.2
* Removed `org.itsallcode:junit5-system-extensions`
* Removed `com.exasol:virtual-schema-common-java`
* Removed transient `org.eclipse.jetty:*`
* Removed transient `io.netty:*`
* Removed transient `org.codehaus.jackson:jackson-mapper-asl`
* Removed transient `com.fasterxml.jackson.core:jackson-databind`
* Removed transient `com.google.guava:guava`
* Removed transient `org.apache.hadoop:hadoop-yarn-server-resourcemanager`
* Removed transient `org.apache.derby:*`
* Removed transient `org.apache.zookeeper:zookeeper`
* Removed transient `org.apache.thrift:libfb303*`
* Removed transient `com.google.protobuf:protobuf-java*`
* Removed transient `com.squareup.okhttp:okhttp`
* Removed transient `org.mortbay.jetty:*`