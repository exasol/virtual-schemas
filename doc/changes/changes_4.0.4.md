# Exasol Virtual Schemas 4.0.4, released 2020-??-??

Code name:

## Documentation

* #371: Documented data types conversion for Hive dialect.
* #377: Improved Scalar Functions API documentation.
* #384: Turned embedded JSON into key-value encoding in Adapter Notes API examples.
* #386: Remove the documentation that was moved to the portal, added links instead.

## Refactoring

* #263: Removed SybaseMetadataReader class as it was not used by the dialect.
* #381: Migrated from version.sh to artifact-reference-checker-maven-plugin

## Dependency updates

* Updated `org.apache.hbase:hbase-server` from 2.3.0 to 2.3.1
* Added com.exasol:artifact-reference-checker-maven-plugin:0.3.1