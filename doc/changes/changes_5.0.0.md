# Exasol Virtual Schemas 5.0.0, released 2021-01-11

Code name: Migrated MySQL dialect implementation to its own repository.

## Summary

Please we aware you can not create MySQL Virtual Schemas using this JAR anymore.
MySQL dialect implementation has been migrated to https://github.com/exasol/mysql-virtual-schema.

## Refactoring

* #431: Removed MySQL dialect implementation as it has been migrated to https://github.com/exasol/mysql-virtual-schema.

## Dependency updates

* Removed `org.testcontainers:mysql:1.15.0`
* Removed `mysql:mysql-connector-java:8.0.22`
