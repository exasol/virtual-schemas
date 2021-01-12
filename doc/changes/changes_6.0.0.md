# Exasol Virtual Schemas 6.0.0, released 2021-XX-XX

Code name: Migrated Oracle dialect implementation to its own repository.

## Summary

Please we aware you can not create Oracle Virtual Schemas using this JAR anymore.
Oracle dialect implementation has been migrated to https://github.com/exasol/oracle-virtual-schema.

## Refactoring

* #438: Removed Oracle dialect implementation as it has been migrated to https://github.com/exasol/mysql-virtual-schema.

## Dependency updates

* Removed `org.testcontainers:oracle-xe:1.15.0`
* Removed `com.oracle.ojdbc:ojdbc8:19.3.0.0`