# Exasol Virtual Schemas 6.0.0, released 2021-XX-XX

Code name: Migrated Oracle, DB2 and SQL Server dialect implementations to their own repository.

## Summary

Please we aware you can not create Oracle, DB2 nor SQL Server Virtual Schemas using this JAR anymore.
- Oracle dialect implementation has been migrated to https://github.com/exasol/oracle-virtual-schema.
- DB2 dialect implementation has been migrated to https://github.com/exasol/db2-virtual-schema.
- SQL Server dialect implementation has been migrated to https://github.com/exasol/sqlserver-virtual-schema.

## Refactoring

* #438: Removed Oracle dialect implementation as it has been migrated to https://github.com/exasol/mysql-virtual-schema.
* #440: Removed DB2 dialect implementation as it has been migrated to https://github.com/exasol/db2-virtual-schema.
* #442: Removed SQL Server dialect implementation as it has been migrated to https://github.com/exasol/sqlserver-virtual-schema.

## Dependency updates

* Removed `org.testcontainers:oracle-xe:1.15.0`
* Removed `com.oracle.ojdbc:ojdbc8:19.3.0.0`
* Removed `org.testcontainers:mssqlserver:1.15.0`
* Removed `com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre11`