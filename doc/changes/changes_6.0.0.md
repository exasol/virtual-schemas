# Exasol Virtual Schemas 6.0.0, released 2021-XX-XX

Code name: Migrated Oracle, DB2, SQL Server, Teradata, Hana, Hive and Athena dialect implementations to their own repositories.

## Summary

Please we aware you can not create Oracle, DB2 nor SQL Server Virtual Schemas using this JAR anymore.
- Oracle dialect implementation has been migrated to https://github.com/exasol/oracle-virtual-schema.
- DB2 dialect implementation has been migrated to https://github.com/exasol/db2-virtual-schema.
- SQL Server dialect implementation has been migrated to https://github.com/exasol/sqlserver-virtual-schema.
- Athena dialect implementation has been migrated to https://github.com/exasol/athena-virtual-schema.
- Teradata dialect implementation has been migrated to https://github.com/exasol/teradata-virtual-schema.
- Hive dialect implementation has been migrated to https://github.com/exasol/hive-virtual-schema.
- Hana dialect implementation has been migrated to https://github.com/exasol/hana-virtual-schema.

## Refactoring

* #438: Removed Oracle dialect implementation as it has been migrated to https://github.com/exasol/mysql-virtual-schema.
* #440: Removed DB2 dialect implementation as it has been migrated to https://github.com/exasol/db2-virtual-schema.
* #442: Removed SQL Server dialect implementation as it has been migrated to https://github.com/exasol/sqlserver-virtual-schema.
* #444: Removed Athena dialect implementation as it has been migrated to https://github.com/exasol/athena-virtual-schema.
* #428: Removed Teradata, Hive and Hana dialects.

## Dependency updates

* Removed `org.testcontainers:oracle-xe:1.15.0`
* Removed `com.oracle.ojdbc:ojdbc8:19.3.0.0`
* Removed `org.testcontainers:mssqlserver:1.15.0`
* Removed `com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre11`