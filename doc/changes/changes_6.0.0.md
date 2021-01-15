# Exasol Virtual Schemas 6.0.0, released 2021-XX-XX

Code name: Dialect implementations migration.

## Summary

The following dialect implementations have been migrate to their own repositories:

- Oracle, moved to https://github.com/exasol/oracle-virtual-schema.
- DB2, moved to https://github.com/exasol/db2-virtual-schema.
- SQL Server, moved to https://github.com/exasol/sqlserver-virtual-schema.
- Athena, moved to https://github.com/exasol/athena-virtual-schema.
- Teradata, moved to https://github.com/exasol/teradata-virtual-schema.
- Hive, moved to https://github.com/exasol/hive-virtual-schema.
- Hana, moved to https://github.com/exasol/hana-virtual-schema.
- Big Query, moved to https://github.com/exasol/bigquery-virtual-schema.
- Sybase, moved to https://github.com/exasol/sybase-virtual-schema.

Please we aware you can not create Virtual Schemas of the mentioned above dialects using this JAR anymore.

## Refactoring

* #428: Removed Teradata, Hive and Hana dialects.
* #438: Removed Oracle dialect implementation as it has been migrated to https://github.com/exasol/mysql-virtual-schema.
* #440: Removed DB2 dialect implementation as it has been migrated to https://github.com/exasol/db2-virtual-schema.
* #442: Removed SQL Server dialect implementation as it has been migrated to https://github.com/exasol/sqlserver-virtual-schema.
* #444: Removed Athena dialect implementation as it has been migrated to https://github.com/exasol/athena-virtual-schema.
* #446: Removed Big Query dialect implementation as it has been migrated to https://github.com/exasol/bigquery-virtual-schema.
* #447: Removed Sybase dialect implementation as it has been migrated to https://github.com/exasol/sybase-virtual-schema.

## Dependency updates

* Removed `org.testcontainers:oracle-xe:1.15.0`
* Removed `com.oracle.ojdbc:ojdbc8:19.3.0.0`
* Removed `org.testcontainers:mssqlserver:1.15.0`
* Removed `com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre11`