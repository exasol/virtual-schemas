# Exasol Virtual Schemas 4.0.5, released 2020-??-??

Code name: 

## Documentation

* #408: Removed PostgreSQL dialect documentation as it has been migrated to https://github.com/exasol/postgresql-virtual-schema.
* #413: Improved documentation on `ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE`
* #402: Updated the documentation about implementing a new dialect.

## Refactoring

* #408: Removed PostgreSQL dialect implementation as it has been migrated to https://github.com/exasol/postgresql-virtual-schema.
* #422: Refactor `OracleQueryRewriter` for more clarity.
* #424: Remove `PostgreSQL` dialect leftovers.

## Dependency updates

* Removed org.postgresql:postgresql:42.2.18
* Removed org.testcontainers:postgresql:1.15.0
* Updated `com.exasol:virtual-schema-common-jdbc:7.0.0` to `8.0.0`
