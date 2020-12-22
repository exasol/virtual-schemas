# Exasol Virtual Schemas 4.0.5, released 2020-12-22

Code name: Migrated PostgreSQL dialect implementation to its own repository.

## Summary

Please we aware you can not create PostgreSQL Virtual Schemas using this JAR anymore.
PostgreSQL dialect implementation has been migrated to https://github.com/exasol/postgresql-virtual-schema.

We also enabled `JOIN` capabilities in `MySQL` dialect adapter, and started a FAQ documentation page.

## Features

* #433: Enabled `JOIN` capabilities in `MySQL` dialect adapter.

## Bug Fixes

* #427: Excluded transitive `org.apache.httpcomponents:httpclient` dependency to avoid CVE-2020-13956.

## Documentation

* #408: Removed PostgreSQL dialect documentation as it has been migrated to https://github.com/exasol/postgresql-virtual-schema.
* #413: Improved documentation on `ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE`
* #402: Updated the documentation about implementing a new dialect.
* #426: Started FAQ documentation for Virtual Schemas.

## Refactoring

* #408: Removed PostgreSQL dialect implementation as it has been migrated to https://github.com/exasol/postgresql-virtual-schema.
* #422: Refactored `OracleQueryRewriter` for more clarity.
* #424: Removed `PostgreSQL` dialect leftovers.

## Dependency updates

* Updated `com.exasol:virtual-schema-common-jdbc:7.0.0` to `8.0.0`
* Removed `org.postgresql:postgresql:42.2.18`
* Removed `org.testcontainers:postgresql:1.15.0`
* Removed `org.apache.httpcomponents:httpclient`