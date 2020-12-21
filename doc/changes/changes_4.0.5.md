# Exasol Virtual Schemas 4.0.5, released 2020-??-??

Code name: 

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