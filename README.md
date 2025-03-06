# Virtual Schemas

# Overview

Exasol **Virtual Schemas** are an abstraction layer that makes external data sources accessible in our data analytics platform through regular SQL commands. The contents of the external data sources are mapped to virtual tables which look like and can be queried as any regular Exasol table.

Regardless of whether that source is a relational database like ours, or it's like the structure of GitHub repositories, the interface that users see is always the same.

This means a user familiar with SQL will immediately feel at home when accessing remote data through Virtual Schemas.

Virtual Schemas can be also described in known terms as `External Tables` or `Foreign Data Wrapper (FDW)`.

## Features

* Read only access to data on remote data sources (see the Supported Data Sources below)
* Data in those sources appears as tables inside Exasol and can be queried using regular SQL statements.
* Pushes down queries to the remote source (some sources)
* Supports sources with no / one / multiple catalogs or schemas
* Allows limiting metadata mapping to selected catalogs and / or schemas
* Allows redirecting log output to a remote machine
* Allows remote debugging with the Java Debugger

## Supported Data Sources

See [List of Supported Dialects](doc/user_guide/dialects.md).

## Limitations

* A Virtual Schema adapter written in Python or Java takes about 1s to start. That means that queries that involve tables from Virtual Schema will take at least 1s.

## Customer Support

This is an open source project officially supported by Exasol. Please contact our support team if you have any questions.

**NOTE: Please report dialect specific issues in the corresponding dialect repository (see the Supported Data Sources above)**

# Table of Contents

## Information for Users

* [User Guide](https://docs.exasol.com/database_concepts/virtual_schemas.htm)
* [Virtual Schema's Properties Reference](https://docs.exasol.com/database_concepts/virtual_schema/adapter_properties.htm)
* [Remote Logging](https://docs.exasol.com/database_concepts/virtual_schema/logging.htm)
* [Frequently Asked Questions (FAQ) for Virtual Schema Users](doc/user_guide/faq.md)

Additional resources:

* [Troubleshooting](doc/user_guide/troubleshooting.md)
* [Changelog](doc/changes/changelog.md)
* [Virtual Schema Privileges](https://docs.exasol.com/database_concepts/virtual_schema/virtual_schema_privilege.htm)

## Information for Developers

* [Frequently Asked Questions (FAQ) for Virtual Schema Developers](developer_guide/faq.md)

### Java-specific Developer Information

* Find all developers information in [Virtual Schema Common JDBC repository](https://github.com/exasol/virtual-schema-common-jdbc#information-for-developers)
