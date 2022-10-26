# Virtual Schemas

<img alt="virtual-schemas logo" src="doc/images/virtual-schemas_128x128.png" style="float:left; padding:0px 10px 10px 10px;"/>

# Overview

Exasol **Virtual Schemas** are an abstraction layer that makes external data sources accessible in our data analytics platform through regular SQL commands. The contents of the external data sources are mapped to virtual tables which look like and can be queried as any regular Exasol table.

Regardless of whether that source is a relational database like ours, or it's like the structure of GitHub repositories, the interface that users see is always the same.

This means an user familiar with SQL will immediately feel at home when accessing remote data through Virtual Schemas.

Virtual Schemas can be also described in known terms as `External Tables` or `Foreign Data Wrapper (FDW)`.


## Version Requirements

The version of a Virtual Schema consists of two parts. The first part is the version of the [Virtual Schema JDBC](https://github.com/exasol/virtual-schema-common-jdbc/releases), the common basis that all JDBC-based Virtual Schemas share. Whereas the second part is the version of the Virtual Schema itself.

Thus the JAR filename of a Virtual Schema release complies with the following format:

    virtual-schema-dist-<Virtual Schema JDBC Version>-<Virtual  Schema Name>-<Virtual Schema Version>.jar

For example, the JAR filename of the [Oracle Virtual Schema 2.3.0 release](https://github.com/exasol/oracle-virtual-schema/releases/tag/2.3.0), which is based on the [10.0.1 version of Virtual Schema JDBC](https://github.com/exasol/virtual-schema-common-jdbc/releases/tag/10.0.1), is:

    virtual-schema-dist-10.0.1-oracle-2.3.0.jar

The version of the Virtual Schema JDBC on which a Virtual Schema is based also tells you whether it is supported or discontinued, as shown in the following table:

Virtual Schema JDBC Version | Required Java Version | Lifecycle
----------------------------|-----------------------|--------------------------------
10.x.x                      |                    11 | supported, active development
9.x.x                       |                    11 | supported
8.x.x                       |                    11 | discontinued
7.x.x                       |                    11 | discontinued
6.x.x                       |                    11 | discontinued
5.x.x                       |                    11 | discontinued
3.x.x                       |                    11 | discontinued
2.x.x                       |                     9 | discontinued
1.x.x                       |                     8 | discontinued

Please update your Virtual Schema to a supported version before writing tickets or contacting Exasol Support.

Exasol Version  | Java Version Installed by Default in Language Container
----------------|--------------------------------------------------------
7.1             | 11
7.0             | 11
6.2             | 11
6.1             | 9
6.0             | 8

We recommend updating the Exasol installation to at least 7.1.10 or newer for best results.

Please do not try to install language containers with lower Java versions on newer Exasol installations. This is untested.

Please contact the [Exasol Support Team](https://www.exasol.com/portal/display/EXA/Support+Dashboard) if you need help upgrading the language container.

The runtime dependencies are specified for each dialect in their own repository.

## Deprecation Warning

Please use the entry point

    com.exasol.adapter.RequestDispatcher

in all your `CREATE JAVA ADAPTER SCRIPT` statements. The old entry point was removed with the Version 2.0.0 of Exasol's Virtual Schema.

## Features

* Read only access to data on remote data sources (see the Supported Data Sources below)
* Data in those sources appears as tables inside Exasol and can be queried using regular SQL statements.
* Pushes down queries to the remote source (some sources)
* Supports sources with no / one / multiple catalogs or schemas
* Allows limiting metadata mapping to selected catalogs and / or schemas
* Allows redirecting log output to a remote machine
* Allows remote debugging with the Java Debugger

## Supported Data Sources

See [List of Supported Dialects](doc/user-guide/dialects.md).

## Limitations

* The Virtual Schema adapter takes about 1s to start.
That means that queries that involve tables from Virtual Schema will take at least 1s.

## Customer Support

This is an open source project officially supported by Exasol. Please contact our support team if you have any questions.

**NOTE: Please report dialect specific issues in the corresponding dialect repository (see the Supported Data Sources above)**

# Table of Contents

## Information for Users

* [User Guide](https://docs.exasol.com/database_concepts/virtual_schemas.htm)
* [Virtual Schema's Properties Reference](https://docs.exasol.com/database_concepts/virtual_schema/adapter_properties.htm)
* [Remote Logging](https://docs.exasol.com/database_concepts/virtual_schema/logging.htm)
* [FAQ](doc/user-guide/faq.md)

Additional resources:

* [Troubleshooting](doc/user-guide/troubleshooting.md)
* [Changelog](doc/changes/changelog.md)
* [Virtual Schema Privileges](https://docs.exasol.com/database_concepts/virtual_schema/virtual_schema_privilege.htm)

## Information for Developers

* Find all developers information in [Virtual Schema Common JDBC repository][developers-information].

[developers-information]: https://github.com/exasol/virtual-schema-common-jdbc#information-for-developers
