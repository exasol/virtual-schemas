## Versioning Virtual Schemas and SQL Dialects

All dialects have the same version as the master project. In the master `pom.xml` file a property called `product-version` is set. Use this in as the artifact version number in the JDBC adapter and all dialects.

This project uses the [artifact-reference-checker-maven-plugin](https://github.com/exasol/artifact-reference-checker-maven-plugin/) to validate and unify references from source code.