# Static Code Analysis

This Open Source project uses [SonarCloud](https://sonarcloud.io/) as static code analysis tool.

## Rules and Exceptions

We are using Sonar's **standard rule set** with the following exceptions:

### Code Duplication Check Exceptions

The SQL dialects and the metadata readers that are part of them are in many parts quite similar. Most duplications are one-liners that configure the behavior of a SQL dialect. This can't be reduced any further without sacrificing self-documentation of dialects and proper abstraction.

Even if the duplications are only configurations and they are small, the numbers add up.

Sonar does not support `@SuppressWarnings` for code duplication checks. The only option at the time of this writing is to exclude whole files from the duplication check using Filename patterns.

The following patterns are excluded:

    **/*SqlDialect.java
    **/*MetadataReader.java

Please note that these exclusions have no impact on other static code analysis checks.
