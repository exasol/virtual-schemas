# Exasol Virtual Schemas 4.0.3, released 2020-08-??

## Documentation

* #283: Testes SQL Server with new JDBC driver. Added integration test, updated documentation.
* #317: Worked on reducing redundancy between user_guid.md and docs.exasol.com
* #321: Fixed SQL Server bug. Added a function COUNT_BIG instead of COUNT.
* #354: Added datatypes mapping info to the postgres documentation. 
* #355: Updated general deployment guide. 
* #359: Replaced links to products with links to dialect in the README's feature list.
* #364: Fixed Oracle dialect bug with invalid Decimal precision (DECIMAL(0,0)).
* #369: Added supported data types list to Oracle dialect.
* #370: Cleaned up Oracle dialect.

## Dependency updates

<details>
  <summary>Click to expand</summary>
  
* Added `org.junit.jupiter:junit-jupiter:5.6.2`  
* Updated `com.exasol:virtual-schema-common-jdbc` from 5.0.2 to 5.0.4
* Updated `com.exasol:exasol-testcontainers` from 2.0.3 to 2.1.0
* Updated `mysql:mysql-connector-java` from 8.0.20 to 8.0.21
* Updated `org.apache.hbase:hbase-server` from 2.2.5 to 2.3.0
* Updated `org.mockito:mockito-junit-jupiter` from 3.3.3 to 3.4.6
* Removed `org.junit.jupiter:junit-jupiter-engine`
* Removed `org.junit.platform:junit-platform-runner`
* Removed `org.mockito.mockito-core`
* Excluded `com.fasterxml.jackson.core:jackson-databind` from `org.apache.hbase:hbase-server` to remove vulnerabilities: 
https://ossindex.sonatype.org/component/pkg:maven/com.fasterxml.jackson.core/jackson-databind@2.7.8

</details>
