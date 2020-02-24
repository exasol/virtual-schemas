# Step-by-Step Guide to Writing Your own SQL Dialect

Sooner or later you might think about connecting Exasol with an external source for which no adapter exists yet. 
If that source **offers a JDBC driver**, you don't need to implement a complete Virtual Schema adapter. 
Instead you can **add a new SQL dialect adapter**. For the sake of simplicity, we will call it SQL dialect. 

In the following section we will walk through the process of developing such a SQL dialect adapter by looking at how the adapter for Amazon's [AWS Athena](https://aws.amazon.com/athena) was created.

_Athena is based on the Open Source project [Apache Presto](https://prestodb.github.io) which in the own words of the Presto team is a "distributed SQL query engine for Big Data". 
In short it's a cluster of machines digging through large amounts of data stored on a distributed file system._

## Prerequisites

To start developing Virtual Schemas you should know:

* How to use [git](https://git-scm.com/)
* How to use [Maven](https://maven.apache.org/). Check the list of dependencies and plugins that we use in the project at the end of [README](../../../README.md) file.
* How to setup a Maven project in the IDE of choice. 
  - [Tips for Eclipse](https://www.eclipse.org/m2e/)
  - [Tips for Intellij IDEA](https://www.jetbrains.com/help/idea/maven-support.html)
* How to use [Docker](https://www.docker.com/) (for integration testing)
* How to [create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm) 
* How to [upload the driver to BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/accessfiles.htm) 
* How to [configure Exasol System Network Settings](https://docs.exasol.com/administration/on-premise/manage_network.htm)

## Creating an SQL Dialect 

- [Part 1. Implementing the SQL Dialect Adapter's Mandatory Classes](implementing_mandatory_sql_dialect_classes.md) 
- [Part 2. Implementing Additional Dialect-Specific Behavior](implementing_additional_dialect_specific_behavior.md)
- [Part 3. Integration Testing](integration_testing.md)
