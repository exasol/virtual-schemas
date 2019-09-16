# Step-by-Step Guide to Writing Your own SQL Dialect

Sooner or later you might think about connecting Exasol with an external source for which no adapter exists yet. 
If that source **offers a JDBC driver**, you don't need to implement a complete Virtual Schema adapter. 
Instead you can **add a new SQL dialect adapter**. For the sake of simplicity, we will call it SQL dialect. 

In the following section we will walk through the process of developing such a SQL dialect adapter by looking at how the adapter for Amazon's [AWS Athena](https://aws.amazon.com/athena) was created.

_Athena is based on the Open Source project [Apache Presto](https://prestodb.github.io) which in the own words of the Presto team is a "distributed SQL query engine for Big Data". 
In short it's a cluster of machines digging through large amounts of data stored on a distributed file system._

## Creating an SQL Dialect 

- [Part 1. Implementing the SQL Dialect Adapter's Mandatory Classes](implementing_mandatory_sql_dialect_classes.md) 
- [Part 2. Implementing Additional Dialect-Specific Behavior](implementing_additional_dialect_specific_behavior.md)
- [Part 3. Integration Testing](integration_testing.md)




