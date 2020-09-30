# Integration Testing

### Setup Data Source

* Setup and start the database
* Testdata: Create a test schema with a simple table (simple data types)

### Setup Exasol

* Setup and start an Exasol database with virtual schemas feature
* Upload the JDBC drivers of the data source via EXAOperation
* Manual test: query data from the data source via `IMPORT FROM JDBC`

### Catalog, Schema & Table Mapping

* Override the `SqlDialect` methods for catalog, schema and table mapping
* Manual test: create a virtual schema by specifying the catalog and/or schema.

### Data Type Mapping

* Testdata: Create a table with all data types and at least one row of data
* Override the `SqlDialect` method for data type mapping
* Automatic test: sys tables show virtual table and columns with correctly mapped type
* Automatic test: running `SELECT` on the virtual table returns the expected result

### Identifier Case Handling & Quoting

* Testdata: Create a schema/table/column with mixed case (if supported)
* Automatic test: sys tables correct
* Automatic test: `SELECT` works as expected

### Projection Capability

* Add capability
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`). Also test with mixed case columns.

### Predicates and Literal Capabilities

* Add capabilities for supported literals and predicates (e.g. `c1='foo'`)
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`) for all predicates & literals

### Aggregation & Set Function Capabilities

* Add capabilities for aggregations and aggregation functions
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`) for all set functions

### Order By / Limit Capabilities

* Testdata: Create a table with null values and non-null values, to check null collation.
* Add capabilities for order by and/or limit
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`)
* Automatic test: default null collation, explicit `NULLS FIRST/LAST`

### Scalar Function Capabilities

* Add capabilities for scalar functions
* Automatic test: pushed down & correct result (incl. `EXPLAIN VIRTUAL`)

### Views

* Testdata: Create a simple view, e.g. joining two existing tables
* Automatic test: Query the view, optionally e.g. with a filter.

## See Also

* [Integration testing with containers](integration_testing_with_containers.md)