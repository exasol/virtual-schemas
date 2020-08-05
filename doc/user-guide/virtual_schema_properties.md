# Virtual Schema's Properties

The following properties can be used to control the behavior of the JDBC adapter. 
As you see above, these properties can be defined in `CREATE VIRTUAL SCHEMA` or changed afterwards via `ALTER VIRTUAL SCHEMA SET`. 
Note that properties are always strings, like `TABLE_FILTER='T1,T2'`.

## Mandatory Properties

Property                    | Value
--------------------------- | -----------
**SQL_DIALECT**             | Name of the SQL dialect: EXASOL, HIVE, IMPALA, ORACLE, TERADATA, REDSHIFT or GENERIC (case insensitive). If you try generating a virtual schema without specifying this property you will see all available dialects in the error message.
**CONNECTION_NAME**         | Name of the connection created with `CREATE CONNECTION` which contains the JDBC connection string, the username and password.


## Common Optional Properties

Property                    | Value
--------------------------- | -----------
**CATALOG_NAME**            | The name of the remote JDBC catalog. This is usually case-sensitive, depending on the dialect. It depends on the dialect whether you have to specify this or not. Usually you have to specify it if the data source JDBC driver supports the concepts of catalogs.
**SCHEMA_NAME**             | The name of the remote JDBC schema. This is usually case-sensitive, depending on the dialect. It depends on the dialect whether you have to specify this or not. Usually you have to specify it if the data source JDBC driver supports the concepts of schemas.
**TABLE_FILTER**            | A comma-separated list of table names (case sensitive). Only these tables will be available as virtual tables, other tables are ignored. Use this if you don't want to have all remote tables in your virtual schema.

## Advanced Optional Properties

Property                    | Value
--------------------------- | -----------
**IMPORT_FROM_EXA**         | Only relevant if your data source is EXASOL. Either `TRUE` or `FALSE` (default). If true, `IMPORT FROM EXA` will be used for the pushdown instead of `IMPORT FROM JDBC`. You have to define `EXA_CONNECTION_STRING` if this property is true.
**EXA_CONNECTION_STRING**   | The connection string used for `IMPORT FROM EXA` in the format 'hostname:port'.
**IMPORT_FROM_ORA**         | Similar to `IMPORT_FROM_EXA` but for an Oracle data source. If enabled, the more performant `IMPORT FROM ORA` operation will be used in place of `IMPORT FROM JDBC`. You also need to define `ORA_CONNECTION_NAME` if this property is set to `TRUE`.
**ORA_CONNECTION_NAME**     | Name of the connection to an Oracle database created with `CREATE CONNECTION`. Used by `IMPORT FROM ORA`.
**IS_LOCAL**                | Only relevant if your data source is the same Exasol database where you create the virtual schema. Either `TRUE` or `FALSE` (default). If true, you are connecting to the local Exasol database (e.g. for testing purposes). In this case, the adapter can avoid the `IMPORT FROM JDBC` overhead.
**EXCEPTION_HANDLING**      | Activates or deactivates different exception handling modes. Supported values: `IGNORE_INVALID_VIEWS` and `NONE` (default). Currently this property only affects the Teradata dialect.
**EXCLUDED_CAPABILITIES**   | A comma-separated list of capabilities that you want to deactivate (although the adapter might support them).
**IGNORE_ERRORS**           | Is used to ignore errors thrown by the adapter. Please, check the documentation of the dialects that support this property for additional information: [Exasol dialect][exasol-dialect-doc], [PostgreSQL dialect][postgresql-dialect-doc];