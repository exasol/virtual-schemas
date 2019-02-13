# Oracle SQL Dialect

## Supported capabilities

The Oracle dialect does not support all capabilities. A complete list can be found in [OracleSqlDialect.getCapabilities()](../../virtualschema-jdbc-adapter/src/main/java/com/exasol/adapter/dialects/impl/OracleSqlDialect.java).

Oracle data types are mapped to their equivalents in Exasol. The following exceptions apply:

- `NUMBER`, `NUMBER with precision > 36` and `LONG` are casted to `VARCHAR` to prevent a loss of precision.
- `DATE` is casted to `TIMESTAMP`. This data type is only supported for positive year values, i.e., years > 0001.
- `TIMESTAMP WITH [LOCAL] TIME ZONE` is casted to `VARCHAR`. Exasol does not support timestamps with time zone information.
- `INTERVAL` is casted to `VARCHAR`.
- `CLOB`, `NCLOB` and `BLOB` are casted to `VARCHAR`.
- `RAW` and `LONG RAW` are not supported.

## JDBC Driver

To setup a virtual schema that communicates with an Oracle database using JDBC, the JDBC driver, e.g., `ojdbc7-12.1.0.2.jar`, must first be installed in EXAoperation and deployed to BucketFS; see [this article](https://www.exasol.com/support/browse/SOL-179#WhichJDBCdriverforOracleshallIuse?) and [Deploying the Adapter Step By Step](deploying_the_virtual_schema_adapter.md) for instructions.

## Adapter Script

After uploading the adapter jar we are ready to create an Oracle adapter script. Adapt the following script as indicated.

```sql
CREATE SCHEMA adapter;
CREATE JAVA ADAPTER SCRIPT adapter.jdbc_oracle AS
  %scriptclass com.exasol.adapter.jdbc.JdbcAdapter;

  // You need to replace `your-bucket-fs` and `your-bucket` to match the actual location
  // of the adapter jar.
  %jar /buckets/your-bucket-fs/your-bucket/virtualschema-jdbc-adapter-dist-1.4.0.jar;

  // Add the oracle jdbc driver to the classpath
  %jar /buckets/bucketfs1/bucket1/ojdbc7-12.1.0.2.jar
/
```

## JDBC Connection

Next, create a JDBC connection to your Oracle database. Adjust the properties to match your environment.

```sql
CREATE CONNECTION jdbc_oracle
  TO 'jdbc:oracle:thin:@//<host>:<port>/<service_name>'
  USER '<user>'
  IDENTIFIED BY '<password>';
```

A quick option to test the `JDBC_ORACLE` connection is to run an `IMPORT FROM JDBC` query. The connection works, if `42` is returned.

```sql
IMPORT FROM JDBC AT jdbc_oracle
  STATEMENT 'SELECT 42 FROM DUAL';
```

### Creating a Virtual schema

Having created both a JDBC adapter script and a JDBC oracle connection, we are ready to create a virtual schema. Insert the name of the schema that you want to expose in Exasol.

```sql
CREATE VIRTUAL SCHEMA virt_oracle USING adapter.jdbc_oracle WITH
  SQL_DIALECT     = 'ORACLE'
  CONNECTION_NAME = 'JDBC_ORACLE'
  SCHEMA_NAME     = '<schema>';
```

## Using IMPORT FROM ORA Instead of IMPORT FROM JDBC

Exasol provides the `IMPORT FROM ORA` command for loading data from Oracle. It is possible to create a virtual schema that uses `IMPORT FROM ORA` instead of JDBC to communicate with Oracle. Both options are indented to support the same features. `IMPORT FROM ORA` almost always offers better performance since it is implemented natively.

This behavior is toggled by the Boolean `IMPORT_FROM_ORA` variable. Note that a JDBC connection to Oracle is still required to fetch metadata. In addition, a "direct" connection to the Oracle database is needed.

### Deploying the Oracle Instant Client

To be able to communicate with Oracle, you first need to supply Exasol with the Oracle Instant Client, which can be obtained [directly from Oracle](http://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html). Open EXAoperation, visit Software -> "Upload Oracle Instant Client" and select the downloaded package. The latest version of Oracle Instant Client we tested is `instantclient-basic-linux.x64-12.1.0.2.0`.

### Creating an Oracle Connection

Having deployed the Oracle Instant Client, a connection to your Oracle database can be set up.

```sql
CREATE CONNECTION conn_oracle
  TO '(DESCRIPTION =
		(ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)
                                   (HOST = <host>)
                                   (PORT = <port>)))
		(CONNECT_DATA = (SERVER = DEDICATED)
                        (SERVICE_NAME = <service_name>)))'
	USER '<username>'
	IDENTIFIED BY '<password>';
```

This connection can be tested using, e.g., the following SQL expression.

```sql
IMPORT FROM ORA at CONN_ORACLE
  STATEMENT 'SELECT 42 FROM DUAL';
```

### Creating a Virtual schema

Assuming you already setup the JDBC connection `JDBC_ORACLE` as shown in the previous section, you can continue with creating the virtual schema.

```sql
CREATE VIRTUAL SCHEMA virt_import_oracle USING adapter.jdbc_oracle WITH
  SQL_DIALECT     = 'ORACLE'
  CONNECTION_NAME = 'JDBC_ORACLE'
  SCHEMA_NAME     = '<schema>'
  IMPORT_FROM_ORA = 'true'
  ORA_CONNECTION_NAME = 'CONN_ORACLE';
```