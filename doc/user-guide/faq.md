# Virtual Schemas FAQ

This FAQ covers general questions and problems that users can encounter in any dialect. For a dialect specific FAQs please check dialects pages.

## Setting up Virtual Schemas

### What happens if I upload two different JDBC drivers with the same name and the same prefix, but the driver's versions are different?

**Answer**: The database uses the first suitable driver detected in the driver's list going through the list top-down. The driver that is higher in the list of JDBC Drivers will be used in this situation.

## Creating Virtual Schemas

This chapter describes the problems that occur on the creating VS step.

### A virtual schema does not recognise a property and throws the dialect-does-not-support-property exception.

```
VM error:
com.exasol.adapter.dialects.PropertyValidationException: The dialect <DIALECT_NAME> does not support <PROPERTY_NAME> property. Please, do not set the BIGQUERY_ENABLE_IMPORT property.
```

**Solutions**:

- Check that you don't have typos in the specified property. Properties are case-sensitive.
- Check for spaces and other hidden characters.
- Check if the dialect supports the property you are trying to use. Each dialect has its own set of supported properties. Check the [dialect documentation][dialects].
- Check that you use a Virtual Schema version that supports the specified property. Check the release logs on the [GitHub][dialects] to find out in which version we added the property you need.

### SQLException: No suitable driver found...

**Solutions**:

- Check the JDBC driver you register in the EXAoperation and make you have registered the driver correctly.
- Check if you have typos in the main class definition or prefix.
- Check for spaces and other hidden characters.
- Now check the driver itself: open the JAR archive and make sure the file `META-INF/services/java.sql.Driver` exists.
- If the file exists, open it and make sure it contains the driver's main class reference you specified in the EXAoperation.
- If the file does not exist or does not contain the correct main class reference, you can add it and re-upload the fixed JAR archive. You should also report the problem to the developers of the driver.

### VM error: End of %scriptclass statement not found

**Solutions**:

- If you are using DbVisualizer, add an empty comment `--/` before the create adapter script statement:

```
--/
CREATE OR REPLACE JAVA ADAPTER SCRIPT ...
```

- If you are using DBeaver, mark the whole statement (except the trailing ;) and execute that statement in isolation (CTRL + RETURN).

### I have started a `CREATE VIRTUAL SCHEMA` statement, but it is running endlessly without giving any output or an error.

**Solutions**:

- Check if you specified a property `SCHEMA_NAME`. If you do not add this property, the virtual schema will try to read metadata of all tables existing in a source database. It can take very long time.
- Check how many tables do exist in the schema you have specified. If there are more than a few hundreds of tables, creation of a virtual schema can also take time.
- Check the [remote log][remote-log] to see whether the VS is stuck trying to establish a connection to the remote source. If so, try to reach that source directly from the same network to rule out network setup issues.

### Can I exclude tables from a virtual schema?

**Answer**: Yes, you can use TABLE_FILTER = 'TABLE1','TABLE2',... . Only these tables will be available as virtual tables, all other tables are excluded.

### I created a Virtual Schema on a view of an Exasol database, but the view does not exist in the Virtual Schema.

You created an Exasol-Exasol Virtual Schema on a schema that contains a view, but the Virtual Schema does not contain the view.

In that case the view was probably outdated, when you created the Virtual Schema.

This can happen if you create the view using `CREATE FORCE VIEW` or you updated a table that is part of the view after the view but did not refresh the view (Exasol refreshes views for example when they are queried).

**Answer:**

1. Refresh the view on the external Exasol database (for example using `DESCRIBE MY_VIEW`). 
1. Refresh the Virtual Schema (using `ALTER VIRTUAL SCHEMA MY_SCHEMA REFRESH`) 

## Selecting From Virtual Schemas

### The virtual schema was created successfully, but when you try to run a SELECT query, you get an `access denied` error with some permission name.

For example:

``` 
JDBC-Client-Error: Failed loading driver 'com.mysql.jdbc.Driver': null, access denied ("java.lang.RuntimePermission" "setContextClassLoader")
```

**Solution**:

- This happens because the JDBC driver requires more permissions than we provide by default. You can disable a security manager of the corresponding driver in [EXAoperation][exaoperation-drivers] to solve this problem.

## Domain Name (DNS) Resolution Issues

### Kerberos connection is unable to create remote metadata reader

When using Kerberos connection with Hive or Impala dialects, you may get the following exception:

```
com.exasol.adapter.jdbc.RemoteMetadataReaderException: Unable to create Impala remote metadata reader. Caused by: [Cloudera]ImpalaJDBCDriver Error creating login context using ticket cache: Unable to obtain Principal Name for authentication .
com.exasol.adapter.dialects.impala.ImpalaSqlDialect.createRemoteMetadataReader(ImpalaSqlDialect.java:127)
com.exasol.adapter.dialects.AbstractSqlDialect.readSchemaMetadata(AbstractSqlDialect.java:138)
com.exasol.adapter.jdbc.JdbcAdapter.readMetadata(JdbcAdapter.java:56)
```

**Solution**:

One of the reasons for this is issue might be the Kerberos service principal (hive or impala) domain name resolution. That is, even though the Kerberos realm is reachable from the Exasol node, the service domains may not be reachable due to the internal DNS settings.

You can confirm this by using `ping` or `dig` commands from one of the Exasol nodes.

To update the [DNS settings][exasol-network] for the Exasol cluster, go to ExaSolution &rarr; Configuration &rarr; Network. And update internal DNS server addresses and search domains entries.

## Other questions

### How can I check what's going on inside Virtual Schemas?

**Answer**:

- Start the [remote logging][remote-log] to see the logs from Virtual Schemas.
- Try to use `EXPLAIN VIRTUAL` command to see a query that Virtual Schemas generate. For example:

```sql
EXPLAIN VIRTUAL SELECT * FROM MY_VIRTUAL_SCHEMA.MY_TABLE;
```

### How many tables can contain a source schema?

**Answer**: We have restricted an amount of mapped tables to 1000. Please, use a TABLE_FILTER property to specify the tables you need if your schema contains more than 1000 tables.

### Does Virtual Schema process views and tables differently?

**Answer**: Views are the same as tables for Virtual Schemas.

### How can I fix low performance due to the log output?

**Solution**: If you use [remote logging][remove-logging], a number of factors can slow down the execution of a Virtual Schema.

Those are the things you can do to improve performance:

* Make sure there is a fast network connection between the cluster nodes running the virtual schema and the machine receiving the log;
* Lower the `DEBUG_LEVEL` to `INFO` or `WARNING`;
* Disable remote logging;

### Hoe can I fix low performance caused by slow randomness source?

**Solution**: Depending on which JDK version Exasol uses to execute Java user-defined functions, a blocking random-number source may be used by default.

Especially cryptographic operations do not complete until the operating system has collected a sufficient amount of entropy (read "real random values").

This problem mostly occurs when Exasol is run in an isolated environment, typically a virtual machine or a container.

#### Option a) Run a Process in Parallel That Generates Entropy

Operating systems use various sources of random data input, like keystroke timing, disk seeks and network timing. You can increase entropy by running processes in parallel that feed the entropy collection. Which ones those are depends on the OS.

#### Option b) Install Drivers That get Entropy From the Host's Hardware

Especially server machines often have dedicated hardware entropy sources. Still commodity hardware parts like sound adapters can be repurposed to create randomness, e.g. form random noise of an analog input.
In order to utilize those in virtual machines you usually need drivers and / or guest extensions that allow reading random data from the host.

#### Option c) Dangerous: Using a Pseudo-random Source

Since randomness is usually used for security measures like cryptography, using pseudo-random data is dangerous! Pseudo-random is another word for "guessable" and that is not what you want for cryptography.

If you intend to use this option, then do it **only for integration tests with non-confidential data**

* Log in to EXAOperation and shutdown the database.
* Append `-etlJdbcJavaEnv -Djava.security.egd=/dev/urandom` to the "Extra Database Parameters" input field and power the database on again.

[dialects]: dialects.md
[github-releases]: https://github.com/exasol/virtual-schemas/releases
[exaoperation-drivers]: https://docs.exasol.com/6.1/administration/on-premise/manage_software/manage_jdbc.htm
[remote-log]: https://docs.exasol.com/database_concepts/virtual_schema/logging.htm
[exasol-network]: https://docs.exasol.com/administration/on-premise/manage_network/configure_network_access.htm
[remove-logging]: https://github.com/exasol/virtual-schema-common-jdbc/blob/main/doc/development/remote_logging.md