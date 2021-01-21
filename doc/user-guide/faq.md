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

### Can I exclude multiple tables from a virtual schema?

**Answer**: Yes, you can use TABLE_FILTER = 'TABLE1','TABLE2',...

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

[dialects]: dialects.md
[github-releases]: https://github.com/exasol/virtual-schemas/releases
[exaoperation-drivers]: https://docs.exasol.com/6.1/administration/on-premise/manage_software/manage_jdbc.htm
[remote-log]: https://docs.exasol.com/database_concepts/virtual_schema/logging.htm
[exasol-network]: https://docs.exasol.com/administration/on-premise/manage_network/configure_network_access.htm
