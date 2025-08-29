# Virtual Schemas FAQ

This FAQ covers general questions and problems that users can encounter in any dialect. For a dialect specific FAQs please check dialects pages.

## Uploading Files to BucketFS

### How can I check that the files ended up in the right bucket?

**Answer**: You can create a User Defined Function and query your bucket to check if the files are in the right place. 
Please check the [exa-toolbox repository](https://github.com/exasol/exa-toolbox/blob/master/utilities/README.md#bucketfs_ls) to find out how to query a bucket.

There are also other ways to access the bucket: `curl` command or `BucketFS Explorer`.
You can read more about them in the [official documentation](https://docs.exasol.com/database_concepts/bucketfs/access_control.htm).

## Establishing Connection to the Datasource

### How can I check that the connection to the source is established correctly?

**Answer**: We recommend checking the connection without Virtual Schemas and start creating Virtual Schemas after you are sure that the connection was established.

To check the connection, you need to create a connection object. You will also need this connection object to create a Virtual Schema.
Create a connection according to the dialect's user guide. A quick option to test the connection is to run an `IMPORT FROM JDBC` query with it.

For example:

```sql
IMPORT FROM JDBC AT <your_connection_name>
  STATEMENT 'SELECT 1 FROM <source schema>.<source table>';
```

If the statement was executed successfully, the connection is established correctly, and you can create a Virtual Schema using this connection.

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

**Answer**: Yes, you can use `TABLE_FILTER = 'TABLE1','TABLE2', …`. Only these tables will be available as virtual tables, all other tables are excluded.

### I created an Exasol-Exasol Virtual Schema on a view of an Exasol database, but the view does not exist in the Virtual Schema.

In that case the view was outdated when you created the Virtual Schema.

This can happen if you created the view using `CREATE FORCE VIEW`, or if you modified a table used by the view after the view creation, without refreshing the view (Exasol refreshes views only when they are queried).

As long as a view is not compiled in the remote schema, it remains invisible to the virtual schema accessing this remote schema. It needs to be compiled _first_ before the virtual schema can see it.

**Solution**:

1. Refresh the view on the source (for example, using `DESCRIBE MY_VIEW`). 
2. Refresh the Virtual Schema (using `ALTER VIRTUAL SCHEMA MY_SCHEMA REFRESH`) 

**Pro Tip**:

Views must be compiled to be accessible. Creating a view with the option `FORCE` explicitly skips the compilation step.  While this allows quickly creating a large number of views, even if you don't know their interdependencies, it comes at the cost of half-finished database objects.

Worse yet, you only discover whether a view definition is valid when you actually access the object. In other words, `FORCE` decouples making a mistake from realizing that it was made.

For this reason, it is much safer to compile views intentionally after they were bulk created. This way, any errors are caught immediately by the creator, not by the end user, allowing the creator to fix broken definitions before they cause problems. The creator of the view can repair broken definitions. The user cannot.

Exasol's support team can provide you with a script that mass-compiles views in parallel to speed up the process.

**See Also*::

Please refer to the section ["CREATE VIEW"](https://docs.exasol.com/db/latest/sql/create_view.htm) in the Exasol handbook for more details on creating and compiling views.

## Selecting From Virtual Schemas

### The virtual schema was created successfully, but when you try to run a SELECT query, you get an `access denied` error with some permission name.

For example:

``` 
JDBC-Client-Error: Failed loading driver 'com.mysql.jdbc.Driver': null, access denied ("java.lang.RuntimePermission" "setContextClassLoader")
```

**Solution**:

- This happens because the JDBC driver requires more permissions than we provide by default. You can disable a security manager of the corresponding driver in [EXAoperation][exaoperation-drivers] to solve this problem.

### I try to filter (e.g, with "LIKE") by a column in a document-based Virtual Schema but get an error message

Document-based virtual schemas like the S3 virtual schema support filtering only on the object reference (think of that like a path in a file system). The reason is that this reduces the actuall network traffic. If you pick fewer objects (files), less data needs to be transmitted.

Filtering on a column that is inside a document on the other hand means that Exasol needs to load the file from the source before it can apply the filter. So pushing down that filter is pointless.

Since disabling virtual schema capabilities is an all-or-nothing decision there is no way to selectively say "this adapter can filter the object reference only". As a consequence, we enable the filtering capability (`LIKE`) but check in the adapter if the filter is applied to the object reference only. If not, we need to raise an error.

**Solution:**

If you need to filter for a column that is inside a document in a document VS, then write an inner query that either has no filter or filters for the object reference and wrap that in an outer `SELECT` where you apply the remaining filters.

```sql
SELECT FROM (
   SELECT FROM my_virtual_schema ...
)
WHERE column1 LIKE `%foobar%`
```

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

## Kerberos Connection Issues

### Kerberos Configuration File Domain Realm

Ensure that the Kerberos, `krb5.conf`, file is correct.

Check that the domain realms are correct after the assignment (`=`) sign. They should be specified in all capital letters. They should not include starting dot (`.`) character after the assignment.

Wrong configuration file example:

```ini
...

[domain_realm]
.zone1.example.net = zone1.example.net
zone1.example.net = zone1.example.net
.example.net = .EXAMPLE.net
example.net = EXAMPLE.net
.exampl-dev.net = .EXAMPLE-DEV.NET
exampl-dev.net = EXAMPLE-DEV.NET
```

Correct configuration file example:

```ini
...

[domain_realm]
.zone1.example.net = ZONE1.EXAMPLE.NET
zone1.example.net = ZONE1.EXAMPLE.NET
.example.net = EXAMPLE.net
example.net = EXAMPLE.net
.example-dev.net = EXAMPLE-DEV.NET
example-dev.net = EXAMPLE-DEV.NET
```

### Kerberos Configuration File Included Directories

Ensure that Kerberos configuration file, `krb5.conf` does not contain any included directories with additional settings. All Kerberos settings should be available in the `krb5.conf` configuration file.

This can cause problems when using Virtual Schema together with Kerberos connection, because included directories do not exist in the UDF container.

Wrong configuration file example:

```ini
includedir /etc/krb5.conf.d/

[logging]
default = FILE:/var/log/krb5libs.log
kdc = FILE:/var/log/krb5kdc.log
admin_server = FILE:/var/log/kadmind.log

[libdefaults]
default_realm = ZONE1.EXAMPLE.NET
dns_lookup_kdc = false
dns_lookup_realm = false

...
```

The `includedir` folder contains a file with a setting, `udp_preference_limit = 1`. Add such settings into the `libdefaults` section in `krb5.conf` file.

Correct configuration file example:

```ini
[logging]
default = FILE:/var/log/krb5libs.log
kdc = FILE:/var/log/krb5kdc.log
admin_server = FILE:/var/log/kadmind.log

[libdefaults]
default_realm = ZONE1.EXAMPLE.NET
dns_lookup_kdc = false
dns_lookup_realm = false
udp_preference_limit = 1

...
```

And then create a connection object using the modified Kerberos configuration file.

### Kerberos with Zookeeper

In Virtual Schema Kerberos connections, users can also use Zookeeper as service discovery for Hive or Impala servers. Zookeeper balances the connections or avoids single point of failure for Hive or Impala servers.

In this cases, set the `KrbHostFQDN` property to `_HOST` value. This removes hardcoded server addresses and uses connection addresses provided by Zookeeper.

Example:

```
jdbc:hive2://zk=zookeeper001.dev.example.com:2181/hiveserver2,zk=zookeeper002.dev.example.com:2181/hiveserver2,zk=zookeeper003.dev.example.com:2181/hiveserver2;AuthMech=1;KrbHostFQDN=_HOST;KrbRealm=ZONE1.EXAMPLE.NET;KrbAuthType=1;KrbServiceName=hive;transportMode=http;httpPath=cliservice
```

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

### How can I fix low performance caused by slow randomness source?

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
[exaoperation-drivers]: https://docs.exasol.com/db/latest/administration/on-premise/manage_software/manage_jdbc.htm
[remote-log]: https://docs.exasol.com/database_concepts/virtual_schema/logging.htm
[exasol-network]: https://docs.exasol.com/administration/on-premise/manage_network/configure_network_access.htm
[remove-logging]: https://github.com/exasol/virtual-schema-common-jdbc/blob/main/doc/development/remote_logging.md

