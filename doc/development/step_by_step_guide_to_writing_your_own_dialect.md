# Step-by-Step Guide to Writing Your own SQL Dialect

Sooner or later you might think about connecting Exasol with an external source for which no adapter exists yet. If that source offers a JDBC driver, you don't need to implement a complete Virtual Schema adapter. Instead you can add a new SQL dialect adapter based on what we call the "JDBC adapter".

In the following section we will walk through the process of developing such a SQL dialect adapter by looking at how the adapter for Amazon's [AWS Athena](https://aws.amazon.com/athena) was created.

Athena is based on the Open Source project [Apache Presto](https://prestodb.github.io) which in the own words of the Presto team is a "distributed SQL query engine for Big Data". In short it's a cluster of machines digging through large amounts of data stored on a distributed file system. As the motto suggests, the user interface is SQL. That means that Presto looks at the source data from a relational perspective. It is no wonder a JDBC driver exist for both Presto and Athena.

## Creating an SQL Dialect Adapter

Start by creating a new package called `com.exasol.adapter.dialects.athena` for the dialect in both `src/main/java` and `src/test/java`.

Now create a stub class for the dialect: `com.exasol.adapter.dialects.athena.AthenaSqlDialect` that extends `AbstractDialect`.

Add a static method to report the name of the dialect:

```java
private static final String NAME = "ATHENA";

public static String getPublicName() {
    return NAME;
}
```

Add a constructor that takes a [JDBC database connection](https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html) and user properties as parameters.

```java
/**
 * Create a new instance of the {@link AthenaSqlDialect}
 *
 * @param connection JDBC connection to the Athena service
 * @param properties user-defined adapter properties
 */
public AthenaSqlDialect(final Connection connection, final AdapterProperties properties) {
    super(connection, properties);
}
```

Add the fully qualified class name `com.exasol.adapter.dialects.athena.AthenaSqlDialect` to the file `src/main/resources/sql_dialects.properties`  so that the class loader can find your new dialect adapter.

Add the dialect name (here `ATHENA`) to the list of dialects for which the `JbdcAdapterFactory` is responsible in the method `getSupportedAdapterNames()`;

Create an empty unit test class for the dialect: `com.exasol.adapter.dialects.athena.AthenaSqlDialectTest` that tests class `AthenaSqlDialectTest`.

Now that you have the skeleton of the dialect adapter, it is time to implement the specifics.

## Acquiring Information About the Specifics of the Dialect

There are three ways to find out, what the specifics of the source that you want to attach to are. Which of those works depends largely on the availability and quality of the available information about that data source.

They are listed in ascending order of effort you need to spend.

1. Read the documentation
2. Read the source code
3. Reverse engineering

In most cases it is at least a combination of 1. and 3. If you are lucky enough to attach to an Open Source product, 2. is incredibly helpful.

In our Athena example, the [user guide](https://docs.aws.amazon.com/athena/latest/ug/what-is.html) is a good starting point for investigating capabilities, data mapping and special behavior.

## Implementing the SQL Dialect Adapter's Main Class

Each SQL Dialect adapter consists of one or more classes, depending on how standard-compliant the source behaves. The minimum implementation requires that you create the Main class of the adapter called `<source name>SqlDialect`.

### Defining the Supported Capabilities

First you need to find out, which capabilities the source supports and looking at the list of [SQL queries, functions and operators](https://docs.aws.amazon.com/athena/latest/ug/functions-operators-reference-section.html) contains what you need to assemble the capability list. All you have to to is read through the SQL reference and each time you come across a capability, mark that to the list.

The list of capabilities that Exasol's Virtual Schemas know can be found in `com.exasol.adapter.capabilities.MainCapabilities` from the project [`virtual-schema-common-java`](https://github.com/exasol/virtual-schema-common-java). If you look at the JavaDoc of that class, you find helpful examples of what that capability means.

Write a unit test that checks whether the SQL dialect adapter reports the capabilities that you find in the documentation of the data source. Here is an example from the Athena adapter.

```java
package com.exasol.adapter.dialects.athena;

import static com.exasol.adapter.capabilities.MainCapability.*;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import com.exasol.adapter.AdapterProperties;

public class AthenaSqlDialectTest {
    private AthenaSqlDialect dialect;

    @BeforeEach
    void beforeEach() {
        this.dialect = new AthenaSqlDialect(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetMainCapabilities() {
        assertThat(this.dialect.getCapabilities().getMainCapabilities(),
                containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                        AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                        AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT));
    }
}
```

Reading through the Athena and Presto documentation you will realize that while `LIMIT` in general is supported `LIMIT_WITH_OFFSET` is not. The unit test reflects that.

Run the test and it must fail, since you did not implement the the capability reporting method yet.

Now implement the method `getCapabilities()` in the dialect adapter so that it returns the main capabilities.


```java
import static com.exasol.adapter.capabilities.MainCapability.*;

// ...

public class AthenaSqlDialect extends AbstractSqlDialect {
    private static final Capabilities CAPABILITIES = createCapabilityList();

    private static Capabilities createCapabilityList() {
        return Capabilities //
                .builder() //
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT) //
                .build();
    }
    
    // ...

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }
    
    // ...
}
```

Since the capabilities of the adapter do not change at runtime, I assigned them to a constant. This way the `Capabilities` object is instantiated only once which makes querying the capabilities cheaper.

Now repeat that procedure for all other kinds of capabilities.

### Defining Catalog and Schema Support

Some databases know the concept of catalogs, others don't. Sometimes databases simulate a single catalog. The same is true for schemas. In case of a relational database you can try to find out whether or not catalogs and / or schemas are supported by simply looking at the Data Definition Language (DDL) statements that the SQL dialect provides. If `CREATE SCHEMA` exists, the database supports schemas.

If on the other hand those DDL commands are missing, that does not rule out that pseudo-catalogs and schemas are used. You will see why shortly.

A Virtual Schema needs to know how the data source handles catalogs and schemas, so that it can:

1. Validate user-defined catalog and schema properties
1. Apply catalog and schema filters only where those concepts are supported

A quick look at the [Athena DDL](https://docs.aws.amazon.com/athena/latest/ug/language-reference.html) tells us that you can't create or drop catalogs and schemas. On the other hand the [JDBC driver simulates catalog support with a single pseudo-catalog](https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.7/docs/Simba+Athena+JDBC+Driver+Install+and+Configuration+Guide.pdf#%5B%7B%22num%22%3A40%2C%22gen%22%3A0%7D%2C%7B%22name%22%3A%22XYZ%22%7D%2C76.5%2C556.31%2C0%5D) called `AwsDataCatalog`.

And the [documentation of the `SHOW DATABASES` command](https://docs.aws.amazon.com/athena/latest/ug/show-databases.html) states that there is a synonym called `SHOW SCHEMAS`. That means that Athena internally creates a 1:1 mapping of databases to schemas with the same name.

 So we implement two very simple unit tests.

```java
@Test
void testSupportsJdbcCatalogs() {
    assertThat(this.dialect.supportsJdbcCatalogs(), equalTo(StructureElementSupport.SINGLE));
}

@Test
void testSupportsJdbcSchemas() {
    assertThat(this.dialect.supportsJdbcSchemas(), equalTo(StructureElementSupport.MULTIPLE));
}
```

Both tests must fail. After that implement the functions `supportsJdbcCatalogs()` and `supportsJdbcSchemas()`. Re-run the test.

The methods `requiresCatalogQualifiedTableNames(SqlGenerationContext)` and `requiresSchemaQualifiedTableNames(SqlGenerationContext)` are closely related. They define under which circumstances table names need to be qualified with catalog and / or schema name.

Below you find two unit test where the first checks that the Athena adapter does not require catalog-qualified IDs when generating SQL code and the second states that schema-qualification is required.

```java
@Test
void testRequiresCatalogQualifiedTableNames() {
    assertThat(this.dialect.requiresCatalogQualifiedTableNames(null), equalTo(false));
}

@Test
void testRequiresSchemaQualifiedTableNames() {
    assertThat(this.dialect.requiresSchemaQualifiedTableNames(null), equalTo(true));
}
```

### Defining Identifier Quoting

Different products have different case-sensitivity and quoting rules for identifiers like table names. Exasol for example silently converts all unquoted identifiers to upper case. PostgreSQL converts them to lower case instead. MySQL table names are case-sensitive since they directly map to the names of the files containing the table data. In order to translate identifiers correctly between Exasol and the remote source, we must define the behavior of the remote data source.

In our Athena example the situation is tricky. The documentation states that Athena itself is uses case-insensitive table names. On the other hand combining Athena with Apache Spark forces case-sensitive table handling. For now we implement the default behavior and let Exasol handle all unquoted identifiers as if they were upper case.

As always, start with the unit test:

```java
@Test
void testGetUnquotedIdentifierHandling() {
    assertThat(this.dialect.getUnquotedIdentifierHandling(), equalTo(IdentifierCaseHandling.INTERPRET_AS_UPPER));
}
```

Let the test fail, implement `getUnquotedIdentifierHandling()` and repeat the test.

Exasol treats quoted identifiers with the exact case. Athena doesn't. A simple SQL command on a table where all identifiers are lower case proves that.

```sql
SELECT "Price" FROM "SALES" LIMIT 10;
```

The above command would fail on Exasol, but runs just fine on Athena. So we can treat all identifiers as upper case again.

Since Athena requires special care when working with identifiers that contain numbers or start with an underscore, this give us the opportunity to write a unit test that verifies a little bit more complex quoting rules.

```java
@CsvSource({ "tableName, \"tableName\"", "table123, \"table123\"", "_table, `_table`",
        "table_name, \"table_name\"" })
@ParameterizedTest
void testApplyQuote(final String unquoted, final String quoted) {
    assertThat(this.dialect.applyQuote(unquoted), equalTo(quoted));
}

@CsvSource({ "tableName, tableName", "table123, \"table123\"", "_table, `_table`", "table_name, table_name" })
@ParameterizedTest
void testApplyQuoteIfNeeded(final String unquoted, final String quoted) {
    assertThat(this.dialect.applyQuoteIfNeeded(unquoted), equalTo(quoted));
}
```

Again the tests must fail before we add the following implementation.

```java
private static final Pattern IDENTIFIER_WITH_NUMBERS = Pattern.compile(".*\\d.*");

@Override
public String applyQuote(final String identifier) {
    if (identifier.startsWith("_")) {
        return quoteWithBackticks(identifier);
    } else {
        return quoteWithDoubleQuotes(identifier);
    }
}

private String quoteWithBackticks(final String identifier) {
    final StringBuilder builder = new StringBuilder("`");
    builder.append(identifier);
    builder.append("`");
    return builder.toString();
}

private String quoteWithDoubleQuotes(final String identifier) {
    final StringBuilder builder = new StringBuilder("\"");
    builder.append(identifier);
    builder.append("\"");
    return builder.toString();
}

@Override
public String applyQuoteIfNeeded(final String identifier) {
    if (identifier.startsWith("_")) {
        return quoteWithBackticks(identifier);
    } else {
        if (containsNumbers(identifier)) {
            return quoteWithDoubleQuotes(identifier);
        } else {
            return identifier;
        }
    }
}

private boolean containsNumbers(final String identifier) {
    return IDENTIFIER_WITH_NUMBERS.matcher(identifier).matches();
}
```

The pre-compiled pattern is a performance measure. You could have used `identifier.matches(".*\\d.*")` instead, which is a little bit slower. But since quoting happens a lot, I felt that it did not want to waste performance here.

### Defining how NULL Values are Sorted

Next we tell the virtual schema how the SQL dialect sorts `NULL` values by default. The Athena documentation states that by default `NULL` values appear last in a search result regardless of search direction.

So the unit test looks like this:

```java
@Test
void testGetDefaultNullSorting() {
    assertThat(this.dialect.getDefaultNullSorting(), equalTo(NullSorting.NULLS_SORTED_AT_END));
}
```

Again run the test, let it fail, implement, let the test succeed.

#### Implement String Literal Conversion

The last thing we need to implement in the dialect class is quoting of string literals. Athena uses an approach typical for many SQL-capable databases. It expects string literals to be wrapped in single quotes and single qoutes inside the literal to be escaped by duplicating each.

```java
@ValueSource(strings = { "ab:\'ab\'", "a'b:'a''b'", "a''b:'a''''b'", "'ab':'''ab'''" })
@ParameterizedTest
void testGetLiteralString(final String definition) {
    final int colonPosition = definition.indexOf(':');
    final String original = definition.substring(0, colonPosition);
    final String literal = definition.substring(colonPosition + 1);
    assertThat(this.dialect.getStringLiteral(original), equalTo(literal));
}
```

You might be wondering why I did not use the `CsvSource` parameterization here. This is owed to the fact that the `CsvSource` syntax interprets single quotes as string quotes which makes this particular scenario untestable.

After we let the test fail, we add the following implementation in the dialect:

```java
@Override
public String getStringLiteral(final String value) {
    final StringBuilder builder = new StringBuilder("'");
    builder.append(value.replaceAll("'", "''"));
    builder.append("'");
    return builder.toString();
}
```

### Checking the Code Coverage of the Dialect Adapter

Before you move on to mapping metadata, first check how well your unit tests cover the dialect adapter. Keep adding test until you reach full coverage.

## Implementing Access to Remote Metadata

Database metadata describes the structure and configuration parameters of a database. You need it for example to find out which tables exist and what columns they have.

The following three interfaces deal with database metadata:

| Interface              | Purpose                                |
|------------------------|----------------------------------------|
| `RemoteMetadataReader` | access to top level database metadata  |
| `TableMetadataReader`  | reads metadata on database table level |
| `ColumnMetadataReader` | reads column information and data type |

For each of them a base implementation exists which works fine with a number of JDBC-capable databases. On the other hand sometimes you need to deal with behavior and structure that deviates from the base implementation.

Let's take Athena again for a concrete example. While the `BaseRemoteMetadataReader` scans the database for objects of type `TABLE` and `VIEW`, in the case of Athena this is not enough. Athena knows what it calls an `EXTERNAL TABLE`, so we are going to create our own implementation that adds this object type in the metadata scan.

First of all create an empty class shell for a `AthenaMetadataReader` in package `com.exasol.adapter.dialects.athena` that you derive from the `BaseRemoteMetadataReader`.

```java
package com.exasol.adapter.dialects.athena;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;

public class AthenaMetadataReader extends BaseRemoteMetadataReader {
    public AthenaMetadataReader(Connection connection, AdapterProperties properties) {
        super(connection, properties);
    }
}
```

Now write a unit test that ensures that the `AthenaSqlDialect` instantiates the specific metadata reader instead of the base implementation.

```java
package com.exasol.adapter.dialects.athena;

//...
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
//...

class AthenaSqlDialectTest {
    private AthenaSqlDialect dialect;
    
    // ...

    @Test
    void testMetadataReaderClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(AthenaMetadataReader.class));
    }
}
```

When you run this test, it must fail and tell you that instead of the expected `AthenaMetadataReader` a `BaseRemoteMetadataReader` is instantiated. This is the default behavior of any SQL dialect.

Now change that by overriding the method `createRemoteMetadataReader` in your dialect.

```java
    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new AthenaMetadataReader(this.connection, this.properties);
    }
```

At this point you dialect uses a specific metadata reader -- but it behaves like the default one. Now let's make the adaption to our data source that we planned. As always we start with the unit test.

```java
package com.exasol.adapter.dialects.athena;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class AthenaMetadataReaderTest {
    AthenaMetadataReader reader;

    @BeforeEach
    void beforeEach() {
        this.reader = new AthenaMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetSupportedTableTypes() {
        assertThat(this.reader.getSupportedTableTypes(),
                containsInAnyOrder("TABLE", "VIEW", "SYSTEM TABLE", "EXTERNAL TABLE"));
    }
}
```

Fails as expected, telling us that `EXTERNAL TABLE` is missing in the list of supported table types. So we change that.

```java
public class AthenaMetadataReader extends BaseRemoteMetadataReader {
    // ...

    @Override
    public Set<String> getSupportedTableTypes() {
        return Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList("TABLE", "VIEW", "SYSTEM TABLE", "EXTERNAL TABLE")));
    }
}
```

Now that the test is green, the Virtual Schema will scan metadata for external tables too.

## Finding Table Metadata

If you are accessing a remote data source for which a JDBC-compliant driver exists, you will in most cases be able to retrieve the information about the tables via built-in functions of the dirver. We use the function [`getTables(String, String, String, String[])`](https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getTables-java.lang.String-java.lang.String-java.lang.String-java.lang.String:A-`) for that purpose in the `BaseTableMetadataReader` and most of the dialect variants.

Athena (at least with driver version 2.0.7) is one of the rare examples, where that approach fails. Issuing `getTables(...)` returns an empty result set. Looking at the documentation, we can see that the recommended way to list tables is issuing a [`SHOW TABLES`](https://docs.aws.amazon.com/athena/latest/ug/show-tables.html) command. If you ever worked with [MySQL](https://www.mysql.com/) this might [look familiar](https://dev.mysql.com/doc/refman/8.0/en/show-tables.html).

Since we need to handle table and column metadata in a dialect-specific way, we make sure that the `AthenaMetadataReader` uses the right mappers.

```java
// ...
public class AthenaMetadataReaderTest {
    // ...
    
    @Test
    void testGetTableMetadataReader() {
        assertThat(this.reader.getTableMetadataReader(), instanceOf(AthenaTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.reader.getColumnMetadataReader(), instanceOf(AthenaColumnMetadataReader.class));
    }
}
```

Create stubs for the two tests, let the test fail and then implement the metods that create the right instances.

## Implementing Data Type Conversion

First of all you need to find out which data types the source supports, what there sizes and possible values are. Then you need to think about how this maps to Exasol types.

For this you need to write a class implementing the interface `ColumnMetadataReader`.

If you are lucky, your source is compatible with the standard mapping found in  `BaseColumnMetadataReader`. In that case you don't need to write any code since this is the column metadata reader that is used by default. This default implementation covers a wide range of typical data types from `TINYINT` over `TIMESTAMP` to `LONGVARCHAR`.

If the database supports type that are not covered in the `BaseColumnMetadataReader` you will still often be able to map the handful of special data types and delegate the rest of the work back to the base reader.

Let's look at our Athena example. The [Atheny documentation data type list](https://docs.aws.amazon.com/athena/latest/ug/data-types.html) contains the following types that are all supported by the `BaseColumnMetadataReader`: `TINYINT`, `SMALLINT`, `INT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `CHAR`, `VARCHAR`, `BINARY` `DATE` and `TIMESTAMP`.

It also support types that have an internal structure, namely [`ARRAY`](https://prestodb.github.io/docs/current/functions/array.html), [`MAP`](https://prestodb.github.io/docs/current/functions/map.html), `STRUCT`. Exasol doesn't have an equivalent for those types. If the users of your SQL dialect adapter are interested in a type like this you can find a textual or binary representation that fits -- e.g. [JSON](https://tools.ietf.org/html/rfc7159), [XML](https://www.w3.org/TR/REC-xml/) or [Protocol Buffers](https://developers.google.com/protocol-buffers/). You can then pack the results into a `VARCHAR` column. The downside of this is that since the database does not really understand the contents, searching them with built-in capabilities of the database is quite limited.

### Handling Subtle Type Differences

Be prepared that even if the remote data source implements an SQL standard type, there are often subtle differences in the implementation. The most obvious one are size restrictions that differ widely between the products. On for `VARCHAR` you also need to be aware of character set encoding. Exasol supports `UTF8` which should be able to receive data from any other character set and `ASCII` which is very limited but only uses a single byte per character.

There are differences in how precise the remote data source can encode integer, fixed point and floating values and so on.

A `ColumnMetadataReader` must be aware of those peculiarities and address them when mapping the content of a dataset field.

### Telling the Remote Metadata Reader Which Table / Column Metadata Reader to use

As mentioned before, if the `BaseTableMetadataReader` and `BaseColumnMetadataReader` work for your source, then there is no extra work involved for you.

If not, you must implement your own variants.

Take a look at the `PostgreSQLColumnMetadataReader` and its unit test for an example of how to instantiate a different sub-reader from the top-level reader.