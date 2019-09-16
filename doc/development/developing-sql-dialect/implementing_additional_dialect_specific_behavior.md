# Implementing additional dialect-specific behavior

In this part we should check whether your new dialect has some **specific behavior**. If it does, you should implement additional classes.    

## Implementing Identifier Case Handling

Different products have different case-sensitivity and quoting rules for identifiers like table names. 
In order to translate identifiers correctly between Exasol and the remote source, we must define the behavior of the remote data source.

_Exasol for example silently converts all unquoted identifiers to upper case. PostgreSQL converts them to lower case instead._ 
_MySQL table names are case-sensitive (at least on Unix-style operating systems) since they directly map to the names of the files containing the table data._

_In our Athena example the situation is tricky. The documentation states that Athena itself is uses case-insensitive table names._ 
_On the other hand combining Athena with Apache Spark forces case-sensitive table handling._ 
_For now we implement the default behavior and let Exasol handle all unquoted identifiers as if they were upper case._

1. First, check if **the default identifiers case handling** is true for your source:

    - **Unquoted** identifiers are treated as **UPPERCASE** text. 
    It means that if you write `SELECT * FROM Schema_Name.Table_Name`, it will be read as `SELECT * FROM SCHEMA_NAME.TABLE_NAME`. 
    All schema/table/column names are converted to uppercase.
    - **Quoted** identifiers are treated as  **CASE SENSITIVE**.
    It means that if you write `SELECT * FROM "Schema_Name"."Table_Name"`, it will be read as `SELECT * FROM Schema_Name.Table_Name`. 
    All schema/table/column names are saved as they are written.

    If the both points are true for the source, you can **skip** `Implementing Identifier Case Handling` part and go to the next one. 

2. If you need to change the default behavior, **create a class for the Metadata Reader**. For example `com.exasol.adapter.dialects.hive.HiveMetadataReader` that **extends** `AbstractRemoteMetadataReader`. 
    Let your IDE to generate necessary **overriding methods and constructor** for you. 
    ```java
    public class HiveMetadataReader extends AbstractRemoteMetadataReader {
       //methods here
   } 
   ```
   Also **create a corresponding test class**.
   
3. Go back to the main dialect class (`HiveSqlDialect.java` for example) and **fix the `createRemoteMetadataReader()`** method instantiating your new Metadata Reader:

   ```java
   @Override
   protected RemoteMetadataReader createRemoteMetadataReader() {
       return new HiveMetadataReader(this.connection, this.properties);
   }
   ```
   Also fix the test.
   
4. Check that you can apply the description from the list below to the unquoted/quoted identifiers:

    - All identifiers are converted to uppercase;
    - All identifiers are converted to lowercase;
    - All identifiers are treated as case-sensitive;
    
    If you **can apply** one of the description above to the unquoted identifiers and also quoted identifiers (not necessary the same one to the both),
    go to the **[Standard Identifier Case Handling](#standard-identifier-case-handling)**. 
    
    **If not**, go to the **[Exotic Identifier Case Handling](#exotic-identifier-case-handling)**. 

    ### Standard Identifier Case Handling
    
   Add the **overriding method `createIdentifierConverter`** to **`<Your dialect name>MetadataReader`** which you created in step 2. 
   The first value from the `BaseIdentifierConverter`'s constructor is for unquoted identifiers. The second one is for quoted.
   
   Here is an example from the HIVE:
    
   ```java
   @Override
   protected IdentifierConverter createIdentifierConverter() {
       return new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_LOWER,
              IdentifierCaseHandling.INTERPRET_AS_LOWER);
   }
   ```
   Don't forget to add the test to the `<Your dialect name>MetadataReaderTest`.

    ### Exotic Identifier Case Handling
    
    If you are unlucky, your data source has non-standard identifier case handling. You have to implement your own `IdentifierConverter` in this case.
    Create a new class which implements IdentifierConverter and implement the methods:
    
    ```java
    public class YourDialectIdentifierConverter implements IdentifierConverter {   
        @Override
        public String convert(final String identifier) {
            //implement it
        }
    
        @Override
        public IdentifierCaseHandling getUnquotedIdentifierHandling() {
            //implement it
        }
    
        @Override
        public IdentifierCaseHandling getQuotedIdentifierHandling() {
            //implement it
        }
    }
    ```
    After you finished the implementation, add the **overriding method `createIdentifierConverter`** to **`<Your dialect name>MetadataReader`** which you created in step 2. 
    Instantiate `YourDialectIdentifierConverter` there:
    
    ```java
    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return new YourDialectIdentifierConverter(//constructor parameters);
    }
    ```    
   
    PostgreSQL is an example, where identifier handling needs special attention. Check the following classes for an example:
    * `PostgresMetadataReader` &mdash; where the converter is instantiated
    * `PostgresIdentifierConverter` &mdash; which implements `IdentifierConverter` interface.
    * `PostgresIdentifierConverterTest`
    
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

class AthenaMetadataReaderTest {
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

## Implementing Query Rewriting

At its very core the Virtual Schema JDBC adapter rewrites queries. When a user issues a query at the Virtual Schema frontend, it is first parsed and interpreted by the database core, then pushed down in parts or as a whole to the Virtual Schema backend. In the case of the JDBC adapter we have remote data source that speak SQL &mdash; or a driver that makes it like as if they did. But before the Virtual Schema backend pushes a query further down to the remote data source, it rewrites it to accommodate dialect differences.

If possible the backend does not even execute that query itself, but aims to construct an [IMPORT](https://docs.exasol.com/sql/import.htm) statement and gives that back to the Virtual Schema frontend. That way the Exasol database can directly run `IMPORT` on the remote data source, effectively remove the need for mashalling, transferring and unmarshalling data payload for the communication between Virtual Schema backend and frontend.

###  Pre-Requisites for Using IMPORT

If you plan to use IMPORT there are some things you need to bare in mind:

1. Remote data source must offer a JDBC driver
   (always the case if you write a dialect for the JDBC adapter instead of an adapter from scratch)
1. The driver must support switching Auto-Commit off

The ExaLoader needs control over committing during an import, so that it is for example able to roll back incomplete imports.

### Overloading Rewriting

The `AbstractSqlDialect` has a base implementation for query rewriting that should work with most databases that support JDBC. Sometimes you need more though and in that case you have different mechanisms for doing that.

#### Variant a) Implementing Your own QueryRewriter

Our recommended way is to implement a specialized `QueryRewriter` for your dialect. In most cases deriving that from the `BaseQueryRewriter` and adding some minimal changes does the trick.

Check the `ExasolQueryRewriter` or `OracleQueryRewriter` for examples on how to extend the base implementation. In those two examples we added support for the specialized `IMPORT FROM EXA` and `IMPORT FROM ORA` variants which are more efficient than a standard `IMPORT`.

Also take a look at the corresponding unit tests to learn how to verify the behavior of the rewriters without being forced to run slow and cumbersome integration tests against the remote data source during development.

This variant is less effort than implementing query rewriting from scratch.

Make sure to override the method `createQueryRewriter()` in the dialect to instantiate the right implementation for your use case. 

#### Variant b) Override `rewriteQuery(...)` in the Dialect

In those very rare case where the `QueryRewriter` does not work for you, override the method `rewriteQuery(...)` in your dialect.
