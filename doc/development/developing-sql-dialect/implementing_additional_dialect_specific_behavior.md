# Implementing additional dialect-specific behavior

In this part we find out whether your new dialect has some **dialect-specific behavior**. If it does, you have to implement additional classes.

Here is a checklist with the most common behavior differences that might need implementation:

1. [Identifier Case Handling](#implementing-identifier-case-handling)   
2. [Supported tables types](#implementing-supported-tables-types)
3. [Finding Table Metadata](#finding-table-metadata)
4. [Data Type Conversion](#implementing-data-type-conversion)
5. [Query Rewriting](#implementing-query-rewriting)

## Implementing Identifier Case Handling

Different products have different case-sensitivity and quoting rules for identifiers like table names. 
In order to translate identifiers correctly between Exasol and the remote source, we must define the behavior of the remote data source.

_Exasol for example silently converts all unquoted identifiers to upper case. PostgreSQL converts them to lower case instead._ 
_MySQL table names are case-sensitive (at least on Unix-style operating systems) since they directly map to the names of the files containing the table data._

_In our Athena example the situation is tricky. The documentation states that Athena itself is uses case-insensitive table names._ 
_On the other hand combining Athena with Apache Spark forces case-sensitive table handling._ 
_For now we implement the default behavior and let Exasol handle all unquoted identifiers as if they were upper case._

1. First, check if the **default identifiers case handling** is suitable for your source:

    - **Unquoted** identifiers are treated as **UPPERCASE** text. 
    It means that if you write a query `SELECT * FROM Schema_Name.Table_Name`, the source reads it as `SELECT * FROM SCHEMA_NAME.TABLE_NAME`. 
    All schema/table/column names are converted to uppercase.
    - **Quoted** identifiers are treated as  **CASE SENSITIVE**.
    It means that if you write a query `SELECT * FROM "Schema_Name"."Table_Name"`, the source reads it as `SELECT * FROM Schema_Name.Table_Name`. 
    All schema/table/column names are used literally without case changes.

2. If the **both statements above are true** for your source, **skip** this part and go to [Supported tables types](implementing-supported-tables-types).
    
3. If you want to override the default identifiers case handling, create a new **class for the Metadata Reader**. 
    Follow steps in [Implementing Access to Remote Metadata](#implementing-access-to-remote-metadata) and then come back here. 
   
4. Check if one of the descriptions from the list below to the unquoted and / or quoted identifiers of the source:

    - All identifiers are converted to uppercase;
    - All identifiers are converted to lowercase;
    - All identifiers are treated as case-sensitive;
    
5. If any descriptions fit your identifiers, refer to the **[Standard Identifier Case Handling](#standard-identifier-case-handling)**. 
    If not, go to the **[Exotic Identifier Case Handling](#exotic-identifier-case-handling)**. 

    ### Standard Identifier Case Handling
    
   **Override the method `createIdentifierConverter()`** in `<Your dialect name>MetadataReader.java`. 

   Here is an example from the Apache Hive dialect. The first value of the `BaseIdentifierConverter`'s constructor is for unquoted identifiers, the second one is for quoted ones.    
   ```java
   @Override
   protected IdentifierConverter createIdentifierConverter() {
       return new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_LOWER,
              IdentifierCaseHandling.INTERPRET_AS_LOWER);
   }
   ```
   Don't forget to add a test to the `<Your dialect name>MetadataReaderTest.java`.

    ### Exotic Identifier Case Handling
    
    If you are unlucky, your data source has non-standard identifier case handling. You have to implement your own `IdentifierConverter` in this case.
    Create a new class which **implements the `IdentifierConverter`** interface and implement methods:
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
    After you finish the implementation, **override the method `createIdentifierConverter()`** in **`<Your dialect name>MetadataReader.java`**. 
    Instantiate `YourDialectIdentifierConverter` there:
    ```java
    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return new YourDialectIdentifierConverter(//constructor parameters);
    }
    ```    
    PostgreSQL is an example, where identifier handling needs special attention. Check the following classes:
    * `PostgresMetadataReader` &mdash; where the converter is instantiated
    * `PostgresIdentifierConverter` &mdash; which implements `IdentifierConverter` interface.
    * `PostgresIdentifierConverterTest`
 
## Implementing supported tables types
  
Each database supports a few table types. Our default implementation includes three supported types: "TABLE", "VIEW", "SYSTEM TABLE".
If **default supported types** are enough for your source, go to the next checkpoint: [Finding Table Metadata](#finding-table-metadata)

If the source supports something else, "EXTERNAL TABLE" for example, you need to change the default behavior:

1. Create a **class for the Metadata Reader** if you haven't created it yet. 
    Follow steps in [Implementing Access to Remote Metadata](#implementing-access-to-remote-metadata) and then come back here.

2. Start with the **unit test** in `<Your dialect name>MetadataReaderTest.java`:
   ```java
   @Test
   void testGetSupportedTableTypes() {
       assertThat(this.reader.getSupportedTableTypes(),
            containsInAnyOrder("TABLE", "VIEW", "SYSTEM TABLE", "EXTERNAL TABLE"));
   }
   ```
   
3. **Override `getSupportedTableTypes()`** method in `<Your dialect name>MetadataReader.java`.    
   ```java
   @Override
   public Set<String> getSupportedTableTypes() {
           return Collections
              .unmodifiableSet(new HashSet<>(Arrays.asList("TABLE", "VIEW", "SYSTEM TABLE", "EXTERNAL TABLE")));
   }
   ```
   Now the Virtual Schema will scan metadata for external tables too.      

## Finding Table Metadata

If you are accessing a remote data source for which a JDBC-compliant driver exists, you will in most cases be able to retrieve the information about the tables via built-in functions of the dirver. 
We use the function [`getTables(String, String, String, String[])`](https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getTables-java.lang.String-java.lang.String-java.lang.String-java.lang.String:A-) for that purpose in the `BaseTableMetadataReader` and most of the dialect variants.

_Athena (at least with driver version 2.0.7) is one of the rare examples, where that approach fails. Issuing `getTables(...)` returns an empty result set._ 
_Looking at the documentation, we can see that the recommended way to list tables is issuing a [`SHOW TABLES`](https://docs.aws.amazon.com/athena/latest/ug/show-tables.html) command._ 
_If you ever worked with [MySQL](https://www.mysql.com/) this might [look familiar](https://dev.mysql.com/doc/refman/8.0/en/show-tables.html)._

The best way to check if your source can find metadata with the default settings &mdash; run the first manual [integration test](integration_testing.md) with [remote logging](../remote_logging.md).
If you can **access metadata** using the default implementation, go to the next checkpoint: [Data Type Conversion](#implementing-data-type-conversion).
Otherwise we have to implement metadata handling:

1. Create a **class for the Metadata Reader** if you haven't created it yet. 
   Follow steps in [Implementing Access to Remote Metadata](#implementing-access-to-remote-metadata) and then come back here. 
   
2. Start with the **unit test** in `<Your dialect name>MetadataReaderTest.java`. You might need to override `getTableMetadataReader()` or `getColumnMetadataReader()` or both.
    ```java
    // ...
    public class YourDialectMetadataReaderTest {
        // ...
        
        @Test
        void testGetTableMetadataReader() {
            assertThat(this.reader.getTableMetadataReader(), instanceOf(YourDialectTableMetadataReader.class));
        }
    
        @Test
        void testGetColumnMetadataReader() {
            assertThat(this.reader.getColumnMetadataReader(), instanceOf(YourDialectColumnMetadataReader.class));
        }
    }
    ```

3. Create necessary classes and then implement the methods and new behavior.

## Implementing Data Type Conversion

First of all you need to find out which data types the source supports, what their sizes and possible values are. Then you need to think about how this maps to Exasol types.

If you are lucky, your source is compatible with the standard mapping found in `BaseColumnMetadataReader.java` (check method `mapJdbcType()` to get more information about default mapping).  
In that case you don't need to write any code since this is the column metadata reader that is used by default. 

If the database supports type that are not covered in the `BaseColumnMetadataReader.java` you will still often be able to map the handful of special data types and delegate the rest of the work back to the base reader.

_Be prepared that even if the remote data source implements an SQL standard type, there are often subtle differences in the implementation. The most obvious one are size restrictions that differ widely between the products._
_On for `VARCHAR` you also need to be aware of character set encoding. Exasol supports `UTF8` which should be able to receive data from any other character set and `ASCII` which is very limited but only uses a single byte per character._
_There are differences in how precise the remote data source can encode integer, fixed point and floating values and so on._

The best way to find out how good the default mapping works for your source &mdash; run a manual [integration test](integration_testing.md) with [remote logging](../remote_logging.md) accessing a table with all data types available in the source.
If you assume that you don't need to change data type conversion &mdash; go to the next checkpoint: [Implementing Query Rewriting](#implementing-query-rewriting)

Let's look at a HIVE dialect example. We only want to change mapping for one data type: DECIMAL.

1. **Create `<Your dialect name>ColumnMetadataReader.java`** class that extends `BaseColumnMetadataReader.java`.

2. **Override `mapJdbcType()`** method implementing type conversion. 
    Call **super** method at the end of an if-else/switch block to use the default implementation for types that you don't want to change. Write a test for this class.

    ```java
    package com.exasol.adapter.dialects.hive;
    
    //imports
    
    public class HiveColumnMetadataReader extends BaseColumnMetadataReader {
        public HiveColumnMetadataReader(final Connection connection, final AdapterProperties properties,
                final IdentifierConverter identifierConverter) {
            super(connection, properties, identifierConverter);
        }
    
        @Override
        public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
            if (jdbcTypeDescription.getJdbcType() == Types.DECIMAL) {
                return mapDecimal(jdbcTypeDescription);
            } else {
                return super.mapJdbcType(jdbcTypeDescription);
            }
        }
    
        protected DataType mapDecimal(final JdbcTypeDescription jdbcTypeDescription) {
            // here is our implementation
        }
    }
    ```

3. Create a **class for the Metadata Reader** if you haven't created it yet. 
   Follow steps in [Implementing Access to Remote Metadata](#implementing-access-to-remote-metadata) and then come back here. 

4. **Rewrite `createColumnMetadataReader()`** of `<Your dialect name>MetadataReader.java` instantiating your new Column Metadata Reader. Write a test. 

    ```java
        @Override
        protected ColumnMetadataReader createColumnMetadataReader() {
            return new HiveColumnMetadataReader(this.connection, this.properties, this.identifierConverter);
        }
    ```

## Implementing Query Rewriting

At its very core the Virtual Schema JDBC adapter rewrites queries. 
When a user issues a query at the Virtual Schema frontend, it is first parsed and interpreted by the database core, then pushed down in parts or as a whole to the Virtual Schema backend. 
In the case of the JDBC adapter we have a remote data source that speaks SQL &mdash; or a driver that makes it look as if they did. 
But before the Virtual Schema backend pushes a query further down to the remote data source, it rewrites it to accommodate dialect differences.

If possible the backend does not even execute that query itself, but aims to construct an [IMPORT](https://docs.exasol.com/sql/import.htm) statement and returns it to the Virtual Schema frontend. 
That way the Exasol database can directly run `IMPORT` on the remote data source. Effectively this removes the need for marshalling, transferring and unmarshalling data payload for the communication between Virtual Schema backend and frontend.

###  Pre-Requisites for Using IMPORT

If you plan to use `IMPORT`, bare in mind that remote data source must **offer a JDBC driver** (always the case if you write a dialect for the JDBC adapter instead of an adapter from scratch).

### Overloading Rewriting

The `AbstractSqlDialect` has a base implementation for query rewriting that should work with most databases that support JDBC. 
Sometimes you need more though and in that case you have different mechanisms for doing that.

#### Variant a) Implementing Your own QueryRewriter

Our recommended way is to **implement a specialized `QueryRewriter`** for your dialect. 
In most cases deriving that from the `BaseQueryRewriter` and adding some minimal changes does the trick.

**Check the `ExasolQueryRewriter` or `OracleQueryRewriter`** for examples on how to extend the base implementation. 
In those two examples we added support for the specialized `IMPORT FROM EXA` and `IMPORT FROM ORA` variants which are more efficient than a standard `IMPORT`.

Also take a look at the corresponding unit tests to learn how to verify the behavior of the rewriters without being forced to run slow and cumbersome integration tests against the remote data source during development.

This variant is less effort than implementing query rewriting from scratch.

Make sure to override the method `createQueryRewriter()` in the dialect to instantiate the right implementation for your use case. 

#### Variant b) Override `rewriteQuery(...)` in the Dialect

In those very rare case where the `QueryRewriter` does not work for you, override the method `rewriteQuery(...)` in your dialect.

## Implementing Access to Remote Metadata

Database metadata describes the structure and configuration parameters of a database. You need it for example to find out which tables exist and what columns they have.

The following three interfaces deal with database metadata:

| Interface              | Purpose                                |
|------------------------|----------------------------------------|
| `RemoteMetadataReader` | access to top level database metadata  |
| `TableMetadataReader`  | reads metadata on database table level |
| `ColumnMetadataReader` | reads column information and data type |

For each of them a base implementation exists which works fine with a number of JDBC-capable databases. On the other hand sometimes you need to deal with behavior and structure that deviates from the base implementation.

1. First of all create a class for the Metadata Reader. For example, `AthenaMetadataReader` in package `com.exasol.adapter.dialects.athena` that **extends** `AbstractRemoteMetadataReader`. 
    Let your IDE to generate necessary **overriding methods and constructor**. Also create a corresponding **test class**.
    ```java
    package com.exasol.adapter.dialects.athena;
    
    import java.sql.Connection;
    
    import com.exasol.adapter.AdapterProperties;
    import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
    
    public class AthenaMetadataReader extends AbstractRemoteMetadataReader {
        public AthenaMetadataReader(Connection connection, AdapterProperties properties) {
            super(connection, properties);
        }
        
      //methods here
    }
    ```
2. Now go back to **the main dialect test class** (`AthenaSqlDialectTest.java`) and write a **unit test** that ensures that the `AthenaSqlDialect` instantiates the specific metadata reader instead of the base implementation.
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

3. Now **change the method `createRemoteMetadataReader`** in the main dialect class instantiating your new Metadata Reader:
    ```java
        @Override
        protected RemoteMetadataReader createRemoteMetadataReader() {
            return new AthenaMetadataReader(this.connection, this.properties);
        }
    ```
4. **Implement methods** in the Metadata Reader. For start, you can use the next default implementations:
    ```java
       @Override
       protected IdentifierConverter createIdentifierConverter() {
           return new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_UPPER,
                   IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE);
       }
   
       @Override
       protected ColumnMetadataReader createColumnMetadataReader() {
           return new BaseColumnMetadataReader(this.connection, this.properties, this.identifierConverter);
       }
   
       @Override
       protected TableMetadataReader createTableMetadataReader() {
           return new BaseTableMetadataReader(this.connection, this.columnMetadataReader, this.properties,
                   this.identifierConverter);
       } 
   ```
