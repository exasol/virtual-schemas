# Virtual Schemas FAQ

This FAQ covers general questions and problems that users can encounter in any dialect.
For a dialect specific FAQs please check dialects repositories.

## Setting up Virtual Schemas

## Creating Virtual Schemas

This chapter describes the problems that occurs on the creating VS step.

### The Dialect Does Not Support Property

**Problem**: a virtual schema does not recognise a property.

```
VM error:
com.exasol.adapter.dialects.PropertyValidationException: The dialect <DIALECT_NAME> does not support <PROPERTY_NAME> property. Please, do not set the BIGQUERY_ENABLE_IMPORT property.
```

**Solutions**:

- Check if the dialect supports the property you are trying to use. Each dialect has its own set of supported properties. Check the [dialect documentation][dialects].
- Check that you use a Virtual Schema version that supports the specified property. Check the release logs on the [GitHub][dialects] to find out in which version we added the property you need.
- Check that you don't have typos in the specified property.

### No Suitable Driver Found

**Problem**:

```
java.sql.SQLException: No suitable driver found for ...
```

**Solutions**:

- Check the JDBC driver you register in the EXAoperation and make you have registered the driver correctly.
- Check if you have typos in the main class definition or prefix.
- Now check the driver itself: open the JAR archive and make sure the file `META-INF/services/java.sql.Driver` exists.
- If the file exists, open it and make sure it contains the driver's main class reference you specified in the EXAoperation.
- If the file does not exist or does not contain the correct main class reference, you can add it and re-upload the fixed JAR archive. You should also report the problem to the developers of the driver.

### Create Virtual Schema Query Runs Endlessly

**Problem**: you have started a `CREATE VIRTUAL SCHEMA` statement, but it is running endlessly without giving you any output or an error.

**Solutions**:

- Check if you specified a property `SCHEMA_NAME`. If you do not add this property, the virtual schema will try to read metadata of all tables existing in a source database. It can take very long time.
- Check how many tables do exist in the schema you have specified. If there are more than a few hundreds of tables, creation of a virtual schema can also take time.

## Selecting From Virtual Schemas

[dialects]: dialects.md

[github-releases]: https://github.com/exasol/virtual-schemas/releases