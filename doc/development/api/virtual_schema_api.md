# Virtual Schema API Documentation

## Table of Contents
- [Introduction](#introduction)
- [Requests and Responses](#requests-and-responses)
  - [Create Virtual Schema](#create-virtual-schema)
  - [Refresh](#refresh)
  - [Set Properties](#set-properties)
  - [Drop Virtual Schema](#drop-virtual-schema)
  - [Get Capabilities](#get-capabilities)
  - [Pushdown](#pushdown)
- [Embedded Commonly Used Json Elements](#embedded-commonly-used-json-elements)
  - [Schema Metadata Info](#schema-metadata-info)
  - [Schema Metadata](#schema-metadata)
- [Expressions](#expressions)
  - [Table](#table)
  - [Join](#join)
  - [Column Lookup](#column-lookup)
  - [Literal](#literal)
  - [Predicates](#predicates)
  - [Scalar Functions](#scalar-functions)
  - [Aggregate Functions](#aggregate-functions)

## Introduction

There are the following request and response types:

| Type                        | Called ...     |
| :-------------------------- | :------------- |
| **Create Virtual Schema**   | ... for each `CREATE VIRTUAL SCHEMA ...` statement |
| **Refresh**                 | ... for each `ALTER VIRTUAL SCHEMA ... REFRESH ...` statement. |
| **Set Properties**          | ... for each `ALTER VIRTUAL SCHEMA ... SET ...` statement. |
| **Drop Virtual Schema**     | ... for each `DROP VIRTUAL SCHEMA ...` statement. |
| **Get Capabilities**        | ... whenever a virtual table is queried in a `SELECT` statement. |
| **Pushdown**                | ... whenever a virtual table is queried in a `SELECT` statement. |

We describe each of the types in the following sections.

**Please note:** To keep the documentation concise we defined the elements which are commonly in separate sections below, e.g. `schemaMetadataInfo` and `schemaMetadata`.

## Requests and Responses

### Create Virtual Schema

Informs the Adapter about the request to create a Virtual Schema, and asks the Adapter for the metadata (tables and columns).

The Adapter is allowed to throw an Exception if the user missed to provide mandatory properties or in case of any other problems (e.g. connectivity).

**Request:**

```json
{
    "type": "createVirtualSchema",
    "schemaMetadataInfo": {
        ...
    }
}
```

**Response:**

```json
{
    "type": "createVirtualSchema",
    "schemaMetadata": {
        ...
    }
}
```

Notes
* `schemaMetadata` is mandatory. However, it is allowed to contain no tables.


### Refresh

Request to refresh the metadata for the whole Virtual Schema, or for specified tables.

**Request:**

```json
{
    "type": "refresh",
    "schemaMetadataInfo": {
        ...
    },
    "requestedTables": ["T1", "T2"]
}
```

Notes
* `requestedTables` is optional. If existing, only the specified tables shall be refreshed. The specified tables do not have to exist, it just tell Adapter to update these tables (which might be changed, deleted, added, or non-existing).

**Response:**

```json
{
    "type": "refresh",
    "schemaMetadata": {
        ...
    },
    "requestedTables": ["T1", "T2"]
}
```

Notes
* `schemaMetadata` is optional. It can be skipped if the adapter does not want to refresh (e.g. because he detected that there is no change).
* `requestedTables` must exist if and only if the element existed in the request. The values must be the same as in the request (to make sure that Adapter only refreshed these tables).

### Set Properties

Request to set properties. The Adapter can decide whether he needs to send back new metadata. The Adapter is allowed to throw an Exception if the user provided invalid properties or in case of any other problems (e.g. connectivity).

**Request:**

```json
{
    "type": "setProperties",
    "schemaMetadataInfo": {
        ...
    },
    "properties": {
        "JDBC_CONNECTION_STRING": "new-jdbc-connection-string",
        "NEW_PROPERTY": "value of a not yet existing property",
        "DELETED_PROPERTY": null
    }
}
```

**Response:**

```json
{
    "type": "setProperties",
    "schemaMetadata": {
        ...
    }
}
```

Notes
* Request: A property set to null means that this property was asked to be deleted. Properties set to null might also not have existed before.
* Response: `schemaMetadata` is optional. It only exists if the adapter wants to send back new metadata. The existing metadata are overwritten completely.


### Drop Virtual Schema

Inform the Adapter that a Virtual Schema is about to be dropped. The Adapter can update external dependencies if he has such. The Adapter is not expected to throw an exception, and if he does, it will be ignored.

**Request:**

```json
{
    "type": "dropVirtualSchema",
    "schemaMetadataInfo": {
        ...
    }
}
```

**Response:**

```json
{
    "type": "dropVirtualSchema"
}
```


### Get Capabilities

Request the list of capabilities supported by the Adapter. Based on these capabilities, the database will collect everything that can be pushed down in the current query and sends a pushdown request afterwards.

**Request:**

```json
{
    "type": "getCapabilities",
    "schemaMetadataInfo": {
        ...
    }
}
```

**Response:**

```json
{
    "type": "getCapabilities",
    "capabilities": [
        "ORDER_BY_COLUMN",
        "AGGREGATE_SINGLE_GROUP",
        "LIMIT",
        "AGGREGATE_GROUP_BY_TUPLE",
        "FILTER_EXPRESSIONS",
        "SELECTLIST_EXPRESSIONS",
        "SELECTLIST_PROJECTION",
        "AGGREGATE_HAVING",
        "ORDER_BY_EXPRESSION",
        "AGGREGATE_GROUP_BY_EXPRESSION",
        "LIMIT_WITH_OFFSET",
        "AGGREGATE_GROUP_BY_COLUMN",
        "FN_PRED_LESSEQUALS",
        "FN_AGG_COUNT",
        "LITERAL_EXACTNUMERIC",
        "LITERAL_DATE",
        "LITERAL_INTERVAL",
        "LITERAL_TIMESTAMP_UTC",
        "LITERAL_TIMESTAMP",
        "LITERAL_NULL",
        "LITERAL_STRING",
        "LITERAL_DOUBLE",
        "LITERAL_BOOL"
    ]
}
```

The set of capabilities in the example above would be sufficient to pushdown all aspects of the following query:
```sql
SELECT user_id, COUNT(url)
FROM   vs.clicks
WHERE  user_id>1
GROUP  BY user_id
HAVING count(url)>1
ORDER  BY user_id
LIMIT  10;
```

##### Capability Prefixes

- Main Capabilities: No prefix
- Literal Capabilities: LITERAL_
- Predicate Capabilities: FN_PRED_
- Scalar Function Capabilities: FN_
- Aggregate Function Capabilities: FN_AGG_

See also [a list of supported Capabilities](capabilities_list.md).

Capabilities can be also found in the sources of the Virtual Schema Common Java:
* [Main Capabilities](https://github.com/exasol/virtual-schema-common-java/blob/master/src/main/java/com/exasol/adapter/capabilities/MainCapability.java)
* [Literal Capabilities](https://github.com/exasol/virtual-schema-common-java/blob/master/src/main/java/com/exasol/adapter/capabilities/LiteralCapability.java)
* [Predicate Capabilities](https://github.com/exasol/virtual-schema-common-java/blob/master/src/main/java/com/exasol/adapter/capabilities/PredicateCapability.java)
* [Scalar Function Capabilities](https://github.com/exasol/virtual-schema-common-java/blob/master/src/main/java/com/exasol/adapter/capabilities/ScalarFunctionCapability.java)
* [Aggregate Function Capabilities](https://github.com/exasol/virtual-schema-common-java/blob/master/src/main/java/com/exasol/adapter/capabilities/AggregateFunctionCapability.java)

### Pushdown

Contains an abstract specification of what to be pushed down, and requests an pushdown SQL statement from the Adapter which can be used to retrieve the requested data.

**Request:**

Running the following query
```sql
SELECT user_id, COUNT(url)
FROM   vs.clicks
WHERE  user_id>1
GROUP  BY user_id
HAVING count(url)>1
ORDER  BY user_id
LIMIT  10;
```
will produce the following Request, assuming that the Adapter has all required capabilities.

```json
{
    "type": "pushdown",
    "pushdownRequest": {
        "type" : "select",
        "aggregationType" : "group_by",
        "from" :
        {
            "type" : "table",
            "name" : "CLICKS"
        },
        "selectList" :
        [
            {
                "type" : "column",
                "name" : "USER_ID",
                "columnNr" : 1,
                "tableName" : "CLICKS"
            },
            {
                "type" : "function_aggregate",
                "name" : "count",
                "arguments" :
                [
                    {
                        "type" : "column",
                        "name" : "URL",
                        "columnNr" : 2,
                        "tableName" : "CLICKS"
                    }
                ]
            }
        ],
        "filter" :
        {
            "type" : "predicate_less",
            "left" :
            {
                "type" : "literal_exactnumeric",
                "value" : "1"
            },
            "right" :
            {
                "type" : "column",
                "name" : "USER_ID",
                "columnNr" : 1,
                "tableName" : "CLICKS"
            }
        },
        "groupBy" :
        [
            {
                "type" : "column",
                "name" : "USER_ID",
                "columnNr" : 1,
                "tableName" : "CLICKS"
            }
        ],
        "having" :
        {
            "type" : "predicate_less",
            "left" :
            {
                "type" : "literal_exactnumeric",
                "value" : "1"
            },
            "right" :
            {
                "type" : "function_aggregate",
                "name" : "count",
                "arguments" :
                [
                    {
                        "type" : "column",
                        "name" : "URL",
                        "columnNr" : 2,
                        "tableName" : "CLICKS"
                    }
                ]
            }
        },
        "orderBy" :
        [
            {
                "type" : "order_by_element",
                "expression" :
                {
                    "type" : "column",
                    "columnNr" : 1,
                    "name" : "USER_ID",
                    "tableName" : "CLICKS"
                },
                "isAscending" : true,
                "nullsLast" : true
            }
        ],
        "limit" :
        {
            "numElements" : 10
        }
    },
    "involvedTables": [
    {
        "name" : "CLICKS",
        "columns" :
        [
            {
                "name" : "ID",
                "dataType" :
                {
                    "type" : "DECIMAL",
                    "precision" : 18,
                    "scale" : 0
                }
            },
            {
                "name" : "USER_ID",
                "dataType" :
                {
                   "type" : "DECIMAL",
                   "precision" : 18,
                    "scale" : 0
                }
            },
            {
                "name" : "URL",
                "dataType" :
                {
                   "type" : "VARCHAR",
                   "size" : 1000
                }
            },
            {
                "name" : "REQUEST_TIME",
                "dataType" :
                {
                    "type" : "TIMESTAMP"
                }
            }
        ]
    }
    ],
    "schemaMetadataInfo": {
        ...
    }
}
```

Notes
* `pushdownRequest`: Specification what needs to be pushed down. You can think of it like a parsed SQL statement.
  * `from`: The requested from clause. This can be a table or a join.
  * `selectList`: The requested select list elements, a list of expression. The order of the selectlist elements matters. If the select list is an empty list, we request at least a single column/expression, which could also be constant TRUE.
  * `selectList.columnNr`: Position of the column in the virtual table, starting with 0
  * `filter`: The requested filter (`where` clause), a single expression.
  * `aggregationType`Optional element, set if an aggregation is requested. Either `group_by` or `single_group`, if a aggregate function is used but no group by.
  * `groupBy`: The requested group by clause, a list of expressions.
  * `having`: The requested having clause, a single expression.
  * `orderBy`: The requested order-by clause, a list of `order_by_element` elements. The field `expression` contains the expression to order by.
  * `limit` The requested limit of the result set, with an optional offset.
* `involvedTables`: Metadata of the involved tables, encoded like in schemaMetadata.


**Response:**

Following the example above, a valid result could look like this:

```json
{
    "type": "pushdown",
    "sql": "IMPORT FROM JDBC AT 'jdbc:exa:remote-db:8563;schema=native' USER 'sys' IDENTIFIED BY 'exasol' STATEMENT 'SELECT USER_ID, count(URL) FROM NATIVE.CLICKS WHERE 1 < USER_ID GROUP BY USER_ID HAVING 1 < count(URL) ORDER BY USER_ID LIMIT 10'"
}
```

Notes
* `sql`: The pushdown SQL statement. It must be either an `SELECT` or `IMPORT` statement.

## Embedded Commonly Used JSON Elements

The following Json objects can be embedded in a request or response. They have a fixed structure.

### Schema Metadata Info
This document contains the most important metadata of the virtual schema and is sent to the adapter just "for information" with each request. It is the value of an element called `schemaMetadataInfo`.

```json
{"schemaMetadataInfo":{
    "name": "MY_HIVE_VSCHEMA",
    "adapterNotes": {
        "lastRefreshed": "2015-03-01 12:10:01",
        "key": "Any custom schema state here"
    },
    "properties": {
        "HIVE_SERVER": "my-hive-server",
        "HIVE_DB": "my-hive-db",
        "HIVE_USER": "my-hive-user"
    }
}}
```

### Schema Metadata

This document is usually embedded in responses from the Adapter and informs the database about all metadata of the Virtual Schema, especially the contained Virtual Tables and it's columns. The Adapter can store so called `adapterNotes` on each level (schema, table, column), to remember information which might be relevant for the Adapter in future. In the example below, the Adapter remembers the table partitioning and the data type of a column which is not directly supported in EXASOL. The Adapter has these information during pushdown and can consider the table partitioning during pushdown or can add an appropriate cast for the column.

```json
{"schemaMetadata":{
    "adapterNotes": {
        "lastRefreshed": "2015-03-01 12:10:01",
        "key": "Any custom schema state here"
    },
    "tables": [
    {
        "type": "table",
        "name": "EXASOL_CUSTOMERS",
        "adapterNotes": {
            "hivePartitionColumns": ["CREATED", "COUNTRY_ISO"]
        },
        "columns": [
        {
            "name": "ID",
            "dataType": {
                "type": "DECIMAL",
                "precision": 18,
                "scale": 0
            },
            "isIdentity": true
        },
        {
            "name": "COMPANY_NAME",
            "dataType": {
                "type": "VARCHAR",
                "size": 1000,
                "characterSet": "UTF8"
            },
            "default": "foo",
            "isNullable": false,
            "comment": "The official name of the company",
            "adapterNotes": {
                "hiveType": {
                    "dataType": "List<String>"
                }
            }
        },
        {
            "name": "DISCOUNT_RATE",
            "dataType": {
                "type": "DOUBLE"
            }
        }
        ]
    },
    {
        "type": "table",
        "name": "TABLE_2",
        "columns": [
        {
            "name": "COL1",
            "dataType": {
                "type": "DECIMAL",
                "precision": 18,
                "scale": 0
            }
        },
        {
            "name": "COL2",
            "dataType": {
                "type": "VARCHAR",
                "size": 1000
            }
        }
        ]
    }
    ]
}}
```

Notes
* `adapterNotes` is an optional field which can be attached to the schema, a table or a column. It can be an arbitrarily nested Json document.

The following EXASOL data types are supported:

#### Decimal

```json
{
    "name": "C_DECIMAL",
    "dataType": {
        "type": "DECIMAL",
        "precision": 18,
        "scale": 2
    }
}
```

#### Double

```json
{
    "name": "C_DOUBLE",
    "dataType": {
        "type": "DOUBLE"
    }
}
```

#### Varchar

```json
{
    "name": "C_VARCHAR_UTF8_1",
    "dataType": {
        "type": "VARCHAR",
        "size": 10000,
        "characterSet": "UTF8"
    }
}
```

```json
{
    "name": "C_VARCHAR_UTF8_2",
    "dataType": {
        "type": "VARCHAR",
        "size": 10000
    }
}
```

```json
{
    "name": "C_VARCHAR_ASCII",
    "dataType": {
        "type": "VARCHAR",
        "size": 10000,
        "characterSet": "ASCII"
    }
}
```

#### Char

```json
{
    "name": "C_CHAR_UTF8_1",
    "dataType": {
        "type": "CHAR",
        "size": 3
    }
}
```

```json
{
    "name": "C_CHAR_UTF8_2",
    "dataType": {
        "type": "CHAR",
        "size": 3,
        "characterSet": "UTF8"
    }
}
```

```json
{
    "name": "C_CHAR_ASCII",
    "dataType": {
        "type": "CHAR",
        "size": 3,
        "characterSet": "ASCII"
    }
}
```

#### Date

```json
{
    "name": "C_DATE",
    "dataType": {
        "type": "DATE"
    }
}
```

#### Timestamp

```json
{
    "name": "C_TIMESTAMP_1",
    "dataType": {
        "type": "TIMESTAMP"
    }
}
```
```json
{
    "name": "C_TIMESTAMP_2",
    "dataType": {
        "type": "TIMESTAMP",
        "withLocalTimeZone": false
    }
}
```
```json
{
    "name": "C_TIMESTAMP_3",
    "dataType": {
        "type": "TIMESTAMP",
        "withLocalTimeZone": true
    }
}
```

#### Boolean

```json
{
    "name": "C_BOOLEAN",
    "dataType": {
        "type": "BOOLEAN"
    }
}
```

#### Geometry

```json
{
    "name": "C_GEOMETRY",
    "dataType": {
        "type": "GEOMETRY",
        "srid": 1
    }
}
```

#### Interval

```json
{
    "name": "C_INTERVAL_DS_1",
    "dataType": {
        "type": "INTERVAL",
        "fromTo": "DAY TO SECONDS"
    }
}
```

```json
{
    "name": "C_INTERVAL_DS_2",
    "dataType": {
        "type": "INTERVAL",
        "fromTo": "DAY TO SECONDS",
        "precision": 3,
        "fraction": 4
    }
}
```

```json
{
    "name": "C_INTERVAL_YM_1",
    "dataType": {
        "type": "INTERVAL",
        "fromTo": "YEAR TO MONTH"
    }
}
```

```json
{
    "name": "C_INTERVAL_YM_2",
    "dataType": {
        "type": "INTERVAL",
        "fromTo": "YEAR TO MONTH",
        "precision": 3
    }
}
```

#### Hashtype

```json
{
    "name": "C_HASHTYPE",
    "dataType": {
        "type" : "HASHTYPE",
        "bytesize" : 16
        
    }
}
```

## Expressions

This section handles the expressions that can occur in a pushdown request. Expressions are consistently encoded in the following way. This allows easy and consisting parsing and serialization.

```json
{
    "type": "<type-of-expression>",
    ...
}
```

Each expression-type can have any number of additional fields of arbitrary type. In the following sections we define the known expressions.

### Table

This element currently only occurs in from clause

```json
{
    "type": "table",
    "name": "CLICKS",
    "alias": "A"
}
```

Notes
* **alias**: This is an optional property and is added if the table has an alias in the original query.

### Join

This element currently only occurs in from clause

```json
{
    "type": "join",
    "join_type": "inner",
    "left": { 
        ... 
    },
    "right" : { 
        ... 
    },
    "condition" : { 
        ... 
    }
}
```

Notes
* **join_type**: Can be `inner`, `left_outer`, `right_outer` or `full_outer`.
* **left**: This can be a `table` or a `join`.
* **right**: This can be a `table` or a `join`.
* **condition**: This can be an arbitrary expression.

### Column Lookup

A column lookup is a reference to a table column. It can reference the table directly or via an alias.

```json
{
    "type": "column",
    "tableName": "T",
    "tableAlias": "A",
    "columnNr": 0,
    "name": "ID"
}
```

Notes
* **tableAlias**: This is an optional property and is added if the referenced table has an alias.
* **columnNr**: Column number in the virtual table, starting with 0.

### Literal

```json
{
    "type": "literal_null"
}
```

```json
{
    "type": "literal_string",
    "value": "my string"
}
```

```json
{
    "type": "literal_double",
    "value": "1.234"
}
```

```json
{
    "type": "literal_exactnumeric",
    "value": "12345"
}
```

```json
{
    "type": "literal_bool",
    "value": true
}
```

```json
{
    "type": "literal_date",
    "value": "2015-12-01"
}
```

```json
{
    "type": "literal_timestamp",
    "value": "2015-12-01 12:01:01.1234"
}
```

```json
{
    "type": "literal_timestamputc",
    "value": "2015-12-01 12:01:01.1234"
}
```

```json
{
    "type": "literal_interval",
    "value": "+2-01",
    "dataType": {
            "type": "INTERVAL",
            "fromTo": "YEAR TO MONTH",
            "precision": 2
        }
}
```

```json
{
    "type": "literal_interval",
    "value": "+0 00:00:02.000",
    "dataType": {
            "type": "INTERVAL",
            "fromTo": "DAY TO SECONDS",
            "precision": 2,
            "fraction": 2
        }
}
```

### Predicates

Whenever there is `...` this is a shortcut for an arbitrary expression.

##### AND / OR

```json
{
    "type": "predicate_and",
    "expressions": [
        ...
    ]
}
```

The same can be used for `predicate_or`.

##### NOT / IS NULL / IS NOT NULL

```json
{
    "type": "predicate_not",
    "expression": {
        ...
    }
}
```

The same can be used for `predicate_is_null`, `predicate_is_not_null`.

##### Comparison operators

```json
{
    "type": "predicate_equal",
    "left": {
        ...
    },
    "right": {
        ...
    }
}
```

The same can be used for `predicate_notequal`, `predicate_less` and `predicate_lessequal`.

##### LIKE / REGEXP_LIKE

```json
{
    "type": "predicate_like",
    "expression": {
        ...
    },
    "pattern": {
        ...
    },
    "escapeChar": "%"
}
```

The same can be used for `predicate_like_regexp`.

Notes
* **escapeChar** is optional

##### BETWEEN

```json
{
    "type": "predicate_between",
    "expression": {
        ...
    },
    "left": {
        ...
    },
    "right": {
        ...
    }
}
```

##### IN

`<exp> IN (<const1>, <const2>)`

```json
{
    "type": "predicate_in_constlist",
    "expression": {
        ...
    }
    "arguments": [
        ...
    ]
}
```

##### IS JSON / IS NOT JSON

`exp1 IS JSON {VALUE | ARRAY | OBJECT | SCALAR} {WITH | WITHOUT} UNIQUE KEYS`
 (requires predicate capability `IS_JSON`)

```json
{
    "type": "predicate_is_json",
    "expression": {
        ...
    },
    "typeConstraint": "VALUE",
    "keyUniquenessConstraint": "WITHOUT UNIQUE KEYS"
}
```

Notes:

- typeConstraint is `"VALUE"`, `"ARRAY"`, `"OBJECT"`, or `"SCALAR"`.
- keyUniquenessConstraint is `"WITH UNIQUE KEYS"` or `"WITHOUT UNIQUE KEYS"`.

The same can be used for a predicate type `predicate_is_not_json` (requires predicate capability `IS_NOT_JSON`).

### Scalar Functions

A scalar function with a single argument (consistent with multiple argument version):

```json
{
    "type": "function_scalar",
    "numArgs": 1,
    "name": "ABS",
    "arguments": [
    {
        ...
    }
    ]
}
```

A scalar function with multiple arguments:

```json
{
    "type": "function_scalar",
    "numArgs": 2,
    "name": "POWER",
    "arguments": [
    {
        ...
    },
    {
        ...
    }
    ]
}
```

```json
{
    "type": "function_scalar",
    "variableInputArgs": true,
    "name": "CONCAT",
    "arguments": [
    {
        ...
    },
    {
        ...
    },
    {
        ...
    }
    ]
}
```

Notes
* **variableInputArgs**: default value is false. If true, `numArgs` is not defined.

##### ADD / SUB / MULT / FLOAT_DIV

Arithmetic operators have following names: `ADD`, `SUB`, `MULT`, `FLOAT_DIV`. They are defined as infix (just a hint, not necessary).

```json
{
    "type": "function_scalar",
    "numArgs": 2,
    "name": "ADD",
    "infix": true,
    "arguments": [
    {
        ...
    },
    {
        ...
    }
    ]
}
```

#### Special Cases of Scalar Functions

##### EXTRACT

`EXTRACT(toExtract FROM exp1)` (requires scalar-function capability `EXTRACT`) 

```json
{
    "type": "function_scalar_extract",
    "name": "EXTRACT",
    "toExtract": "MINUTE",
    "arguments": [
    {
        ...
    }
    ],
}
```

##### CAST

`CAST(exp1 AS dataType)` (requires scalar-function capability `CAST`)

```json
{
    "type": "function_scalar_cast",
    "name": "CAST",
    "dataType": 
    {
        "type" : "VARCHAR",
        "size" : 10000
    },
    "arguments": [
    {
        ...
    }
    ],
}
```

##### CASE

`CASE` (requires scalar-function capability `CAST`)

```sql
CASE basis WHEN exp1 THEN result1
           WHEN exp2 THEN result2
           ELSE result3
           END
```

```json
{
    "type": "function_scalar_case",
    "name": "CASE",
    "basis" :
    {
        "type" : "column",
        "columnNr" : 0,
        "name" : "NUMERIC_GRADES",
        "tableName" : "GRADES"
    },
    "arguments": [
    {        
        "type" : "literal_exactnumeric",
        "value" : "1"
    },       
    {        
        "type" : "literal_exactnumeric",
        "value" : "2"
    }
    ],
    "results": [
    {        
        "type" : "literal_string",
        "value" : "VERY GOOD"
    },       
    {        
        "type" : "literal_string",
        "value" : "GOOD"
    },
    {        
        "type" : "literal_string",
        "value" : "INVALID"
    }
    ]
}
```
Notes:
* `arguments`: The different cases.
* `results`: The different results in the same order as the arguments. If present, the ELSE result is the last entry in the `results` array.

##### JSON_VALUE

`JSON_VALUE(arg1, arg2 RETURNING dataType {ERROR | NULL | DEFAULT exp1} ON EMPTY {ERROR | NULL | DEFAULT exp2} ON ERROR)`
 (requires scalar-function capability `JSON_VALUE`)
```json
{
    "type": "function_scalar_json_value",
    "name": "JSON_VALUE",
    "arguments":
    [
        {
            ...
        },
        {
            ...
        }
    ],
    "returningDataType": dataType,
    "emptyBehavior":
    {
        "type": "ERROR"
    },
    "errorBehavior":
    {
        "type": "DEFAULT",
        "expression": exp2
    }
}
```

Notes:

- arguments: Contains two entries: The JSON item and the path specification.
- emptyBehavior and errorBehavior: `type` is `"ERROR"`, `"NULL"`, or `"DEFAULT"`. Only for `"DEFAULT"` the member `expression` containing the default value exists.

### Aggregate Functions

Consistent with scalar functions. To be detailed: `star-operator`, `distinct`, ...

An aggregate function with a single argument (consistent with multiple argument version):

```json
{
    "type": "function_aggregate",
    "name": "SUM",
    "arguments": [
    {
        ...
    }
    ]
}
```

An aggregate function with multiple arguments:

```json
{
    "type": "function_aggregate",
    "name": "CORR",
    "arguments": [
    {
        ...
    },
    {
        ...
    }
    ]
}
```

#### Special Cases of Aggregate Functions

##### COUNT(exp)

`COUNT(exp)`
 (requires set-function capability `COUNT`)

```json
{
    "type": "function_aggregate",
    "name": "COUNT",
    "arguments": [
    {
        ...
    }
    ]
}
```
##### COUNT(*)

`COUNT(*)`
 (requires set-function capability `COUNT` and `COUNT_STAR`)

```json
{
    "type": "function_aggregate",
    "name": "COUNT"
}
```

##### COUNT(DISTINCT exp)

`COUNT(DISTINCT exp)`
 (requires set-function capability `COUNT` and `COUNT_DISTINCT`)

```json
{
    "type": "function_aggregate",
    "name": "COUNT",
    "distinct": true,
    "arguments": [
    {
        ...
    }
    ]
}
```

##### COUNT((exp1, exp2))

`COUNT((exp1, exp2))`
(requires set-function capability `COUNT` and `COUNT_TUPLE`)

```json
{
    "type": "function_aggregate",
    "name": "COUNT",
    "distinct": true,
    "arguments": [
    {
        ...
    },
    {
        ...
    }
    ]
}
```

##### AVG(exp)

`AVG(exp)`
 (requires set-function capability `AVG`)

```json
{
    "type": "function_aggregate",
    "name": "AVG",
    "arguments": [
    {
        ...
    }
    ]
}
```

##### AVG(DISTINCT exp)

`AVG(DISTINCT exp)`
 (requires set-function capability `AVG` and `AVG_DISTINCT`)

```json
{
    "type": "function_aggregate",
    "name": "AVG",
    "distinct": true,
    "arguments": [
    {
        ...
    }
    ]
}
```

##### GROUP_CONCAT

`GROUP_CONCAT(DISTINCT exp1 orderBy SEPARATOR ', ')`
 (requires set-function capability `GROUP_CONCAT`)

```json
{
    "type": "function_aggregate_group_concat",
    "name": "GROUP_CONCAT",
    "distinct": true,
    "arguments": [
    {
        ...
    }
    ],
    "orderBy" : [
        {
            "type" : "order_by_element",
            "expression" :
            {
              "type" : "column",
               "columnNr" : 1,
                "name" : "USER_ID",
                "tableName" : "CLICKS"
            },
            "isAscending" : true,
            "nullsLast" : true
        }
    ],
    "separator": ", "
}
```

Notes:
* `distinct`: Optional. Requires set-function capability `GROUP_CONCAT_DISTINCT.`
* `orderBy`: Optional. The requested order-by clause, a list of `order_by_element` elements. The field `expression` contains the expression to order by. The `group by` clause of a `SELECT` query uses the same `order_by_element` element type. The clause requires the set-function capability `GROUP_CONCAT_ORDER_BY`.
* `separator`: Optional. Requires set-function capability `GROUP_CONCAT_SEPARATOR`.
