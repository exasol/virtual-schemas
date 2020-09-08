# Exasol Scalar Functions API

This page describes how Exasol scalar functions map to the Virtual Schemas push-down request API.

## Exasol Scalar Functions List

### Functions with Common API

The most part of functions supported by Exasol database share a common API syntax depending on a number of arguments a function takes:

* [Function without arguments](#function-with-a-single-argument);
* [Function with a single argument](#function-with-a-single-argument);
* [Function with two or more arguments](#function-with-two-or-more-arguments);
* [Function with unspecified number of arguments](#function-with-unspecified-number-of-arguments).
  Here belongs a few functions, some of them are treated as a function with unspecified number of arguments despite a fact that actually a number of arguments is limited:
  BIT_TO_NUM, CONCAT, DUMP, GREATEST, INSERT, INSTR, LEAST, LOCATE, REGEXP_INSTR, REGEXP_REPLACE, REGEXP_SUBSTR.

Note that function names go to the `<function name>` placeholder of the API examples. 

The functions that does not belong to this common group are describe in the following sections:

- [Functions with a special API](#functions-with-special-api)
- [Functions not included into the API](#functions-not-included-into-api)

### Functions with Special API

This section contains functions that have a special API mapping.
  
| Function Name     | API mapping link                      |
|-------------------|---------------------------------------|
| EXTRACT           | [EXTRACT function](#extract-function) |
| CASE              | [CASE function](#case-function)       |
| CAST              | [CAST function](#cast-function)       |
| JSON_VALUE        | [JSON_VALUE function](#json_value-function)       |
     
### Functions not Included into API     

This section contains Exasol function which do not appear in the API.
See [Additional Information](#additional-information) for an explanation why functions do not appear in the API.

| Function Name     | Comment                                                                                        |
|-------------------|------------------------------------------------------------------------------------------------|
| BIT_LROTATE       | Does not appear in the API.                                                                    |
| BIT_LSHIFT        | Does not appear in the API.                                                                    |
| BIT_RROTATE       | Does not appear in the API.                                                                    |
| BIT_RSHIFT        | Does not appear in the API.                                                                    |
| CEILING           | API uses the CEIL function.                                                                    |
| CHAR              | API uses the CHR function.                                                                     |
| CHARACTER_LENGTH  | API uses the LENGTH function.                                                                  |
| COALESCE          | API uses the CASE function.                                                                  |
| CONVERT           | API uses the CAST function.                                                                  |
| CURDATE           | API uses the CURRENT_DATE function.                                                            |
| DECODE            | API uses the CASE function.                                                            |
| FROM_POSIX_TIME   | Does not appear in the API.                                                                    |
| HOUR              | Does not appear in the API.                                                                    |
| INITCAP           | Does not appear in the API.                                                                    |
| IPROC             | Does not appear in the API.                                                                    |
| LCASE             | API uses the LOWER function.                                                                   |
| LEFT              | API uses the SUBSTR function. The `position argument` is always set to 1.                      |
| LOG2              | API uses the LOG function, one argument is always a `literal_exactnumeric` with a value 2.     |
| MID               | API uses the SUBSTR function.                                                                  |
| NVL               | API uses the CASE function.                                                                  |
| NVL2              | API uses the CASE function.                                                                  |
| NOW               | API uses the CURRENT_TIMESTAMP function.                                                                  |
| NPROC             | Does not appear in the API.                                                                    |
| NULLIF            | API uses the CASE function.                                                                  |
| PI                | Does not appear in the API.                                                                    |
| POSITION          | API uses the INSTR function with arguments `string` and `search_string`.                       |
| RANDOM            | API uses the RAND function.                                                                    |
| ROWID             | Does not appear in the API.                                                                    |
| ROWNUM            | Does not appear in the API.                                                                    |
| SCOPE_USER        | Does not appear in the API.                                                                    |
| SUBSTRING         | API uses the SUBSTR function.                                                                  |
| TRUNCATE          | API uses the TRUNC function.                                                                   |
| UCASE             | API uses the UPPER function.                                                                   |
| USER              | API uses the CURRENT_USER function.                                                                   |
| VALUE2PROC        | Does not appear in the API.                                                                  |

* Functions for hierarchical queries are not in the API.

## Scalar Functions API

### Function Without Arguments

A scalar function without arguments has a following JSON structure:

```json
{
  "arguments": [],
  "name": "<function name>",
  "numArgs": 0,
  "type": "function_scalar"
}
```

### Function With a Single Argument

A scalar function with a single argument has a following JSON structure:

```json
{
  "arguments": [
    {
      ...
    }
  ],
  "name": "<function name>",
  "numArgs": 1,
  "type": "function_scalar"
}
```

For example, for a query `SELECT ABS(c5) FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES` the scalar function part of the JSON request might look like this:

```json
{
  "arguments": [
    {
      "columnNr": 4,
      "name": "C5",
      "tableName": "ALL_EXASOL_TYPES",
      "type": "column"
    }
  ],
  "name": "ABS",
  "numArgs": 1,
  "type": "function_scalar"
}
```

### Function With Two or More Arguments

A scalar function with two argument has a following JSON structure:

```json
{
  "arguments": [
    {
      ...
    },
    {
      ...
    }
  ],
  "name": "<function name>",
  "numArgs": 2,
  "type": "function_scalar"
}
```

If a function has more than two arguments, the `numArgs` field has a different value corresponding to a number of arguments.
Also, the `arguments` list has a corresponding amount of nested elements.

Let us check an example of the API part containing the scalar function with two arguments for the following query `SELECT ATAN2(c5, c6) FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES`:

```json
{
  "arguments": [
    {
      "columnNr": 4,
      "name": "C5",
      "tableName": "ALL_EXASOL_TYPES",
      "type": "column"
    },
    {
      "columnNr": 5,
      "name": "C6",
      "tableName": "ALL_EXASOL_TYPES",
      "type": "column"
    }
  ],
  "name": "ATAN2",
  "numArgs": 2,
  "type": "function_scalar"
}
```

### Function With Unspecified Number of Arguments

Some scalar functions do not have a defined number of arguments. A function with an undefined number of arguments has a following JSON structure:

```json
{
  "arguments": [
    ...
  ],
  "name": "<function name>",
  "type": "function_scalar",
  "variableInputArgs" : true
}
```

For example, for a query `SSELECT CONCAT('prefix_', c2) VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES` the scalar function part of the JSON request might look like this:

```json
{
  "arguments": [
    {
      "type": "literal_string",
      "value": "prefix_"
    },
    {
      "columnNr": 1,
      "name": "C2",
      "tableName": "ALL_EXASOL_TYPES",
      "type": "column"
    }
  ],
  "name": "CONCAT",
  "type": "function_scalar",
  "variableInputArgs": true
}
```

### EXTRACT Function

`EXTRACT(toExtract FROM exp1)` (requires scalar-function capability `EXTRACT`).

EXTRACT function takes one argument and has a special field `toExtract`.

```json
{
  "arguments": [
    {
      ...
    }
  ],
  "name": "EXTRACT",
  "toExtract": "<YEAR/MONTH/DAY/HOUR/MINUTE/SECOND>",
  "type": "function_scalar_extract"
}
```

### CAST function

`CAST(exp1 AS dataType)` (requires scalar-function capability `CAST`).

CAST function takes one argument and has a special field `dataType` describing a datatype to cast to.

```json
{
  "arguments": [
    {
      ...
    }
  ],
  "dataType": {
    ...
  },
  "name": "CAST",
  "type": "function_scalar_cast"
}
```
For example, for a query `SELECT CAST(c5 AS VARCHAR(10)) FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES` the CAST function part of the JSON request will look like this:

```json
{
  "arguments": [
    {
      "columnNr": 4,
      "name": "C5",
      "tableName": "ALL_EXASOL_TYPES",
      "type": "column"
    }
  ],
  "dataType": {
    "size": 10,
    "type": "VARCHAR"
  },
  "name": "CAST",
  "type": "function_scalar_cast"
}
```

 ### CASE function

`CASE` (requires scalar-function capability `CASE`)

```sql
CASE basis WHEN exp1 THEN result1
           WHEN exp2 THEN result2
           ELSE result3
           END
```

```json
{
  "arguments": [
    ...
  ],
  "basis": {
    ...
  },
  "name": "CASE",
  "results": [
    ...
  ],
  "type": "function_scalar_case"
}
```

Notes:
* `arguments`: The different cases.
* `results`: The different results in the same order as the arguments. If present, the ELSE result is the last entry in the `results` array.

Here is an example of a query containing a CASE function and its JSON representation(only the function part): 
```sql
SELECT CASE grade
          WHEN 1 THEN 'GOOD'
          WHEN 2 THEN 'FAIR' 
          WHEN 3 THEN 'POOR'
          ELSE 'INVALID'
          END FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES;
```

```json
{
  "arguments": [
    {
      "type": "literal_exactnumeric",
      "value": "1"
    },
    {
      "type": "literal_exactnumeric",
      "value": "2"
    },
    {
      "type": "literal_exactnumeric",
      "value": "3"
    }
  ],
  "basis": {
    "columnNr": 4,
    "name": "grade",
    "tableName": "ALL_EXASOL_TYPES",
    "type": "column"
  },
  "name": "CASE",
  "results": [
    {
      "type": "literal_string",
      "value": "GOOD"
    },
    {
      "type": "literal_string",
      "value": "FAIR"
    },
    {
      "type": "literal_string",
      "value": "POOR"
    },
    {
      "type": "literal_string",
      "value": "INVALID"
    }
  ],
  "type": "function_scalar_case"
}
```

### JSON_VALUE Function

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


## Additional Information

* A scalar function in a select list that does not contain any column reference executes before reaching Virtual Schemas.
  That means the JSON request does not contain a scalar function, but a literal value representing a result.
  For example, a query `SELECT ABS(-123), c5 FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES` will have a following select list:
  
```json
{
  ...

  "selectList": [
    {
      "type": "literal_exactnumeric",
      "value": "123"
    },
    {
      "columnNr": 4,
      "name": "C5",
      "tableName": "ALL_EXASOL_TYPES",
      "type": "column"
    }
  ],
  
  ...
}
```