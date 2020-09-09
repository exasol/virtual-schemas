# Exasol Scalar Functions API

This page describes how Exasol scalar functions map to the Virtual Schemas push-down request API.

## Exasol Scalar Functions List

### Functions With a Common API

The majority of functions supported by the Exasol database shares a common API syntax depending on the number of function arguments:

* [Functions without arguments](#functions-without-arguments);
* [Functions with a single argument](#functions-with-a-single-argument);
* [Functions with two or more arguments](#functions-with-two-or-more-arguments);
* [Functions with a variable number of arguments](#functions-with-a-variable-number-of-arguments).
  Here belong a few functions, some of them are treated as a function with variable number of arguments despite the fact that actually the number of arguments is limited:
  BIT_TO_NUM, CONCAT, DUMP, GREATEST, INSERT, INSTR, LEAST, LOCATE, REGEXP_INSTR, REGEXP_REPLACE, REGEXP_SUBSTR.

Note that function names go to the `<function name>` placeholder of the API examples. 

The functions that do not belong to this common API group are described in the following sections:

- [Functions with a special API](#functions-with-a-special-api)
- [Functions not included in the the API](#functions-not-included-in-the-api)

### Functions With a Special API

This section contains functions that have a special API mapping.
  
| Function Name     | API mapping link                      |
|-------------------|---------------------------------------|
| EXTRACT           | [EXTRACT function](#extract-function) |
| CASE              | [CASE function](#case-function)       |
| CAST              | [CAST function](#cast-function)       |
| JSON_VALUE        | [JSON_VALUE function](#json_value-function)       |
     
### Functions not Included in the API     

This section contains Exasol functions which do not appear in the API.
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

### Functions Without Arguments

A scalar function without arguments has the following JSON structure:

```json
{
  "type": "function_scalar",
  "name": "<function name>",
  "numArgs": 0,
  "arguments": []
}
```

### Functions With a Single Argument

A scalar function with a single argument has the following JSON structure:

```json
{
  "type": "function_scalar",
  "name": "<function name>",
  "numArgs": 1,
  "arguments": [
    {
      ...
    }
  ]
}
```

For example, for the query `SELECT ABS(c5) FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES` the scalar function part of the JSON request might look like this:

```json
{
  "type": "function_scalar",
  "name": "ABS",
  "numArgs": 1,
  "arguments": [
    {
      "columnNr": 4,
      "name": "C5",
      "tableName": "ALL_EXASOL_TYPES",
      "type": "column"
    }
  ]
}
```

### Functions With Two or More Arguments

A scalar function with two arguments has the following JSON structure:

```json
{
  "type": "function_scalar",
  "name": "<function name>",
  "numArgs": 2,
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

If a function has more than two arguments, the `numArgs` field has a different value corresponding to the number of arguments.
Also, the `arguments` list has a corresponding amount of nested elements.

Let us check an example of the API part containing the scalar function with two arguments for the following query `SELECT ATAN2(c5, c6) FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES`:

```json
{
  "type": "function_scalar",
  "name": "ATAN2",
  "numArgs": 2,
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
  ]
}
```

### Functions With a Variable Number of Arguments

Some scalar functions do not have a constant number of arguments. A function with a variable number of arguments has the following JSON structure:

```json
{
  "type": "function_scalar",
  "name": "<function name>",
  "variableInputArgs" : true,
  "arguments": [
    ...
  ]
}
```

For example, for the query `SELECT CONCAT('prefix_', c2) VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES` the scalar function part of the JSON request might look like this:

```json
{
  "type": "function_scalar",
  "name": "CONCAT",
  "variableInputArgs": true,
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
  ]
}
```

### EXTRACT Function

`EXTRACT(toExtract FROM exp1)` (requires scalar-function capability `EXTRACT`).

The EXTRACT function takes one argument and has a special field `toExtract`.

```json
{
  "type": "function_scalar_extract",
  "name": "EXTRACT",
  "toExtract": "<YEAR/MONTH/DAY/HOUR/MINUTE/SECOND>",
  "arguments": [
    {
      ...
    }
  ]
}
```

### CAST function

`CAST(exp1 AS dataType)` (requires scalar-function capability `CAST`).

The CAST function takes one argument and has a special field `dataType` describing the datatype to cast to.

```json
{
  "type": "function_scalar_cast",
  "name": "CAST",
  "arguments": [
    {
      ...
    }
  ],
  "dataType": {
    ...
  }
}
```
For example, for the query `SELECT CAST(c5 AS VARCHAR(10)) FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES` the CAST function part of the JSON request will look like this:

```json
{
  "type": "function_scalar_cast",
  "name": "CAST",
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
  }
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
  "type": "function_scalar_case",
  "name": "CASE",
  "basis": {
    ...
  },
  "arguments": [
    ...
  ],
  "results": [
    ...
  ]
}
```

Notes:
* `arguments`: The different cases.
* `results`: The different results in the same order as the arguments. If present, the ELSE result is the last entry in the `results` array.

Here is an example of a query containing a CASE function and its JSON representation (only the function part): 
```sql
SELECT CASE grade
          WHEN 1 THEN 'GOOD'
          WHEN 2 THEN 'FAIR' 
          WHEN 3 THEN 'POOR'
          ELSE 'INVALID'
          END
FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES;
```

```json
{
  "type": "function_scalar_case",
  "name": "CASE",
  "basis": {
    "columnNr": 4,
    "name": "grade",
    "tableName": "ALL_EXASOL_TYPES",
    "type": "column"
  },
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
  ]
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

* `arguments`: Contains two entries: The JSON item and the path specification.
* `emptyBehavior` and `errorBehavior`: `type` is `"ERROR"`, `"NULL"`, or `"DEFAULT"`. Only for `"DEFAULT"` the member `expression` containing the default value exists.


## Additional Information

A scalar function, that does not contain any column references, is executed before reaching Virtual Schemas.
That means the JSON request does not contain the scalar function, but a literal value representing its result.
For example, the query `SELECT ABS(-123), c5 FROM VIRTUAL_SCHEMA_EXASOL.ALL_EXASOL_TYPES` will have the following select list:
  
```json
{
  ...

  "selectList": [
    {
      "type": "literal_exactnumeric",
      "value": "123"
    },
    {
      "type": "column",
      "tableName": "ALL_EXASOL_TYPES",
      "columnNr": 4,
      "name": "C5"
    }
  ],
  
  ...
}
```
