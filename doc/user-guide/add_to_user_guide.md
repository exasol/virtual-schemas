# Unrecognized and Unsupported Data Types

Not all data types present in a source database have a matching equivalent in Exasol. Also software updates on the source database can introduce new data type that the Virtual schema does not recognize.

There are a few important things you need to know about those data types.

1. Columns of an unrecognized / unsupported data type are not mapped in a Virtual Schema. From Exasol's perspective those columns do not exist on a table. This is done so that tables containing those columns can still be mapped and do not have to be rejected as a whole.
2. You can't query columns of an unrecognized / unsupported data type. If the source table contains them, you have to *explicitly* exclude them from the query. You can for example not use the asterisk (`*`) on a table that contains one ore more of those columns. This will result in an error issued by the Virtual schema.
3. If you want to query all columns except unsupported, add `1` to the columns list. Otherwise you will see the same error as if you query with the asterisk (`*`).
    For example, a table contains 3 columns: `bool_column` BOOLEAN, `timestamp_column` TIMESTAMP, `blob_column` BLOB. The column BLOB is not supported. If you want to query two other columns, use: `SELECT "bool_column", "timestamp_column", 1 FROM table_name;` .
4. You can't use functions that result in an unsupported / unknown data type. 
