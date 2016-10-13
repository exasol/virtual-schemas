# Python Redis Adapter
This Virtual Schema Adapter exposes the key value store [Redis](http://redis.io/) as a virtual table.
It is a very simple Adapter for demonstration purposes, but still tries to address a real use case.

You can directly jump into the Adapter Code if you like: [python-redis-adapter.sql](https://github.com/EXASOL/virtual-schema-jdbc-adapter/blob/master/python-redis-adapter/python-redis-adapter.sql)

This Adapter aims to demonstrate
* how to write a simple Adapter, with less than 100 lines of code
* how to use UDFs for pushdown
* how you can map a NoSQL data source to a virtual schema
* how you can control the pushdown behaviour via properties

This Adapter is NOT meant
* to show how you write a good and stable Adapter for Redis
* to be used in production

## How to use
First start redis and make sure that all EXASolution nodes can access redis.

Then run all statements in the [python-redis-adapter.sql](https://github.com/EXASOL/virtual-schema-jdbc-adapter/blob/master/python-redis-adapter/python-redis-adapter.sql) file to create the Adapter Script and the UDFs.

You can now use the Virtual Schema as follows:
```sql
-- Create the virtual schema pointing to your redis server
CREATE VIRTUAL SCHEMA redis USING adapter.redis_adapter WITH
  REDIS_HOST = 'localhost'
  REDIS_PORT = '6379';

-- This will create a virtual table KEY_VALS, with a key and a value column
DESCRIBE KEY_VALS;

-- The recommended way is to query by key.
-- The Adapter supports pushdown for filters like KEY = 'value'.
-- This gets pushed down as redis "get" operation, which is extremely fast
SELECT * FROM key_vals WHERE KEY = 'foo';

-- This will run a scan on redis, which is pretty slow
SELECT * FROM key_vals;

-- This also gets pushed down as scan, and filter happens in database.
-- It could easily be improved by adding the FN_PRED_LIKE capability and push it down to Redis, which offers pattern based search.
SELECT * FROM key_vals WHERE KEY like 'foo%';

-- Projection is also not pushed down, so little overhead here
SELECT "VALUE" FROM key_vals WHERE KEY = 'foo';

-- We can also change or add properties, here to change the pushdown behaviour
ALTER VIRTUAL SCHEMA redis SET DISABLE_SCAN='TRUE';

-- Now all queries where the KEY = 'VALUE' filter cannot be pushed down will fail.
-- We could also change the behaviour and return an empty table, but this is less intuitive for the user
SELECT * FROM key_vals;

```
