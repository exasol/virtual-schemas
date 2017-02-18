# DB2 Dialect
Short description of DB2 Dialect functionality and features

## Casting of Data Types
* TIMESTAMP and TIMESTAMP(x) will be cast to VARCHAR to not lose precision.
* VARCHAR and CHAR for bit data will be cast to a hex string with double the original size
* TIME will be cast to VARCHAR(8)
* XML will be cast to VARCHAR(DB2_MAX_LENGTH)
* BLOB is not supported

## Casting of Functions
* LIMIT will replaced by FETCH FIRST x ROWS ONLY
* OFFESET currently not supported as only DB2 V11 support this
* ADD_DAYS, ADD_WEEKS ... will be replaced by COLUMN + DAYS, COLUMN + ....

## Unit Tests 
Unit Test setup how to has been included with various tests.
See [setup sql file](../integration-test-data/db2-testdata.sql) for details
