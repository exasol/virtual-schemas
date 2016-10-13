
-- Execute following statements in Impala:
create table all_hive_impala_types (
 c1 tinyint,
 c2 smallint,
 c3 int,
 c4 bigint,
 c5 float,
 c6 double,
 c7 decimal,
 c8 decimal(12,2),
 c9 decimal(38,2),
 c10 timestamp,
 c11 string,
 c12 varchar(1000),
 c13 char(10),
 c14 boolean
);

insert into all_hive_impala_types values (
 123,
 12345,
 1234567890,
 1234567890123456789,
 12.2,
 12.2,
 12345,
 12345.12,
 12345.12,
 '1985-09-25 17:45:30.005',
 'abc',
 CAST('varchar 茶' AS VARCHAR(1000)),
 CAST('char 茶' AS CHAR(10)),
 true
);

CREATE TABLE simple(a int, b string, c double);

INSERT INTO simple VALUES
 (1, 'a', 1.1),
 (2, 'b', 2.2),
 (3, 'c', 3.3),
 (1, 'd', 4.4),
 (2, 'e', 5.5),
 (3, 'f', 6.6);

CREATE TABLE simple_with_nulls (c1 int, c2 string);

INSERT INTO simple_with_nulls VALUES 
 (1, 'a'),
 (2, null),
 (3, 'b'),
 (1, null),
 (null, 'c');

