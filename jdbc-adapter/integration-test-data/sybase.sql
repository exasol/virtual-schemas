DROP TABLE testdb.tester.ittable go
CREATE TABLE testdb.tester.ittable (
    a varchar(100),
    b decimal
) go

INSERT INTO testdb.tester.ittable (a, b) VALUES('e', 2)
INSERT INTO testdb.tester.ittable (a, b) VALUES('b', 3)
INSERT INTO testdb.tester.ittable (a, b) VALUES(NULL, -1)
INSERT INTO testdb.tester.ittable (a, b) VALUES('a', NULL)
INSERT INTO testdb.tester.ittable (a, b) VALUES('z', 0)
INSERT INTO testdb.tester.ittable (a, b) VALUES('z', 0) go

DROP TABLE testdb.tester.timetypes go
CREATE TABLE testdb.tester.timetypes (
    c_smalldatetime smalldatetime,
    c_datetime datetime,
    c_date date,
    c_time time,
    c_bigdatetime bigdatetime, -- error data truncation
    c_bigtime bigtime
) go

INSERT INTO testdb.tester.timetypes
  VALUES('1.1.1900 01:02',
         '1.1.1753 01:02:03.100',
         '12/3/2032',
         '11:22:33.456',
         '6.4.1553 11:11:11.111111',
         '11:11:11.111111'
    )
go


-- https://help.sap.com/viewer/b65d6a040c4a4709afd93068071b2a76/16.0.3.5/en-US/aa354eb4bc2b101495d29877b5bd3c5b.html DROP TABLE testdb.tester.integertypes go CREATE TABLE testdb.tester.integertypes (
  c_bigint bigint,
  c_int int,
  c_smallint smallint,
  c_ubigint unsigned bigint,
  c_uint unsigned int,
  c_usmallint unsigned smallint
) go

INSERT INTO testdb.tester.integertypes
  VALUES(-9223372036854775808,
         -2147483648,
         -32768,
         0,
         0,
         0
    )
INSERT INTO testdb.tester.integertypes
  VALUES(9223372036854775807,
         2147483647,
         32767,
         18446744073709551615,
         4294967295,
         65535
  )
go


-- https://help.sap.com/viewer/b65d6a040c4a4709afd93068071b2a76/16.0.3.5/en-US/aa357b76bc2b1014ba159ac9d0074e1d.html
DROP TABLE testdb.tester.decimaltypes go
CREATE TABLE testdb.tester.decimaltypes (
    c_numeric_36_0 numeric(36, 0),
    c_numeric_38_0 numeric(38, 0),
    c_decimal_20_10 decimal(20, 10),
    c_decimal_37_10 decimal(37, 10)
) go

INSERT INTO testdb.tester.decimaltypes
VALUES(12345678901234567890123456,
       1234567890123456789012345678,
       1234567890.0123456789,
       12345678901234567.0123456789
)
INSERT INTO testdb.tester.decimaltypes
  VALUES(-12345678901234567890123456,
         -1234567890123456789012345678,
         -1234567890.0123456789,
         -12345678901234567.0123456789
  )
go


-- https://help.sap.com/viewer/b65d6a040c4a4709afd93068071b2a76/16.0.3.5/en-US/aa357b76bc2b1014ba159ac9d0074e1d.html
-- FLOAT(p) is alias for either DOUBLE PRECISION or REAL. If p < 16, FLOAT is stored as REAL, if p >= 16, FLOAT is stored as DOUBLE PRECISION.
DROP TABLE testdb.tester.approxtypes go
CREATE TABLE testdb.tester.approxtypes (
    c_double double precision,
    c_real real,
) go

INSERT INTO testdb.tester.approxtypes VALUES(
    2.2250738585072014e-308,
    1.175494351e-38
)
INSERT INTO testdb.tester.approxtypes VALUES(
    1.797693134862315708e+308,
    3.402823466e+38
)
go


DROP TABLE testdb.tester.moneytypes go
CREATE TABLE testdb.tester.moneytypes (
    c_smallmoney smallmoney,
    c_money money,
) go

INSERT INTO testdb.tester.moneytypes VALUES(
  214748.3647,
  922337203685477.5807
)
INSERT INTO testdb.tester.moneytypes VALUES(
  -214748.3648,
  -922337203685477.5808
)
go


-- https://help.sap.com/viewer/b65d6a040c4a4709afd93068071b2a76/16.0.3.5/en-US/aa362f6cbc2b1014b1ed808e2a54e693.html
DROP TABLE testdb.tester.chartypes go
CREATE TABLE testdb.tester.chartypes (
    c_char_10 char(10),
    c_char_toobig char(2001),
    c_varchar varchar(10), -- maximum size in Sybase is 16384 -> smaller than Exasol's limit
    c_unichar_10 unichar(10), -- NOT right-padded with spaces
    c_unichar_toobig unichar(8192), -- NOT right-padded with spaces
    c_univarchar univarchar(10), -- maximum size is 8192
    c_nchar nchar(10), -- maximum size in Sybase is 16384. NOT right-padded with spaces.
    c_nvarchar nvarchar(10), -- maximum size in Sybase is 16384
    c_text text,
    c_unitext unitext
) go

INSERT INTO testdb.tester.chartypes VALUES(
    'abcd',
    'Lorem ipsum dolor sit amet... rest is zero.',
    'Lorem.',
    'Ipsum.',
    'xyz',
    'Dolor.',
    'Sit.',
    'Amet.',
    'Text. A wall of text.',
    'Text. A wall of Unicode text.'
) go


DROP TABLE testdb.tester.misctypes go
CREATE TABLE testdb.tester.misctypes (
  c_binary binary(10), -- n <= 255
  c_varbinary varbinary(10),
  c_image image,
  c_bit bit NOT NULL
) go

INSERT INTO testdb.tester.misctypes VALUES(
  0xdeadbeef,
  0xdeadbeef,
  0xdeadbeef,
  0
) go
