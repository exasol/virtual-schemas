-- ORACLE 12

CREATE USER loader IDENTIFIED BY loader;

GRANT CONNECT TO loader;
GRANT CREATE SESSION TO loader;
GRANT UNLIMITED TABLESPACE TO loader;

DROP TABLE LOADER.TYPE_TEST;
CREATE TABLE LOADER.TYPE_TEST (
  c1 char(50),
  c2 nchar(50),
  c3 varchar2(50),
  c4 nvarchar2(50),
  c5 number,
  c_number36 number(36),
  c6 number(38),
  c7 number(10,5),
  c_binfloat binary_float,
  c_bindouble binary_double,
  c10 date,
  c11 timestamp(3),
  c12 timestamp,
  c13 timestamp(9),
  c14 timestamp with time zone,
  c15 timestamp with local time zone,
  c16 interval year to month,
  c17 interval day to second,
  c18 blob,
  c19 clob,
  c20 nclob,
  --c21 rowid,
  --c22 urowid,
  c_float float,
  c_float126 float(126),
  c_long long
);

INSERT INTO LOADER.TYPE_TEST VALUES (
  'aaaaaaaaaaaaaaaaaaaa',
  'bbbbbbbbbbbbbbbbbbbb',
  'cccccccccccccccccccc',
  'dddddddddddddddddddd',
  123456789012345678901234567890123456, --C5
  123456789012345678901234567890123456, -- c_number36
  12345678901234567890123456789012345678, --C6
  12345.12345, -- C7
  1234.1241723, -- C_BINFLOAT
  1234987.120871234, -- C_BINDOUBLE
  TO_DATE('2016-08-19', 'YYYY-MM-DD'), -- c10
  TO_TIMESTAMP('2013-03-11 17:30:15.123', 'YYYY-MM-DD HH24:MI:SS.FF'), -- c11
  TO_TIMESTAMP('2013-03-11 17:30:15.123456', 'YYYY-MM-DD HH24:MI:SS.FF'), -- c12
  TO_TIMESTAMP('2013-03-11 17:30:15.123456789', 'YYYY-MM-DD HH24:MI:SS.FF'), -- c13
  TO_TIMESTAMP_TZ('2016-08-19 11:28:05 -08:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), -- c14
  TO_TIMESTAMP_TZ('2018-04-30 10:00:05 -08:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), -- c15
  '54-2', -- c16
  '1 11:12:10.123', -- c17
  '0102030405060708090a0b0c0d0e0f', -- c18
  '0987asdlfkjq2222qawsf;lkja09ed8q2w;43lkrjasdf09uqaw43lkjra0-98sf[iqjw4,mfas[dpiuj[qa09w44', -- c19
  '0987asdlfkjq2222qawsf;lkja09ed8q2w;43lkrjasdf09uqaw43lkjra0-98sf[iqjw4,mfas[dpiuj[qa09w44', -- c20
  12345.01982348239, -- c_float
  12345678.01234567901234567890123456789, -- c_float126
  'test long 123' -- long
);

INSERT INTO LOADER.TYPE_TEST (c3, c5, c7, c_binfloat, c17) VALUES (
  -- C1
  -- C2
  'cccccccccccccccccccc', -- C3
  -- C4
  1234567890.123456789, -- C5
  -- c_number36
  -- C6
  12355.12345, -- C7
  123.12345687987654321, -- c_binfloat
  -- c_bindouble
  -- C10
  -- C11
  -- C12
  -- C13
  -- C14
  -- C15
  -- C16
  '2 02:03:04.123456' -- C17
  -- C18
  -- C19
  -- C20
  -- -- C21
  -- -- C22
  -- c_float
  -- c_float126
  -- c_long
);
