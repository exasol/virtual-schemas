USE testdb go

TRUNCATE TABLE tester.ittable go
TRUNCATE TABLE tester.timetypes go
TRUNCATE TABLE tester.integertypes go
TRUNCATE TABLE tester.decimaltypes go
TRUNCATE TABLE tester.approxtypes go
TRUNCATE TABLE tester.moneytypes go
TRUNCATE TABLE tester.chartypes go
TRUNCATE TABLE tester.fatunichartypes go
TRUNCATE TABLE tester.texttypes go
TRUNCATE TABLE tester.misctypes go

INSERT INTO tester.ittable (a, b) VALUES('e', 2) go
INSERT INTO tester.ittable (a, b) VALUES('b', 3) go
INSERT INTO tester.ittable (a, b) VALUES(NULL, -1) go
INSERT INTO tester.ittable (a, b) VALUES('a', NULL) go
INSERT INTO tester.ittable (a, b) VALUES('z', 0) go
INSERT INTO tester.ittable (a, b) VALUES('z', 0) go
INSERT INTO tester.timetypes VALUES(
	'1.1.1900 01:02',
	'1.1.1753 01:02:03.100',
	'12/3/2032',
	'11:22:33.456',
	'6.4.1553 11:11:11.111111',
	'11:11:11.111111'
)
INSERT INTO tester.approxtypes VALUES(
    2.2250738585072014e-308,
    1.175494351e-38
) go
INSERT INTO tester.approxtypes VALUES(
    1.797693134862315708e+308,
    3.402823466e+38
) go
INSERT INTO tester.decimaltypes VALUES(
    12345678901234567890123456,
    1234567890123456789012345678,
    1234567890.0123456789,
    12345678901234567.0123456789
) go
INSERT INTO tester.decimaltypes VALUES(
    -12345678901234567890123456,
    -1234567890123456789012345678,
    -1234567890.0123456789,
    -12345678901234567.0123456789
) go
INSERT INTO tester.integertypes VALUES(
    -9223372036854775808,
    -2147483648,
    -32768,
    0,
    0,
    0
) go
INSERT INTO tester.integertypes VALUES(
    9223372036854775807,
    2147483647,
    32767,
    18446744073709551615,
    4294967295,
    65535
) go
INSERT INTO tester.moneytypes VALUES(
    214748.3647,
    922337203685477.5807
) go
INSERT INTO tester.moneytypes VALUES(
    -214748.3648,
    -922337203685477.5808
) go
INSERT INTO tester.chartypes VALUES(
    'c10',
    'c2001',
    'vc10',
    'uc10',
    'uvc10',
    'nc10',
    'nvc10'
) go
INSERT INTO tester.fatunichartypes VALUES(
    'xyz'
) go
INSERT INTO tester.texttypes VALUES(
    'Text. A wall of text.',
    'Text. A wall of Unicode text.'
) go
INSERT INTO tester.misctypes VALUES(
    0xdeadbeef,
    0xdeadbeef,
    0xdeadbeef,
    0
) go
COMMIT go