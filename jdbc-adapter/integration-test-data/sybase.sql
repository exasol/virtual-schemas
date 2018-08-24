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
