DROP TABLE testdb.tester.ittable go

CREATE TABLE testdb.tester.ittable (
	a varchar(100),
	b decimal
) go

INSERT INTO testdb.tester.ittable (a, b) VALUES('e', 2) go
INSERT INTO testdb.tester.ittable (a, b) VALUES('b', 3) go
INSERT INTO testdb.tester.ittable (a, b) VALUES(NULL, -1) go
INSERT INTO testdb.tester.ittable (a, b) VALUES('a', NULL) go
INSERT INTO testdb.tester.ittable (a, b) VALUES('z', 0) go
INSERT INTO testdb.tester.ittable (a, b) VALUES('z', 0) go
