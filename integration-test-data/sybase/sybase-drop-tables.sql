USE testdb go

DROP TABLE ittable go
DROP TABLE timetypes go
DROP TABLE integertypes go
DROP TABLE decimaltypes go
DROP TABLE approxtypes go
DROP TABLE moneytypes go
DROP TABLE chartypes go
DROP TABLE fatunichartypes go
DROP TABLE texttypes go
DROP TABLE misctypes go
SELECT * FROM sysobjects WHERE type = 'U' go