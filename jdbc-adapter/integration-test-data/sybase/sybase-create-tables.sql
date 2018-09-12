USE testdb go
sp_adduser 'tester' go
SETUSER 'tester' go
CREATE SCHEMA AUTHORIZATION tester 
	CREATE TABLE ittable (
	    a varchar(100) null,
	    b decimal null
	)
	CREATE TABLE timetypes (
	   	c_smalldatetime smalldatetime,
	   	c_datetime datetime,
	   	c_date date,
	   	c_time time,
	   	c_bigdatetime bigdatetime, -- error data truncation
	   	c_bigtime bigtime
	)
	-- https://help.sap.com/viewer/b65d6a040c4a4709afd93068071b2a76/16.0.3.5/en-US/aa357b76bc2b1014ba159ac9d0074e1d.html
	-- FLOAT(p) is alias for either DOUBLE PRECISION or REAL.
	-- If p < 16, FLOAT is stored as REAL, if p >= 16, FLOAT is stored as DOUBLE PRECISION.
	CREATE TABLE approxtypes (
	    c_double double precision,
	    c_real real,
	)
	-- https://help.sap.com/viewer/b65d6a040c4a4709afd93068071b2a76/16.0.3.5/en-US/aa357b76bc2b1014ba159ac9d0074e1d.html
	CREATE TABLE decimaltypes (
	    c_numeric_36_0 numeric(36, 0),
	   	c_numeric_38_0 numeric(38, 0),
	   	c_decimal_20_10 decimal(20, 10),
	   	c_decimal_37_10 decimal(37, 10)
	)
	CREATE TABLE integertypes (
	  c_bigint bigint,
	  c_int int,
	  c_smallint smallint,
	  c_ubigint unsigned bigint,
	  c_uint unsigned int,
	  c_usmallint unsigned smallint
	) 
	CREATE TABLE moneytypes (
	    c_smallmoney smallmoney,
		c_money money,
	)
	-- https://help.sap.com/viewer/b65d6a040c4a4709afd93068071b2a76/16.0.3.5/en-US/aa362f6cbc2b1014b1ed808e2a54e693.html
	CREATE TABLE chartypes (
	    c_char_10 char(10),
	    c_char_toobig char(2001),
	    c_varchar varchar(10), -- maximum size in Sybase is 16384 -> smaller than Exasol's limit
	    c_unichar_10 unichar(10),
	    c_univarchar univarchar(10), -- maximum size is 8192
	    c_nchar nchar(10), -- maximum size in Sybase is 16384. NOT right-padded with spaces.
	    c_nvarchar nvarchar(10), -- maximum size in Sybase is 16384
	)
	-- NOT right-padded with spaces.
	-- While the theoretical maximum is 8192 unichars, effectively only 8148 are possible because
	-- Sybase otherwise complains that the maximum row width is exceeded. 
	CREATE TABLE fatunichartypes (
		c_unichar_toobig unichar(8148)
	)
	CREATE TABLE texttypes (
		c_text text,
		c_unitext unitext
	)
	CREATE TABLE misctypes (
		c_binary binary(10), -- n <= 255
		c_varbinary varbinary(10),
		c_image image,
		c_bit bit NOT NULL
	)
go