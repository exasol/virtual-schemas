drop table public.all_datatypes;

create table public.all_datatypes 
(
	mySmallint SMALLINT,	--INT2	Signed two-byte integer
	myInteger INTEGER,	--INT, INT4	Signed four-byte integer
	myBigint BIGINT,	--INT8	Signed eight-byte integer
	myDecimal DECIMAL,	--NUMERIC	Exact numeric of selectable precision
	myDecimal2 DECIMAL(36,5),	--NUMERIC	Exact numeric of selectable precision
	myDecimal3 DECIMAL(38,10),
	myReal REAL	, --FLOAT4	Single precision floating-point number
	myDouble DOUBLE PRECISION,	--FLOAT8, FLOAT	Double precision floating-point number
	myBoolean BOOLEAN,	--BOOL	Logical Boolean (true/false)
	myChar CHAR(255),	--CHARACTER, NCHAR, BPCHAR	Fixed-length character string
	myVarchar VARCHAR(255),	--CHARACTER VARYING, NVARCHAR, TEXT	Variable-length character string with a user-defined limit
	myDate DATE,	--Calendar date (year, month, day)
	myTimestamp TIMESTAMP,	--TIMESTAMP WITHOUT TIME ZONE	Date and time (without time zone)
	myTimestampTz TIMESTAMPTZ	--TIMESTAMP WITH TIME ZONE	Date and time (with time zone)
);


insert into public.all_datatypes 
VALUES
(
1,
2,
3,
412345,
1234124.12345,
1728341234234234.1234234,
243234.143,
12341234234.12344,
true,
'hallo welt',
'hallo welt 2',
CURRENT_DATE,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP
);


select *
FROM
public.all_datatypes ;