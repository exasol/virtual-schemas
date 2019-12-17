

CREATE  TABLE retail.numeric_data_types(
mybyteint BYTEINT,
mysmallint SMALLINT,
myinteger INTEGER,
myBIGINT BIGINT,
myDecimal  DECIMAL(8,2),
myFloat FLOAT,
myReal REAL,
myDouble DOUBLE PRECISION,
n1 NUMBER(*,3),
n2 NUMBER,
n3 NUMBER(*),
n4 NUMBER(5,1),
n5 NUMBER(3)
);


insert into retail.numeric_data_types values (
1,
2,
3,
4,
7.22,
1214325.1234,
1345134513.5541,
1234513245.783,
113.321,
3143,
23452345,
1234.1,
132
);

)


DROP TABLE retail.DateTime_and_Interval_Data_Types;

CREATE TABLE retail.DateTime_and_Interval_Data_Types (
	myDate DATE,
	myTime TIME,
	myTimestamp TIMESTAMP,
	myTimeWithTimezone TIME WITH TIME ZONE,
	myTimestampWithTimezone TIMESTAMP WITH TIME ZONE,
	myIntervalYear INTERVAL YEAR,
	myIntervalYearToMonth INTERVAL YEAR TO MONTH,
	myIntervalDayToSecond INTERVAL DAY (4) TO SECOND (4),
	myIntervalMinuteToSecond INTERVAL MINUTE (1) TO SECOND (2)
);


insert into retail.DateTime_and_Interval_Data_Types values 
(
CURRENT_DATE,
CURRENT_TIME,
CURRENT_TIMESTAMP,
CURRENT_TIME,
CURRENT_TIMESTAMP,
INTERVAL -'2' YEAR,
INTERVAL '10-10' YEAR TO MONTH,
INTERVAL '30 12:30:30.5' DAY TO SECOND, 
INTERVAL '6:15.24' MINUTE TO SECOND
);


drop table retail.Period_Data_Types;

CREATE TABLE retail.Period_Data_Types
(
      employee_id         		INTEGER,
      employee_name       		CHARACTER(15),
      myPeriodDate   			PERIOD(DATE) DEFAULT PERIOD '(2005-02-03, 2006-02-03)',
      myPeriodTime     			PERIOD(TIME),
      myPeriodTimeWithTimeZone  PERIOD(TIME WITH TIME ZONE),
      myPeriodTimestamp   		PERIOD(TIMESTAMP(3)),
      myPeriodTimestampTimezone PERIOD(TIMESTAMP(3) WITH TIME ZONE) 
);


insert into retail.Period_Data_Types
values (
1,
'hans',
 PERIOD '(2005-02-03, 2006-02-04)' ,
 PERIOD '(10:00:00.123456, 11:00:00.123456)',
 PERIOD '(10:37:58.123456+08:00, 11:37:58.123456+08:00)',
 PERIOD '(2005-02-03 10:00:00.123,2005-02-03 11:00:00.123)',
 PERIOD '(2005-02-03 10:37:58.123+08:00,2005-02-03 11:37:58.123+08:00)'
);

select * from retail.Period_Data_Types;

 CREATE TABLE retail.T_CLOB
 (
 id INTEGER,
 myClob CLOB(2K) CHARACTER SET UNICODE
 );


 insert into retail.T_CLOB 
 values (
 1,
 'my cloooooovbbbbbbbbbbbbbbbbbbbbbbb'
 );
 
   
drop table retail.bvalues;

CREATE TABLE retail.bvalues (
	IDVal INTEGER, 
	myByte BYTE(2),
	myVarByte VARBYTE(1200),
	myBlob BLOB(128K)
);

INSERT retail.bvalues (
112193, 
'7879'XB,
'787989123'XB,
'787989123'XB
) ;


--Consider the following statement that creates a distinct UDT named euro:
   CREATE TYPE euro
   AS DECIMAL(8,2)
   FINAL;
   
   
--The following statement creates a table that defines a euro column named sales:
   CREATE TABLE retail.european_sales
     (region INTEGER
     ,sales euro);

     insert into retail.european_sales VALUES
     (
		1, 12.4     
     
     );

     
 select *
 FROM
 retail.european_sales;
     
SELECT IDVal FROM bvalues WHERE CodeVal = '7879'XB ;


select * from retail.bvalues;


CREATE TABLE retail.bldg_location
(
    bldg_id    INT NOT NULL,
    geo        ST_GEOMETRY
);

INSERT retail.bldg_location(bldg_id, geo) VALUES(2,'Point(-117.093861 33.020725)');



CREATE TABLE retail.T_XMLDOCS(
 
    ID     VARCHAR(30),
 
    XMLDOC XML
 
);


INSERT INTO retail.T_XMLDOCS VALUES('a', CREATEXML('<Greeting>Hello World</Greeting>') );


CREATE TABLE retail.json_table(id INTEGER, json_j1 JSON);


INSERT INTO  retail.json_table(1, '{"a":1}');



SELECT  CAST("geo"  as VARCHAR(64000) ) a
--, 
--XMLSERIALIZE(DOCUMENT XMLDOC as VARCHAR(64000) INCLUDING XMLDECLARATION) b, 
--CAST("json_doc"  as VARCHAR(64000) ) c
FROM "retail"."all_types";