-- !!!! Attention SAMPLE Database will be dropped in the process
-- create SAMPLE Database with sample data and tables; drop if exists
-- before execution  db2 command window on windows or in any shell on unix set these environment variables:
-- DB2CODEPAGE=1208
-- in windows execute: chcp 65001
-- then execute 
-- db2 -svtf db2-testdata.sql

--force create database sample
!db2sampl -force -verbose;
connect to sample;

--create additional table for bit data, large timestamp and unicode tests
update command options using s off;
drop table ADDITIONAL_DATATYPES ;
update command options using s on;
create table ADDITIONAL_DATATYPES 
(
	BIDATAVARCHAR VARCHAR(1024) FOR BIT DATA
	,BIDATACHAR CHAR(100) FOR BIT DATA
	,UNICODECOL VARCHAR(1024) 
	,DETAIL_TIMESTAMP TIMESTAMP(12)
	,UHRZEIT TIME
);
INSERT INTO ADDITIONAL_DATATYPES  VALUES ('0001','AABB','CHAR 茶', '2020-01-01-00.00.00.123456789123','12.05.11');




-- create alias for having the sample schema for the table -> normally sample tables are created in the schema db2userid

 --time, unicode, detail timestamp, mixed case table and bit data columns test
create alias db2test."Additional_Datatypes" for ADDITIONAL_DATATYPES;

--clob test
create alias db2test.emp_resume for emp_resume; 

--xml and decimal test
create alias db2test.product for product; 


commit;
connect reset;
