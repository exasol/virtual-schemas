drop table  public.all_datatypes ;

create table public.all_datatypes (
mybigint bigint,	--signed eight-byte integer
--mybigserial bigserial,	--autoincrementing eight-byte integer
mybit bit, 	--fixed-length bit string
mybitVar bit varying  (5) ,	--variable-length bit string
myBoolean boolean,		--logical Boolean (true/false)
myBox box,	 	--rectangular box on a plane
myBytea bytea,	 	--binary data ("byte array")
myCharacter character (1000),	--fixed-length character string
myCharacterVar character varying  (1000), --variable-length character string
myCidr cidr	, 	--IPv4 or IPv6 network address
myCircle circle	 ,	--circle on a plane
myDate date	, 	--calendar date (year, month, day)
myDouble double precision, --floating-point number (8 bytes)
myinet inet	, 	--IPv4 or IPv6 host address
myInteger integer,	--signed four-byte integer
myInterval interval,	--time span
myJson json, 	--textual JSON data
myJsonB jsonb,	 	--binary JSON data, decomposed
myLine line,	 	--infinite line on a plane
myLseg lseg,	 	--line segment on a plane
myMacAddr macaddr,	 	--MAC (Media Access Control) address
myMoney money,	 	--currency amount
myNumeric numeric(36, 10), --exact numeric of selectable precision
myPath path,	 	--geometric path on a plane
mypoint point,	 	--geometric point on a plane
mypolygon polygon,	 	--closed geometric path on a plane
myreal real	,     --floating-point number (4 bytes)
mysmallint smallint,	--signed two-byte integer
mysmallserial smallserial,		--autoincrementing two-byte integer
--myserial serial	,		--autoincrementing four-byte integer
mytext text,	 	--variable-length character string
mytime time, --time of day (no time zone)
mytimeWithTimeZone time with time zone,	--timetz	--time of day, including time zone
mytimestamp timestamp, 	 	--date and time (no time zone)
mytimestampWithTimeZone timestamp with time zone,	--timestamptz	date and time, including time zone
mytsquery tsquery,	 	--text search query
mytsvector tsvector	, 	--text search document
myuuid uuid,	 	--universally unique identifier
myxml xml	 	--XML data
);


INSERT INTO public.all_datatypes
(mybigint, /*mybigserial,*/ mybit, mybitvar, myboolean, mybox, mybytea, mycharacter, mycharactervar, mycidr, mycircle, mydate, mydouble, myinet, myinteger, myinterval, myjson, myjsonb, myline, mylseg, mymacaddr, mymoney, mynumeric, mypath,  mypoint, mypolygon, myreal, mysmallint, mysmallserial,  mytext, mytime, mytimewithtimezone, mytimestamp, mytimestampwithtimezone, mytsquery, mytsvector,  myuuid, myxml)
VALUES(
10000000000, 
--nextval('all_datatypes_mybigserial_seq'::regclass), 
B'1', 
B'0', 
false, 
'( ( 1 , 8 ) , ( 4 , 16 ) )', 
E'\\000'::bytea, 
'hajksdf', 
'hjkdhjgfh', 
'192.168.100.128/25'::cidr, 
'( ( 1 , 5 ) , 3 )'::circle, 
current_date, 
192189234.1723854, 
'192.168.100.128'::inet, 
7189234, 
INTERVAL '1' YEAR, 
'{"bar": "baz", "balance": 7.77, "active": false}'::json, 
'{"bar": "baz", "balance": 7.77, "active": false}'::jsonb, 
'{ 1, 2, 3 }'::line, 
'[ ( 1 , 2 ) , ( 3 , 4 ) ]'::lseg, 
'08:00:2b:01:02:03'::macaddr, 
100.01, 
24.23, 
'[ ( 1 , 2 ) , ( 3 , 4 ) ]'::path,  
'( 1 , 3 )'::point, 
'( ( 1 , 2 ) , (2,4),(3,7) )'::polygon, 
10.12, 
100, 
nextval('all_datatypes_mysmallserial_seq'::regclass), 
--nextval('all_datatypes_myserial_seq'::regclass), 
'skldfjgkl jsdklfgjklsdjfg jsklfdjg', 
current_time, 
current_time, 
current_timestamp, 
current_timestamp, 
'fat & rat'::tsquery, 
to_tsvector('english', 'The Fat Rats'), 
'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 
XMLPARSE (DOCUMENT '<?xml version="1.0"?><book><title>Manual</title><chapter>...</chapter></book>'));
