create table dbo.all_datatypes(
        a text,
        b uniqueidentifier,
        c date,
        d time,
        e datetime2,
        f datetimeoffset,
        g tinyint,
        h smallint,
        i int,
        j smalldatetime,
        k real,
        l money,
        m datetime,
        n float,
        o sql_variant,
        p ntext,
        q bit,
        r decimal,
        s numeric,
        t smallmoney,
        u bigint,
        v hierarchyid,
        w geometry,
        x geography,
        y varbinary,
        z varchar,
        a1 binary,
        b1 char,
        b2 timestamp,
        b3 nvarchar,
        b4 nchar,
        b5 xml,
        b6 sysname
);

INSERT INTO all_datatypes (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, a1, b1, b2, b3, b4, b5, b6) 

VALUES 

('text',  -- a text
newid(),  -- b uniqueidentifier
getdate(), -- c date
getdate(), -- d time
getdate(), --  e datetime2
getdate(), -- f datetimeoffset
1, -- g tinyint
2, --  h smallint
3, --i int
getdate(), --  j smalldatetime
2.34536, --  k real
3.62354, -- l money
getdate(),  -- m datetime,
5.7128345, --  n float,
'sql_variant', -- o sql_variant,
'ntext', -- p ntext,
1,  --  q bit,
10.5, --  r decimal,
123123, -- s numeric,
1.1234, -- t smallmoney,
123123, -- u bigint,
2, -- v hierarchyid,
geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0), --  w geometry,
geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656 )', 4326),  -- x geography,
CAST( 123456 AS BINARY(4) ), --y varbinary,
'varchar', -- z varchar,
CAST( 123456 AS BINARY(4) ),  --a1 binary,
'char',   -- b1 char,
getdate(), -- b2 timestamp
'nvarchar', --b3 nvarchar,
'nchar', -- b4 nchar,
'<?xml version="1.0" encoding="utf-8"?>', --b5 xml,
'sysname' -- b6 sysname
);
