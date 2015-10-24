-- Basic RLE test
create table rle_basic (i int encoding (compresstype=rle_type))
       with (appendonly=true, orientation=column);

insert into rle_basic select 1 from generate_series(1,10) i;
select * from rle_basic;
drop table rle_basic;

create table rle_manycoltypes (c1 int, c2 int, c3 char(10), c4 varchar, c5 timestamp, c6 numeric(7,7)) 
  with (appendonly=true, orientation=column, 
       compresstype=rle_type, 
       compresslevel=1)                                                             distributed by (c1);
insert into rle_manycoltypes select  1, 1, 'abcdefghij', 'x', '2011-09-16 05:33:45.37103-07', 0.1 FROM generate_series(1,1000);

select * from rle_manycoltypes;
drop table rle_manycoltypes;

create table rle_runtest (c1 INT, c2 CHAR(30)) WITH (appendonly=true, orientation=column, compresstype=rle_type, compresslevel=1) DISTRIBUTED BY (C1);
insert into rle_runtest select 1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' FROM generate_series(1,10000);
select * from rle_runtest;
drop table rle_runtest;

DROP TABLE if exists CO_1_create_table_storage_directive_RLE_TYPE_8192_1;

--
-- Create table
--
CREATE TABLE CO_1_create_table_storage_directive_RLE_TYPE_8192_1 (id SERIAL,
	 a1 int ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a2 char(5) ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a3 numeric ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a4 boolean DEFAULT false  ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a5 char DEFAULT 'd' ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a6 text ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a7 timestamp ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a8 character varying(555) ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a9 bigint ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a10 date ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a11 varchar(400) ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a12 text ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a13 decimal ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a14 real ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a15 bigint ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a16 int4  ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a17 bytea ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a18 timestamp with time zone ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a19 timetz ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a20 path ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a21 box ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a22 macaddr ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a23 interval ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a24 character varying(800) ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a25 lseg ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a26 point ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a27 double precision ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a28 circle ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a29 int4 ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a30 numeric(8) ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a31 polygon ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a32 date ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a33 real ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a34 money ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a35 cidr ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a36 inet ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a37 time ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a38 text ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a39 bit ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a40 bit varying(5) ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a41 smallint ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192),
a42 int ENCODING (compresstype=RLE_TYPE,compresslevel=1,blocksize=8192)) WITH (appendonly=true, orientation=column) distributed randomly;

-- 
-- Create Indexes
--
CREATE INDEX CO_1_create_table_storage_directive_RLE_TYPE_8192_1_idx_bitmap ON CO_1_create_table_storage_directive_RLE_TYPE_8192_1 USING bitmap (a1);

CREATE INDEX CO_1_create_table_storage_directive_RLE_TYPE_8192_1_idx_btree ON CO_1_create_table_storage_directive_RLE_TYPE_8192_1(a9);

--
-- Insert data to the table
--
 INSERT INTO CO_1_create_table_storage_directive_RLE_TYPE_8192_1(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42) values(generate_series(1,10),'5char',2011,'t','a','This is news of today: Deadlock between Republicans and Democrats over how best to reduce the U.S. deficit, and over what period, has blocked an agreement to allow the raising of the $14.3 trillion debt ceiling','2001-12-24 02:26:11','U.S. House of Representatives Speaker John Boehner, the top Republican in Congress who has put forward a deficit reduction plan to be voted on later on Thursday said he had no control over whether his bill would avert a credit downgrade.',generate_series(2450,2460),'2011-10-11','The Republican-controlled House is tentatively scheduled to vote on Boehner proposal this afternoon at around 6 p.m. EDT (2200 GMT). The main Republican vote counter in the House, Kevin McCarthy, would not say if there were enough votes to pass the bill.','WASHINGTON:House Speaker John Boehner says his plan mixing spending cuts in exchange for raising the nations $14.3 trillion debt limit is not perfect but is as large a step that a divided government can take that is doable and signable by President Barack Obama.The Ohio Republican says the measure is an honest and sincere attempt at compromise and was negotiated with Democrats last weekend and that passing it would end the ongoing debt crisis. The plan blends $900 billion-plus in spending cuts with a companion increase in the nations borrowing cap.','1234.56',323453,generate_series(3452,3462),7845,'0011','2005-07-16 01:51:15+1359','2001-12-13 01:51:15','((1,2),(0,3),(2,1))','((2,3)(4,5))','08:00:2b:01:02:03','1-2','Republicans had been working throughout the day Thursday to lock down support for their plan to raise the nations debt ceiling, even as Senate Democrats vowed to swiftly kill it if passed.','((2,3)(4,5))','(6,7)',11.222,'((4,5),7)',32,3214,'(1,0,2,3)','2010-02-21',43564,'$1,000.00','192.168.1','126.1.3.4','12:30:45','Johnson & Johnsons McNeil Consumer Healthcare announced the voluntary dosage reduction today. Labels will carry new dosing instructions this fall.The company says it will cut the maximum dosage of Regular Strength Tylenol and other acetaminophen-containing products in 2012.Acetaminophen is safe when used as directed, says Edwin Kuffner, MD, McNeil vice president of over-the-counter medical affairs. But, when too much is taken, it can cause liver damage.The action is intended to cut the risk of such accidental overdoses, the company says in a news release.','1','0',12,23);

select * from CO_1_create_table_storage_directive_RLE_TYPE_8192_1;
DROP TABLE CO_1_create_table_storage_directive_RLE_TYPE_8192_1;


