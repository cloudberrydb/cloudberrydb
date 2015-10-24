\c regression

--start_ignore
drop table if exists T3;
drop table if exists T11;
drop table if exists T20;
drop table if exists T29;
drop table if exists T33;
drop table if exists T34;
drop table if exists T41;
drop table if exists T43;
drop table if exists T44;
drop table if exists T50;
drop table if exists T52;
drop table if exists T53;
drop table if exists T54;
drop table if exists T60;
drop table if exists T70;
drop table if exists T76;
drop table if exists T83;
drop table if exists T92;
drop table if exists T97;
drop table if exists T98;
drop table if exists T99;
--end_ignore

create table T3(        C32 int,        C33 int,        C34 int,        C35 int,        C36 int,        C37 int,        C38 int,
        C39 int,        C40 int,        C41 int,        C42 int,        C43 int,        C44 int,        C45 int,        C46 int,
C47 int,        C48 int,        C49 int,        C50 int,        C51 int,        C52 int) WITH (compresstype=zlib,blocksize=8192,appendonly=true,compresslevel=2);

INSERT INTO T3 VALUES ( 1, 1, 4, 1, 3, 2, 2, 1, 2, 4, 1, 1, 3, 5, 5, 3, 2, 2, 5, 3, 1 );

INSERT INTO T3 VALUES ( 5, 1, 1, 1, 1, 5, 1, 2, 3, 3, 2, 4, 3, 3, 3, 2, 1, 3, 2, 2, 5 );

INSERT INTO T3 VALUES ( 4, 5, 5, 2, 5, 3, 5, 4, 1, 5, 3, 4, 1, 3, 5, 2, 5, 2, 5, 3, 4 );

INSERT INTO T3 VALUES ( 5, 2, 5, 4, 4, 2, 2, 3, 1, 1, 1, 2, 4, 5, 3, 2, 2, 4, 3, 2, 3 );

INSERT INTO T3 VALUES ( 4, 1, 5, 5, 4, 1, 4, 1, 1, 2, 1, 2, 5, 5, 1, 1, 4, 1, 3, 3, 1 );

INSERT INTO T3 VALUES ( 5, 4, 2, 4, 4, 3, 3, 2, 2, 1, 3, 2, 5, 3, 2, 4, 1, 2, 5, 2, 4 );

INSERT INTO T3 VALUES ( 1, 2, 1, 3, 2, 1, 5, 5, 1, 5, 4, 1, 5, 4, 3, 5, 3, 3, 5, 2, 1 );

INSERT INTO T3 VALUES ( 2, 2, 2, 2, 2, 5, 3, 2, 1, 2, 3, 5, 3, 1, 2, 1, 4, 1, 3, 1, 5 );

INSERT INTO T3 VALUES ( 2, 5, 1, 2, 5, 5, 1, 3, 1, 4, 3, 2, 3, 4, 4, 2, 3, 3, 3, 4, 1 );

INSERT INTO T3 VALUES ( 1, 2, 4, 3, 1, 2, 2, 1, 1, 5, 2, 2, 2, 1, 4, 3, 1, 3, 3, 2, 5 );



create table T11(	C152 int,	C153 int,	C154 int,	C155 int,	C156 int) WITH (compresstype=zlib,compresslevel=7,appendonly=true,blocksize=770048,checksum=false);

INSERT INTO T11 VALUES ( 4, 2, 5, 4, 2 );

INSERT INTO T11 VALUES ( 2, 5, 1, 4, 2 );

INSERT INTO T11 VALUES ( 2, 5, 5, 2, 2 );

INSERT INTO T11 VALUES ( 3, 3, 1, 2, 5 );

INSERT INTO T11 VALUES ( 5, 4, 5, 4, 2 );

INSERT INTO T11 VALUES ( 5, 1, 1, 1, 4 );

INSERT INTO T11 VALUES ( 2, 4, 2, 5, 3 );

INSERT INTO T11 VALUES ( 2, 2, 1, 2, 2 );

INSERT INTO T11 VALUES ( 1, 4, 4, 5, 2 );

INSERT INTO T11 VALUES ( 3, 5, 3, 4, 4 );



create table T20(	C233 int,	C234 int,	C235 int,	C236 int,	C237 int,	C238 int,	C239 int,	C240 int,	C241 int,	C242 int,	C243 int,	C244 int,	C245 int,	C246 int) WITH (compresstype=zlib,blocksize=229376,appendonly=true,compresslevel=1,checksum=true);

INSERT INTO T20 VALUES ( 2, 3, 3, 4, 3, 4, 5, 5, 5, 4, 1, 4, 5, 3 );

INSERT INTO T20 VALUES ( 3, 5, 4, 2, 3, 1, 1, 5, 1, 5, 3, 4, 4, 3 );

INSERT INTO T20 VALUES ( 4, 3, 1, 4, 2, 3, 1, 4, 5, 2, 3, 2, 3, 5 );

INSERT INTO T20 VALUES ( 2, 5, 5, 5, 1, 1, 4, 1, 4, 1, 3, 3, 5, 1 );

INSERT INTO T20 VALUES ( 4, 5, 4, 1, 2, 4, 1, 2, 2, 4, 5, 4, 2, 1 );

INSERT INTO T20 VALUES ( 4, 3, 1, 1, 4, 1, 4, 4, 5, 5, 5, 4, 2, 5 );

INSERT INTO T20 VALUES ( 2, 4, 4, 1, 2, 2, 3, 4, 5, 4, 4, 1, 5, 5 );

INSERT INTO T20 VALUES ( 5, 4, 5, 3, 1, 1, 2, 5, 1, 1, 5, 2, 1, 1 );

INSERT INTO T20 VALUES ( 2, 4, 5, 4, 4, 4, 1, 5, 4, 4, 3, 5, 3, 4 );

INSERT INTO T20 VALUES ( 1, 3, 1, 4, 4, 5, 5, 4, 1, 3, 4, 4, 2, 5 );


create table T29(	C334 int,	C335 int,	C336 int,	C337 int,	C338 int,	C339 int,	C340 int) WITH (compresstype=zlib,checksum=true,appendonly=true,blocksize=1155072,compresslevel=1);

INSERT INTO T29 VALUES ( 5, 1, 1, 3, 1, 2, 4 );

INSERT INTO T29 VALUES ( 5, 5, 4, 3, 3, 3, 2 );

INSERT INTO T29 VALUES ( 5, 1, 3, 2, 1, 5, 4 );

INSERT INTO T29 VALUES ( 4, 1, 4, 4, 3, 2, 3 );

INSERT INTO T29 VALUES ( 3, 4, 1, 5, 3, 3, 3 );

INSERT INTO T29 VALUES ( 2, 5, 4, 3, 5, 3, 4 );

INSERT INTO T29 VALUES ( 4, 5, 1, 1, 1, 3, 2 );

INSERT INTO T29 VALUES ( 4, 5, 2, 1, 5, 3, 4 );

INSERT INTO T29 VALUES ( 4, 5, 2, 1, 5, 3, 1 );

INSERT INTO T29 VALUES ( 1, 2, 2, 2, 4, 3, 3 );



create table T33(	C383 int,	C384 int,	C385 int,	C386 int,	C387 int,	C388 int,	C389 int,	C390 int,	C391 int,	C392 int,	C393 int,	C394 int,	C395 int,	C396 int,	C397 int,	C398 int,	C399 int,	C400 int,	C401 int) WITH (compresstype=zlib,blocksize=548864,appendonly=true,compresslevel=2,checksum=true);

INSERT INTO T33 VALUES ( 5, 1, 3, 1, 5, 2, 4, 2, 2, 2, 4, 5, 3, 4, 1, 4, 2, 1, 3 );

INSERT INTO T33 VALUES ( 2, 2, 1, 5, 2, 4, 3, 4, 5, 5, 2, 4, 2, 4, 2, 1, 2, 3, 5 );

INSERT INTO T33 VALUES ( 1, 3, 2, 3, 5, 3, 2, 2, 5, 5, 5, 1, 4, 1, 5, 2, 4, 2, 4 );

INSERT INTO T33 VALUES ( 1, 1, 4, 3, 1, 5, 1, 2, 1, 1, 3, 2, 4, 3, 5, 1, 1, 2, 2 );

INSERT INTO T33 VALUES ( 1, 4, 1, 2, 5, 2, 5, 1, 4, 2, 3, 5, 3, 2, 3, 3, 2, 2, 4 );

INSERT INTO T33 VALUES ( 4, 2, 5, 3, 4, 4, 3, 2, 1, 2, 1, 3, 3, 3, 5, 4, 2, 1, 4 );

INSERT INTO T33 VALUES ( 2, 3, 1, 5, 2, 2, 3, 4, 2, 5, 2, 3, 4, 2, 4, 4, 5, 5, 3 );

INSERT INTO T33 VALUES ( 3, 1, 2, 2, 2, 5, 3, 3, 3, 5, 3, 2, 2, 4, 3, 5, 3, 4, 1 );

INSERT INTO T33 VALUES ( 2, 5, 5, 3, 1, 2, 4, 3, 3, 4, 1, 4, 3, 2, 5, 3, 2, 1, 1 );

INSERT INTO T33 VALUES ( 1, 2, 5, 1, 2, 2, 4, 5, 2, 1, 2, 2, 3, 5, 3, 5, 5, 1, 2 );


create table T34(	C402 int,	C403 int,	C404 int,	C405 int,	C406 int,	C407 int,	C408 int,	C409 int,	C410 int) WITH (compresstype=zlib,compresslevel=1,appendonly=true,blocksize=1318912);

COPY T34 FROM STDIN delimiter '|';
4|5|2|1|1|3|4|2|4
2|5|4|1|3|3|3|4|3
5|3|4|1|5|4|4|1|5
5|1|3|4|3|1|3|1|1
2|5|2|4|3|1|4|1|4
5|1|4|3|4|2|2|3|3
1|4|4|4|4|3|3|4|5
3|3|2|5|5|5|3|3|2
4|4|3|5|4|1|4|5|4
2|5|1|3|3|4|2|5|4
\.


create table T41(	C451 int,	C452 int,	C453 int,	C454 int,	C455 int,	C456 int,	C457 int,	C458 int,	C459 int,	C460 int,	C461 int,	C462 int,	C463 int,	C464 int,	C465 int,	C466 int,	C467 int,	C468 int,	C469 int,	C470 int,	C471 int,	C472 int) WITH (compresstype=zlib,appendonly=true,blocksize=1859584,compresslevel=9,checksum=false);

COPY T41 FROM STDIN delimiter '|';
4|2|1|3|5|2|5|4|4|5|1|4|3|3|4|3|2|1|2|1|1|2
5|4|3|3|3|4|1|5|5|4|3|2|3|4|2|5|1|1|2|5|5|2
4|4|3|2|4|2|4|2|1|1|4|1|1|1|5|2|5|3|1|2|4|4
4|1|3|3|1|5|4|3|1|4|2|3|4|5|3|4|3|3|2|3|3|2
5|1|3|5|3|5|5|3|5|5|2|3|1|5|3|4|4|5|5|1|4|3
1|5|3|4|5|5|3|2|3|5|2|3|4|3|1|2|5|3|4|4|1|2
3|1|2|1|5|5|1|3|3|5|1|3|5|4|1|4|2|3|5|5|2|1
1|2|2|5|5|2|3|3|2|1|1|5|3|4|5|4|1|5|3|1|3|1
4|3|3|3|3|5|1|5|3|4|5|4|2|1|4|3|5|5|3|3|5|3
3|1|3|2|1|1|4|3|3|5|1|2|2|5|2|3|3|5|5|1|4|5
\.



create table T43(	C487 int,	C488 int,	C489 int,	C490 int,	C491 int,	C492 int,	C493 int,	C494 int,	C495 int,	C496 int,	C497 int,	C498 int,	C499 int,	C500 int,	C501 int) WITH (compresstype=zlib,compresslevel=1,appendonly=true,blocksize=1327104,checksum=true);

COPY T43 FROM STDIN delimiter '|';
5|3|4|1|3|2|1|2|5|2|4|3|2|5|1
4|1|2|5|4|5|3|5|4|3|4|2|5|3|1
5|3|3|1|5|3|2|1|5|5|3|5|1|4|4
3|2|1|4|3|3|5|1|1|3|3|5|4|5|4
2|3|4|4|5|3|3|3|3|4|2|1|5|3|4
3|5|5|3|4|1|4|4|4|5|2|3|1|3|1
5|5|3|4|3|1|2|2|5|2|4|2|1|5|3
2|5|2|3|1|1|2|4|5|4|2|1|3|5|1
2|4|3|3|1|1|3|1|5|4|5|1|2|5|1
3|3|3|4|1|1|2|3|2|5|4|2|5|1|3
\.

create table T44(	C502 int,	C503 int,	C504 int,	C505 int,	C506 int,	C507 int,	C508 int,	C509 int,	C510 int,	C511 int) WITH (compresstype=zlib,compresslevel=1,appendonly=true,blocksize=1703936,checksum=true);

COPY T44 FROM STDIN delimiter '|';
5|5|4|2|4|5|1|3|5|5
5|4|3|1|3|5|1|1|5|2
1|3|4|2|4|1|4|5|1|5
5|2|1|3|1|2|1|2|5|4
2|2|5|5|1|5|1|5|5|4
5|3|4|1|1|1|4|5|1|3
3|4|1|3|3|4|2|4|1|1
3|5|1|4|4|3|4|3|1|2
1|2|2|5|3|5|5|5|4|2
1|2|3|2|5|5|5|2|3|2
\.

create table T50(	C600 int,	C601 int,	C602 int,	C603 int,	C604 int,	C605 int,	C606 int,	C607 int) WITH (compresstype=zlib,checksum=false,appendonly=true,blocksize=393216,compresslevel=8);

COPY T50 FROM STDIN delimiter '|';
2|5|1|3|3|2|3|4
2|4|1|4|4|2|4|2
1|4|3|2|2|3|5|1
4|5|4|2|5|4|4|1
5|2|2|4|5|2|1|2
3|5|4|3|1|4|1|1
5|1|1|5|5|1|5|1
3|2|2|5|2|1|1|5
2|4|2|5|4|3|4|4
4|4|5|1|4|2|3|1
\.


create table T52(	C625 int,	C626 int,	C627 int,	C628 int,	C629 int,	C630 int,	C631 int,	C632 int,	C633 int,	C634 int,	C635 int) WITH (compresstype=zlib,blocksize=2031616,appendonly=true,compresslevel=1);

COPY T52 FROM STDIN delimiter '|';
1|4|2|1|3|4|5|5|3|5|3
5|4|1|4|4|4|2|1|1|5|5
4|3|2|2|3|4|2|3|5|5|4
5|2|4|4|5|3|1|1|3|5|1
4|5|5|4|5|4|4|4|3|2|4
1|3|5|2|1|1|2|3|3|4|5
1|1|4|1|4|2|2|1|4|2|4
4|4|2|5|3|1|5|3|4|4|3
3|4|1|4|2|2|5|1|3|1|5
4|1|5|4|3|5|5|3|5|2|2
\.

create table T53(	C636 int,	C637 int,	C638 int,	C639 int,	C640 int,	C641 int,	C642 int,	C643 int,	C644 int,	C645 int,	C646 int,	C647 int,	C648 int,	C649 int,	C650 int,	C651 int,	C652 int,	C653 int) WITH (compresstype=zlib,compresslevel=1,appendonly=true,blocksize=491520);

COPY T53 FROM STDIN delimiter '|';
4|1|4|5|5|2|5|3|5|4|3|4|5|4|2|5|2|4
4|5|2|3|2|3|4|5|4|4|1|4|3|1|5|5|2|3
2|3|1|2|3|4|5|3|4|3|5|3|1|3|1|3|1|5
1|2|4|1|4|4|1|3|4|3|1|5|1|5|4|1|5|1
4|2|3|1|2|1|4|5|4|2|4|3|5|1|3|5|4|3
4|3|4|5|1|5|2|2|5|1|1|2|3|4|4|1|2|1
2|2|3|2|1|3|1|5|3|1|3|2|1|4|1|4|2|3
3|1|5|2|5|4|1|3|1|4|2|5|2|3|5|2|4|1
4|5|3|3|2|3|5|1|2|1|5|1|4|4|4|1|3|4
4|1|5|2|5|5|4|2|2|1|2|5|3|3|2|1|5|3
\.

create table T54(	C654 int,	C655 int,	C656 int,	C657 int) WITH (compresstype=zlib,checksum=true,appendonly=true,blocksize=1409024,compresslevel=6);

COPY T54 FROM STDIN delimiter '|';
5|1|1|2
4|2|3|4
5|3|3|5
5|3|4|1
1|3|1|5
1|3|2|1
2|2|3|3
1|2|2|4
5|2|1|3
4|5|1|4
\.


create table T60(	C717 int,	C718 int,	C719 int,	C720 int,	C721 int,	C722 int,	C723 int,	C724 int) WITH (compresstype=zlib,compresslevel=1,appendonly=true,blocksize=1433600);

COPY T60 FROM STDIN delimiter '|';
4|4|3|1|2|1|4|4
4|2|1|5|5|1|3|1
5|4|5|2|5|5|2|2
2|2|1|4|1|2|2|3
4|3|2|4|5|3|1|4
4|4|3|5|5|4|4|1
2|2|4|1|5|1|4|4
5|3|1|1|4|5|5|1
3|4|2|1|1|3|2|4
5|2|2|1|4|2|2|1
\.

create table T67(	C809 int,	C810 int,	C811 int,	C812 int,	C813 int,	C814 int,	C815 int,	C816 int,	C817 int,	C818 int,	C819 int,	C820 int) WITH (compresstype=zlib,compresslevel=5,appendonly=true,blocksize=1900544,checksum=true);

INSERT INTO T67 VALUES ( 4, 5, 5, 3, 5, 4, 2, 2, 5, 5, 4, 4 );
INSERT INTO T67 VALUES ( 2, 2, 2, 3, 2, 5, 5, 4, 2, 1, 4, 4 );
INSERT INTO T67 VALUES ( 1, 4, 3, 2, 5, 4, 2, 4, 5, 5, 1, 5 );
COPY T67 FROM STDIN delimiter '|';
3|2|3|2|2|3|3|2|4|2|5|2
2|4|5|3|2|4|5|3|3|1|1|3
\.
INSERT INTO T67 VALUES ( 5, 3, 1, 1, 5, 3, 3, 1, 4, 2, 4, 4 );
INSERT INTO T67 VALUES ( 5, 3, 3, 5, 2, 1, 2, 5, 2, 1, 3, 5 );
COPY T67 FROM STDIN delimiter '|';
1|3|1|4|2|3|2|5|5|5|5|2
\.
INSERT INTO T67 VALUES ( 4, 2, 3, 2, 2, 4, 4, 3, 2, 1, 2, 5 );
COPY T67 FROM STDIN delimiter '|';
4|2|1|3|5|2|1|2|1|5|5|1
\.

create table T70(	C839 int,	C840 int,	C841 int,	C842 int,	C843 int,	C844 int,	C845 int,	C846 int,	C847 int,	C848 int,	C849 int,	C850 int,	C851 int,	C852 int,	C853 int,	C854 int,	C855 int,	C856 int,	C857 int,	C858 int,	C859 int) WITH (compresstype=zlib,checksum=false,appendonly=true,blocksize=1417216);

INSERT INTO T70 VALUES ( 3, 4, 4, 5, 4, 4, 1, 3, 3, 4, 4, 1, 5, 5, 4, 5, 3, 5, 2, 3, 4 );
INSERT INTO T70 VALUES ( 1, 3, 3, 5, 3, 2, 3, 1, 1, 2, 1, 4, 1, 1, 1, 5, 2, 4, 4, 5, 3 );
COPY T70 FROM STDIN delimiter '|';
3|3|5|5|2|4|5|2|4|4|1|1|2|1|5|5|5|3|4|3|1
\.
INSERT INTO T70 VALUES ( 2, 3, 2, 2, 4, 1, 1, 1, 5, 4, 3, 4, 5, 1, 2, 4, 4, 5, 5, 1, 5 );
COPY T70 FROM STDIN delimiter '|';
4|5|2|1|2|4|2|1|1|1|2|4|3|1|5|4|2|2|1|3|4
\.
INSERT INTO T70 VALUES ( 5, 1, 2, 5, 2, 2, 3, 2, 1, 1, 5, 5, 4, 1, 1, 2, 1, 5, 5, 2, 5 );
COPY T70 FROM STDIN delimiter '|';
4|1|5|2|5|4|4|1|4|4|2|1|3|5|2|2|4|2|4|1|1
5|3|1|5|5|5|5|2|3|2|3|5|2|2|4|5|2|4|3|4|2
\.
INSERT INTO T70 VALUES ( 4, 5, 2, 1, 3, 5, 5, 5, 1, 1, 5, 1, 4, 1, 3, 5, 5, 1, 2, 2, 3 );
COPY T70 FROM STDIN delimiter '|';
2|2|2|1|3|3|1|2|5|3|2|3|1|3|2|3|4|5|2|2|3
\.

create table T76(	C938 int,	C939 int,	C940 int,	C941 int,	C942 int,	C943 int,	C944 int,	C945 int,	C946 int) WITH (compresstype=zlib,checksum=false,appendonly=true,blocksize=491520,compresslevel=1);

COPY T76 FROM STDIN delimiter '|';
5|5|2|4|5|1|2|5|3
\.
INSERT INTO T76 VALUES ( 2, 1, 5, 2, 1, 5, 1, 2, 3 );
COPY T76 FROM STDIN delimiter '|';
4|1|1|3|4|4|5|5|5
\.
INSERT INTO T76 VALUES ( 4, 4, 2, 2, 1, 4, 4, 2, 3 );
INSERT INTO T76 VALUES ( 5, 2, 5, 1, 4, 3, 3, 5, 4 );
INSERT INTO T76 VALUES ( 2, 4, 3, 1, 1, 1, 1, 2, 4 );
COPY T76 FROM STDIN delimiter '|';
1|2|1|1|4|4|3|3|4
\.
INSERT INTO T76 VALUES ( 4, 4, 3, 1, 4, 2, 2, 2, 2 );
COPY T76 FROM STDIN delimiter '|';
1|3|1|2|1|2|4|2|1
\.
INSERT INTO T76 VALUES ( 2, 2, 4, 3, 5, 1, 3, 5, 2 );




create table T83(	C1001 int,	C1002 int,	C1003 int,	C1004 int,	C1005 int,	C1006 int,	C1007 int) WITH (compresstype=zlib,compresslevel=8,appendonly=true,blocksize=335872,checksum=false);

COPY T83 FROM STDIN delimiter '|';
3|4|2|5|4|4|1
\.
INSERT INTO T83 VALUES ( 4, 4, 5, 3, 2, 1, 5 );
INSERT INTO T83 VALUES ( 5, 5, 2, 2, 2, 2, 5 );
COPY T83 FROM STDIN delimiter '|';
1|2|3|2|3|2|3
\.
INSERT INTO T83 VALUES ( 5, 4, 1, 3, 3, 1, 3 );
INSERT INTO T83 VALUES ( 4, 1, 3, 2, 2, 5, 5 );
INSERT INTO T83 VALUES ( 2, 3, 3, 1, 3, 3, 1 );
COPY T83 FROM STDIN delimiter '|';
2|4|5|2|5|4|1
\.
COPY T83 FROM STDIN delimiter '|';
3|3|2|3|2|1|2
\.
COPY T83 FROM STDIN delimiter '|';
1|3|4|2|4|4|4
\.


create table T92(	C1115 int,	C1116 int,	C1117 int,	C1118 int,	C1119 int,	C1120 int,	C1121 int,	C1122 int,	C1123 int,	C1124 int,	C1125 int,	C1126 int,	C1127 int,	C1128 int,	C1129 int,	C1130 int,	C1131 int,	C1132 int,	C1133 int,	C1134 int,	C1135 int) WITH (compresstype=zlib,blocksize=1974272,appendonly=true,compresslevel=6,checksum=false);

COPY T92 FROM STDIN delimiter '|';
2|5|5|2|3|5|2|4|3|1|3|3|3|5|5|1|5|2|4|1|3
\.
COPY T92 FROM STDIN delimiter '|';
2|1|1|3|2|2|3|2|2|3|5|1|1|4|5|5|1|1|4|1|2
\.
INSERT INTO T92 VALUES ( 5, 4, 3, 5, 1, 5, 3, 2, 5, 2, 4, 2, 4, 5, 5, 4, 3, 2, 4, 2, 5 );
COPY T92 FROM STDIN delimiter '|';
5|5|3|3|3|2|5|2|2|5|1|5|5|3|2|1|2|2|2|1|1
\.
INSERT INTO T92 VALUES ( 3, 1, 5, 5, 4, 2, 5, 2, 5, 1, 1, 3, 5, 3, 5, 4, 1, 2, 1, 2, 5 );
COPY T92 FROM STDIN delimiter '|';
1|2|4|1|1|1|4|3|5|5|5|1|4|5|1|5|1|3|1|4|2
\.
COPY T92 FROM STDIN delimiter '|';
2|1|3|2|2|4|4|1|1|1|3|5|1|1|4|2|2|2|4|5|3
\.
INSERT INTO T92 VALUES ( 3, 2, 3, 5, 4, 3, 1, 4, 2, 4, 2, 1, 3, 5, 1, 5, 3, 2, 4, 4, 1 );
COPY T92 FROM STDIN delimiter '|';
5|2|4|3|1|1|5|1|1|5|1|2|2|3|2|3|2|5|5|5|5
\.
INSERT INTO T92 VALUES ( 1, 4, 1, 4, 2, 4, 1, 2, 4, 4, 1, 2, 2, 5, 2, 4, 1, 2, 5, 3, 3 );





create table T97(	C1190 int,	C1191 int,	C1192 int,	C1193 int,	C1194 int,	C1195 int,	C1196 int,	C1197 int,	C1198 int,	C1199 int,	C1200 int) WITH (compresstype=zlib,compresslevel=6,appendonly=true,blocksize=573440,checksum=false);

INSERT INTO T97 VALUES ( 2, 1, 1, 2, 1, 3, 1, 2, 5, 2, 5 );
INSERT INTO T97 VALUES ( 4, 3, 5, 5, 5, 2, 2, 3, 5, 1, 1 );
COPY T97 FROM STDIN delimiter '|';
3|4|1|2|2|2|4|5|4|4|4
\.
INSERT INTO T97 VALUES ( 4, 2, 5, 1, 4, 4, 5, 5, 4, 5, 2 );
COPY T97 FROM STDIN delimiter '|';
1|1|5|1|2|4|3|4|1|3|1
\.
COPY T97 FROM STDIN delimiter '|';
5|1|5|4|1|3|1|3|1|4|1
\.
COPY T97 FROM STDIN delimiter '|';
5|2|2|4|1|4|4|4|2|1|3
\.
COPY T97 FROM STDIN delimiter '|';
5|1|1|5|4|3|1|4|3|2|3
\.
COPY T97 FROM STDIN delimiter '|';
4|5|5|1|3|3|5|5|5|5|1
\.
COPY T97 FROM STDIN delimiter '|';
5|4|4|2|3|1|4|2|5|4|4
\.


create table T98(	C1201 int,	C1202 int,	C1203 int) WITH (compresstype=zlib,blocksize=1769472,appendonly=true,compresslevel=4);

INSERT INTO T98 VALUES ( 4, 4, 4 );
COPY T98 FROM STDIN delimiter '|';
2|1|1
\.
COPY T98 FROM STDIN delimiter '|';
4|5|4
\.
COPY T98 FROM STDIN delimiter '|';
2|3|3
\.
COPY T98 FROM STDIN delimiter '|';
3|4|2
\.
COPY T98 FROM STDIN delimiter '|';
4|1|3
\.
COPY T98 FROM STDIN delimiter '|';
5|4|1
\.
COPY T98 FROM STDIN delimiter '|';
1|5|4
\.
INSERT INTO T98 VALUES ( 4, 3, 5 );
COPY T98 FROM STDIN delimiter '|';
5|2|5
\.

create table T99(	C1204 int,	C1205 int,	C1206 int,	C1207 int,	C1208 int) WITH (compresstype=zlib,appendonly=true,blocksize=1490944,compresslevel=6,checksum=false);

INSERT INTO T99 VALUES ( 5, 3, 3, 4, 1 );
COPY T99 FROM STDIN delimiter '|';
1|1|1|1|1
\.
INSERT INTO T99 VALUES ( 2, 2, 1, 4, 2 );
INSERT INTO T99 VALUES ( 4, 1, 5, 4, 5 );
COPY T99 FROM STDIN delimiter '|';
1|1|2|2|1
\.
INSERT INTO T99 VALUES ( 3, 3, 5, 2, 2 );
INSERT INTO T99 VALUES ( 3, 1, 5, 1, 1 );
INSERT INTO T99 VALUES ( 4, 4, 2, 3, 1 );
COPY T99 FROM STDIN delimiter '|';
2|1|4|2|4
\.
COPY T99 FROM STDIN delimiter '|';
4|5|5|2|3
\.


CREATE VIEW view_DeepSlicedqueries_AOtables01 (col1,col2,col3,col4,col5,col6) AS
SELECT
	DT102.C339
	, DT102.C340
	, DT100.C498
	, SUM( DT102.C339 )
	, SUM( DT102.C340 )
	, COUNT( DT100.C498 )
FROM
	(
		(
			T43 DT100
		INNER JOIN
			T33 DT101
		ON
			DT100.C498 = DT101.C401
		)
	INNER JOIN
		T29 DT102
	ON
		DT101.C385 = DT102.C338
	)
WHERE
	(
		(
			DT101.C390 = DT100.C500
		)
		OR
		(
			(
				DT102.C338 <> DT101.C388
			)
			AND
			(
				DT102.C336 < DT100.C494
			)
		)
	)
	OR
	(
		(
			DT101.C391 < DT101.C399
		)
		AND
		(
			DT102.C335 = DT100.C490
		)
	)
GROUP BY
	DT100.C498
	, DT102.C339
	, DT102.C340
ORDER BY
	DT102.C339
	, DT102.C340
	, DT100.C498
	, SUM( DT102.C339 )
	, SUM( DT102.C340 )
	, COUNT( DT100.C498 )
LIMIT 497;

SELECT * FROM view_DeepSlicedqueries_AOtables01;

CREATE VIEW view_DeepSlicedqueries_AOtables07 (col1,col2,col3,col4) AS
SELECT
	DT186.C721
	, AVG( DT186.C720 )
	, DT186.C720
	, DT184.C1297
FROM
	(
		(
			(
				T53 DT187
			INNER JOIN
				T60 DT186
			ON
				DT187.C638 = DT186.C720
			)
		INNER JOIN
			(
			SELECT
				DT183.C812
			FROM
				(
					T67 DT183
				INNER JOIN
					T3 DT182
				ON
					DT183.C814 = DT182.C50
				)
			WHERE
				(
					(
						(
							DT182.C50 <> DT182.C47
						)
						AND
						(
							(
								(
									DT182.C52 = DT182.C41
								)
								AND
								(
									(
										DT182.C39 <> DT182.C44
									)
									AND
									(
										DT182.C35 > DT182.C52
									)
								)
							)
							OR
							(
								DT182.C38 = DT182.C42
							)
						)
					)
					AND
					(
						DT182.C45 = DT182.C32
					)
				)
				AND
				(
					DT182.C45 = DT183.C818
				)
			GROUP BY
				DT182.C33
				, DT182.C44
				, DT183.C812
				, DT182.C42
				, DT183.C814
			ORDER BY
				DT183.C812
			LIMIT 590
			) AS DT184 ( C1297 ) 
		ON
			DT187.C651 = DT184.C1297
		)
	INNER JOIN
		T41 DT185
	ON
		DT186.C724 <> DT185.C464
	)
WHERE
	(
		(
			(
				DT185.C465 <> DT187.C648
			)
			AND
			(
				(
					DT184.C1297 = DT187.C647
				)
				OR
				(
					DT185.C470 = DT187.C650
				)
			)
		)
		AND
		(
			(
				(
					DT186.C723 <> DT187.C651
				)
				OR
				(
					DT185.C456 < DT187.C647
				)
			)
			AND
			(
				DT187.C639 > DT185.C453
			)
		)
	)
	AND
	(
		(
			DT187.C653 = DT185.C472
		)
		AND
		(
			DT185.C458 <> DT185.C460
		)
	)
GROUP BY
	DT184.C1297
	, DT186.C720
	, DT185.C461
	, DT185.C460
	, DT186.C721
ORDER BY
	DT186.C721
	, AVG( DT186.C720 )
	, DT186.C720
	, DT184.C1297
LIMIT 260;

SELECT * FROM view_DeepSlicedqueries_AOtables07;

CREATE VIEW view_DeepSlicedqueries_AOtables13 (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17) AS

SELECT
	DT120.C629
	, DT119.C241
	, DT119.C246
	, DT119.C242
	, DT120.C626
	, DT119.C239
	, DT119.C237
	, DT120.C632
	, DT120.C633
	, DT119.C244
	, DT120.C635
	, DT119.C234
	, DT119.C245
	, DT120.C631
	, DT120.C630
	, DT120.C628
	, DT119.C243
FROM
	(
		T20 DT119
	INNER JOIN
		T52 DT120
	ON
		DT119.C233 = DT120.C631
	)
WHERE
	(
		DT119.C244 = DT120.C627
	)
	OR
	(
		(
			DT119.C246 < DT119.C245
		)
		AND
		(
			DT119.C246 = DT120.C626
		)
	)
ORDER BY
	DT120.C629
	, DT119.C241
	, DT119.C246
	, DT119.C242
	, DT120.C626
	, DT119.C239
	, DT119.C237
	, DT120.C632
	, DT120.C633
	, DT119.C244
	, DT120.C635
	, DT119.C234
	, DT119.C245
	, DT120.C631
	, DT120.C630
	, DT120.C628
	, DT119.C243
LIMIT 118;

SELECT * FROM view_DeepSlicedqueries_AOtables13;


CREATE VIEW view_DeepSlicedqueries_AOtables19 AS
SELECT
	DT126.C405
	, DT125.C605
	, DT126.C409
	, DT124.C510
	, DT124.C504
	, DT124.C507
	, DT127.C1201
	, DT126.C406
	, DT123.C455
	, DT123.C453
	, DT123.C462
	, DT123.C452
	, DT123.C457
	, DT123.C459
	, DT123.C472
	, DT125.C604
	, DT126.C410
	, DT124.C502
	, DT124.C508
	, DT123.C468
	, DT123.C454
	, DT123.C456
	, DT126.C407
	, DT124.C509
FROM
	(
		(
			(
				T34 DT126
			LEFT OUTER JOIN
				T44 DT124
			ON
				DT126.C402 > DT124.C511
			)
		RIGHT OUTER JOIN
			(
				T41 DT123
			LEFT OUTER JOIN
				T98 DT127
			ON
				DT123.C458 > DT127.C1201
			)
		ON
			DT126.C409 = DT123.C465
		)
	INNER JOIN
		T50 DT125
	ON
		DT127.C1202 = DT125.C603
	)
WHERE
	(
		DT123.C469 < DT123.C470
	)
	AND
	(
		DT123.C452 = DT127.C1201
	)
ORDER BY
	DT126.C405
	, DT125.C605
	, DT126.C409
	, DT124.C510
	, DT124.C504
	, DT124.C507
	, DT127.C1201
	, DT126.C406
	, DT123.C455
	, DT123.C453
	, DT123.C462
	, DT123.C452
	, DT123.C457
	, DT123.C459
	, DT123.C472
	, DT125.C604
	, DT126.C410
	, DT124.C502
	, DT124.C508
	, DT123.C468
	, DT123.C454
	, DT123.C456
	, DT126.C407
	, DT124.C509
LIMIT 505;

SELECT * FROM view_DeepSlicedqueries_AOtables19;

CREATE VIEW view_DeepSlicedqueries_AOtables25 AS
SELECT
	DT240.C845
	, DT240.C858
	, DT240.C841
	, MAX( DT240.C845 )
	, DT240.C844
FROM
	(
		(
			(
			SELECT
				DT229.C1124
				, AVG( DT230.C456 )
			FROM
				(
					(
						(
							T41 DT230
						INNER JOIN
							T83 DT232
						ON
							DT230.C465 > DT232.C1007
						)
					INNER JOIN
						T92 DT229
					ON
						DT230.C456 = DT229.C1125
					)
				INNER JOIN
					T11 DT231
				ON
					DT229.C1128 <> DT231.C153
				)
			WHERE
				(
					(
						DT230.C472 = DT230.C465
					)
					AND
					(
						DT229.C1122 > DT230.C456
					)
				)
				AND
				(
					(
						DT229.C1132 = DT230.C454
					)
					AND
					(
						DT229.C1121 = DT230.C459
					)
				)
			GROUP BY
				DT230.C456
				, DT229.C1119
				, DT229.C1124
			ORDER BY
				DT229.C1124
				, AVG( DT230.C456 )
			LIMIT 217
			)  DT233 ( C1427, C1428 ) 
		INNER JOIN
			T98 DT239
		ON
			DT233.C1427 <> DT239.C1202
		)
	INNER JOIN
		(
			(
				T70 DT240
			INNER JOIN
				T76 DT238
			ON
				DT240.C844 = DT238.C945
			)
		INNER JOIN
			(
			SELECT
				DT234.C1193
				, DT235.C657
			FROM
				(
					(
						T97 DT234
					LEFT OUTER JOIN
						T54 DT235
					ON
						DT234.C1192 = DT235.C657
					)
				INNER JOIN
					T99 DT236
				ON
					DT234.C1191 = DT236.C1205
				)
			WHERE
				(
					(
						DT234.C1196 > DT234.C1199
					)
					OR
					(
						DT235.C654 = DT234.C1190
					)
				)
				AND
				(
					(
						DT234.C1193 = DT236.C1204
					)
					AND
					(
						DT235.C657 <> DT234.C1198
					)
				)
			GROUP BY
				DT234.C1193
				, DT235.C655
				, DT235.C657
			ORDER BY
				DT234.C1193
				, DT235.C657
			LIMIT 196
			) AS DT237 ( C1430, C1431 ) 
		ON
			DT238.C946 = DT237.C1431
		)
	ON
		DT239.C1201 < DT240.C842
	)
WHERE
	(
		(
			(
				DT240.C845 > DT238.C941
			)
			AND
			(
				DT238.C946 > DT239.C1201
			)
		)
		OR
		(
			DT237.C1431 = DT238.C940
		)
	)
	AND
	(
		DT233.C1428 = DT238.C943
	)
GROUP BY
	DT240.C845
	, DT240.C841
	, DT240.C858
	, DT240.C844
ORDER BY
	DT240.C845
	, DT240.C858
	, DT240.C841
	, MAX( DT240.C845 )
	, DT240.C844
LIMIT 178;

SELECT * FROM view_DeepSlicedqueries_AOtables25;


drop table if exists customer;
drop table if exists vendor;
drop table if exists product;
drop table if exists sale;
drop table if exists sale_ord;
drop table if exists util;


create table customer 
(
	cn int not null,
	cname text not null,
	cloc text,
	
	primary key (cn)
	
) distributed by (cn);

create table vendor 
(
	vn int not null,
	vname text not null,
	vloc text,
	
	primary key (vn)
	
) distributed by (vn);

create table product 
(
	pn int not null,
	pname text not null,
	pcolor text,
	
	primary key (pn)
	
) distributed by (pn);

create table sale
(
	cn int not null,
	vn int not null,
	pn int not null,
	dt date not null,
	qty int not null,
	prc float not null,
	
	primary key (cn, vn, pn)
	
) distributed by (cn,vn,pn);

create table sale_ord
(
        ord int not null,
	cn int not null,
	vn int not null,
	pn int not null,
	dt date not null,
	qty int not null,
	prc float not null,
	
	primary key (cn, vn, pn)
	
) distributed by (cn,vn,pn);

create table util
(
	xn int not null,
	
	primary key (xn)
	
) distributed by (xn);

-- Customers
insert into customer values 
  ( 1, 'Macbeth', 'Inverness'),
  ( 2, 'Duncan', 'Forres'),
  ( 3, 'Lady Macbeth', 'Inverness'),
  ( 4, 'Witches, Inc', 'Lonely Heath');

-- Vendors
insert into vendor values 
  ( 10, 'Witches, Inc', 'Lonely Heath'),
  ( 20, 'Lady Macbeth', 'Inverness'),
  ( 30, 'Duncan', 'Forres'),
  ( 40, 'Macbeth', 'Inverness'),
  ( 50, 'Macduff', 'Fife');

-- Products
insert into product values 
  ( 100, 'Sword', 'Black'),
  ( 200, 'Dream', 'Black'),
  ( 300, 'Castle', 'Grey'),
  ( 400, 'Justice', 'Clear'),
  ( 500, 'Donuts', 'Plain'),
  ( 600, 'Donuts', 'Chocolate'),
  ( 700, 'Hamburger', 'Grey'),
  ( 800, 'Fries', 'Grey');


-- Sales (transactions)
insert into sale values 
  ( 2, 40, 100, '1401-1-1', 1100, 2400),
  ( 1, 10, 200, '1401-3-1', 1, 0),
  ( 3, 40, 200, '1401-4-1', 1, 0),
  ( 1, 20, 100, '1401-5-1', 1, 0),
  ( 1, 30, 300, '1401-5-2', 1, 0),
  ( 1, 50, 400, '1401-6-1', 1, 0),
  ( 2, 50, 400, '1401-6-1', 1, 0),
  ( 1, 30, 500, '1401-6-1', 12, 5),
  ( 3, 30, 500, '1401-6-1', 12, 5),
  ( 3, 30, 600, '1401-6-1', 12, 5),
  ( 4, 40, 700, '1401-6-1', 1, 1),
  ( 4, 40, 800, '1401-6-1', 1, 1);

-- Sales (ord transactions)
insert into sale_ord values 
  ( 1,2, 40, 100, '1401-1-1', 1100, 2400),
  ( 2,1, 10, 200, '1401-3-1', 1, 0),
  ( 3,3, 40, 200, '1401-4-1', 1, 0),
  ( 4,1, 20, 100, '1401-5-1', 1, 0),
  ( 5,1, 30, 300, '1401-5-2', 1, 0),
  ( 6,1, 50, 400, '1401-6-1', 1, 0),
  ( 7,2, 50, 400, '1401-6-1', 1, 0),
  ( 8,1, 30, 500, '1401-6-1', 12, 5),
  ( 9,3, 30, 500, '1401-6-1', 12, 5),
  ( 10,3, 30, 600, '1401-6-1', 12, 5),
  ( 11,4, 40, 700, '1401-6-1', 1, 1),
  ( 12,4, 40, 800, '1401-6-1', 1, 1);

-- Util

insert into util values 
 (1),
(20),
(300);
                   -- ###### Queries involving AVG() function ###### --

CREATE VIEW view_olap_group1 (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15) AS 
SELECT sale.cn,sale.pn,sale.cn,sale.prc,sale.pn,sale.dt,GROUPING(sale.pn,sale.cn,sale.pn),GROUP_ID(),
TO_CHAR(COALESCE(AVG(floor(sale.vn+sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(DISTINCT floor(sale.qty-sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(DISTINCT floor(sale.qty/sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(DISTINCT floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_POP(floor(sale.cn/sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.vn-sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.qty*sale.vn)),0),'99999999.9999999')
FROM sale,customer,product
WHERE sale.cn=customer.cn AND sale.pn=product.pn
GROUP BY CUBE((sale.pn,sale.cn,sale.prc),(sale.vn,sale.qty,sale.vn)),sale.dt;

select * from view_olap_group1;
                   -- ###### Queries involving COUNT() function ###### --

CREATE VIEW view_olap_group2(col1,col2,col3,col4,col5,col6) AS SELECT sale.vn,sale.pn,GROUP_ID(), 
TO_CHAR(COALESCE(COUNT(floor(sale.qty*sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.vn)),0),'99999999.9999999')
FROM sale,product
WHERE sale.pn=product.pn
GROUP BY ROLLUP((sale.dt,sale.qty,sale.cn)),CUBE((sale.prc,sale.pn)),GROUPING SETS(CUBE((sale.cn,sale.cn),(sale.dt,sale.cn),(sale.cn)),
CUBE((sale.cn,sale.cn),(sale.vn,sale.cn),(sale.pn),(sale.qty),(sale.qty,sale.vn,sale.vn),(sale.pn))) HAVING GROUP_ID() <> 0;

select * from view_olap_group2;
                   -- ###### Queries involving MAX() function ###### --

CREATE VIEW view_olap_group3(col1,col2,col3,col4,col5,col6,col7) AS 
SELECT sale.qty,sale.vn, 
TO_CHAR(COALESCE(MAX(floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_SAMP(floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_POP(floor(sale.qty-sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(AVG(DISTINCT floor(sale.cn-sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(DISTINCT floor(sale.pn-sale.prc)),0),'99999999.9999999') 
FROM sale,customer,vendor
WHERE sale.cn=customer.cn AND sale.vn=vendor.vn
GROUP BY GROUPING SETS((sale.qty),(sale.dt,sale.qty),(sale.dt,sale.vn),(sale.vn),CUBE((sale.vn),(sale.qty,sale.pn,sale.pn),(sale.prc,sale.qty)));

select * from view_olap_group3;
                   -- ###### Queries involving MIN() function ###### --

CREATE VIEW view_olap_group4(col1,col2,col3,col4,col5,col6) AS SELECT sale.cn, 
TO_CHAR(COALESCE(MIN(floor(sale.cn+sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(DISTINCT floor(sale.vn+sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_POP(floor(sale.pn-sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(AVG(DISTINCT floor(sale.prc+sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(floor(sale.prc)),0),'99999999.9999999')
FROM sale,product,vendor,customer
WHERE sale.pn=product.pn AND sale.vn=vendor.vn AND sale.cn=customer.cn
GROUP BY (sale.pn,sale.dt),(sale.qty,sale.qty),(sale.qty),(sale.vn,sale.vn),GROUPING SETS(ROLLUP((sale.qty,sale.qty),(sale.vn))),sale.cn;

select * from view_olap_group4;
                   -- ###### Queries involving STDDEV() function ###### --

CREATE VIEW view_olap_group5(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14) AS 
SELECT sale.dt,sale.pn,sale.vn,GROUPING(sale.vn,sale.pn,sale.dt),
TO_CHAR(COALESCE(STDDEV(DISTINCT floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(DISTINCT floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(DISTINCT floor(sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.qty+sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(DISTINCT floor(sale.vn-sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(DISTINCT floor(sale.qty*sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(DISTINCT floor(sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.pn+sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(DISTINCT floor(sale.qty)),0),'99999999.9999999')
FROM sale,product,vendor,customer
WHERE sale.pn=product.pn AND sale.vn=vendor.vn AND sale.cn=customer.cn
GROUP BY ROLLUP((sale.vn)),ROLLUP((sale.cn,sale.pn)),(),sale.dt;

select * from view_olap_group5;
                   -- ###### Queries involving STDDEV_POP() function ###### --

CREATE VIEW view_olap_group6(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16) AS 
SELECT sale.dt,sale.vn,sale.qty,sale.qty,sale.qty,sale.pn, 
TO_CHAR(COALESCE(STDDEV_POP(floor(sale.prc+sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.prc+sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.pn-sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.prc/sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_SAMP(floor(sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(DISTINCT floor(sale.cn)),0),'99999999.9999999'),TO_CHAR(COALESCE(SUM(floor(sale.vn/sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(DISTINCT floor(sale.qty-sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.prc+sale.pn)),0),'99999999.9999999')
FROM sale,customer,product
WHERE sale.cn=customer.cn AND sale.pn=product.pn
GROUP BY ROLLUP((sale.pn,sale.vn)),GROUPING SETS(ROLLUP((sale.pn,sale.qty),(sale.cn,sale.cn),(sale.cn,sale.dt)));

select * from view_olap_group6;
                   -- ###### Queries involving STDDEV_SAMP() function ###### --

CREATE VIEW view_olap_group7(col1,col2,col3,col4) AS SELECT sale.prc,sale.dt, 
TO_CHAR(COALESCE(STDDEV_SAMP(floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(AVG(DISTINCT floor(sale.pn*sale.prc)),0),'99999999.9999999')
FROM sale,vendor
WHERE sale.vn=vendor.vn
GROUP BY CUBE((sale.vn,sale.cn),(sale.cn),(sale.vn)),CUBE((sale.qty,sale.cn,sale.cn),(sale.prc),
(sale.pn,sale.qty,sale.prc),(sale.cn),(sale.cn),(sale.qty,sale.dt));

select * from view_olap_group7;
                   -- ###### Queries involving SUM() function ###### --

CREATE VIEW view_olap_group8(col1,col2,col3,col4,col5,col6,col7,col8,col9) AS SELECT sale.qty,sale.vn,GROUP_ID(), 
TO_CHAR(COALESCE(SUM(floor(sale.prc+sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.pn/sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.vn+sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(DISTINCT floor(sale.pn*sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.qty-sale.prc)),0),'99999999.9999999')
FROM sale,product
WHERE sale.pn=product.pn
GROUP BY CUBE((sale.cn,sale.dt),(sale.prc,sale.cn)),sale.qty,sale.vn;

select * from view_olap_group8;
                   -- ###### Queries involving VAR_POP() function ###### --

CREATE VIEW view_olap_group9(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15) AS 
SELECT sale.vn,sale.cn,sale.dt,sale.qty,sale.cn,sale.cn,GROUP_ID(), 
TO_CHAR(COALESCE(VAR_POP(floor(sale.cn-sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_POP(floor(sale.pn/sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_POP(floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_POP(floor(sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(DISTINCT floor(sale.cn-sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.prc*sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_POP(floor(sale.cn)),0),'99999999.9999999')
FROM sale,customer
WHERE sale.cn=customer.cn
GROUP BY ROLLUP((sale.cn,sale.vn,sale.cn)),sale.dt,sale.qty HAVING COALESCE(VAR_POP(sale.cn),0) <> 51.3347071039536;

select * from view_olap_group9;
                   -- ###### Queries involving VAR_SAMP() function ###### --

CREATE VIEW view_olap_group10(col1,col2,col3,col4,col5,col6,col7,col8,col9) AS SELECT sale.dt,sale.vn,GROUP_ID(), 
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.vn-sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.pn-sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_SAMP(floor(sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(floor(sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.qty)),0),'99999999.9999999')
FROM sale,product,vendor
WHERE sale.pn=product.pn AND sale.vn=vendor.vn
GROUP BY GROUPING SETS(CUBE((sale.pn,sale.vn),(sale.pn,sale.qty),(sale.qty),(sale.vn,sale.dt)),ROLLUP((sale.cn),(sale.prc,sale.vn,sale.qty),(sale.dt,sale.prc),(sale.
pn,sale.vn),(sale.pn,sale.dt)),(sale.qty,sale.cn,sale.vn),(sale.dt,sale.cn),(sale.pn)),ROLLUP((sale.pn),(sale.cn,sale.cn),(sale.qty),(sale.pn,sale.pn),(sale.qty,sale
.vn),(sale.pn,sale.qty)) HAVING COALESCE(STDDEV_POP(sale.vn),0) = 7.00061881102982;

select * from view_olap_group10;
                   -- ###### Queries involving VARIANCE() function ###### --

CREATE VIEW view_olap_group11(col1,col2,col3,col4,col5,col6) AS SELECT sale.pn, 
TO_CHAR(COALESCE(VARIANCE(floor(sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(AVG(floor(sale.qty+sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.cn+sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_SAMP(floor(sale.qty)),0),'99999999.9999999')
FROM sale,product,vendor
WHERE sale.pn=product.pn AND sale.vn=vendor.vn
GROUP BY (),GROUPING SETS((),(),()),sale.pn HAVING COALESCE(COUNT(sale.pn),0) <> 38.4292159148934 OR NOT GROUPING(sale.pn) < 0;

select * from view_olap_group11;
                   -- ###### Queries involving CORR() function ###### --

CREATE VIEW view_olap_group12(col1,col2,col3,col4,col5,col6) AS SELECT sale.pn,sale.dt,GROUP_ID(), 
TO_CHAR(COALESCE(CORR(floor(sale.qty),floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_POP(floor(sale.vn+sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(DISTINCT floor(sale.pn-sale.qty)),0),'99999999.9999999')
FROM sale,product,vendor,customer
WHERE sale.pn=product.pn AND sale.vn=vendor.vn AND sale.cn=customer.cn
GROUP BY ROLLUP((sale.prc,sale.vn)),GROUPING SETS((),ROLLUP((sale.qty),(sale.pn,sale.vn,sale.cn),(sale.qty),(sale.pn,sale.cn,sale.pn)),()),sale.dt 
HAVING GROUP_ID()= 1;

select * from view_olap_group12;
                   -- ###### Queries involving COVAR_POP() function ###### --

CREATE VIEW view_olap_group13(col1,col2,col3,col4,col5,col6,col7,col8) AS 
SELECT sale.qty,sale.qty,sale.pn,sale.cn,GROUPING(sale.qty,sale.cn,sale.qty,sale.qty),GROUP_ID(), 
TO_CHAR(COALESCE(COVAR_POP(floor(sale.pn),floor(sale.prc+sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(AVG(DISTINCT floor(sale.prc)),0),'99999999.9999999')
FROM sale,customer,vendor
WHERE sale.cn=customer.cn AND sale.vn=vendor.vn
GROUP BY GROUPING SETS((sale.prc),(sale.qty),(sale.dt,sale.pn),(sale.pn),CUBE((sale.cn),(sale.vn,sale.pn,sale.cn),
(sale.pn,sale.prc),(sale.dt),(sale.cn,sale.cn),(sale.qty)),()),(),CUBE((sale.qty,sale.prc,sale.dt),(sale.vn),(sale.vn,sale.dt));

select * from view_olap_group13;
                   -- ###### Queries involving COVAR_SAMP() function ###### --

CREATE VIEW view_olap_group14(col1,col2,col3,col4,col5) AS SELECT sale.qty,GROUPING(sale.qty),GROUP_ID(), 
TO_CHAR(COALESCE(COVAR_SAMP(floor(sale.vn*sale.pn),floor(sale.vn-sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_POP(floor(sale.vn)),0),'99999999.9999999')
FROM sale,customer
WHERE sale.cn=customer.cn
GROUP BY (),sale.qty HAVING GROUPING(sale.qty) < 1;

select * from view_olap_group14;
                   -- ###### Queries involving REGR_AVGX() function ###### --

CREATE VIEW view_olap_group15(col1,col2,col3,col4,col5,col6,col7) AS 
SELECT sale.qty,sale.pn,GROUP_ID(), 
TO_CHAR(COALESCE(REGR_AVGX(floor(sale.vn),floor(sale.prc-sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_SAMP(floor(sale.prc+sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(DISTINCT floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(floor(sale.vn)),0),'99999999.9999999') 
FROM sale,vendor
WHERE sale.vn=vendor.vn
GROUP BY ROLLUP((sale.qty,sale.qty),(sale.prc),(sale.pn)),CUBE((sale.qty,sale.qty)),ROLLUP((sale.prc,sale.qty),(sale.vn,sale.prc,sale.dt),
(sale.cn,sale.pn,sale.prc),(sale.vn,sale.qty,sale.vn)) HAVING GROUP_ID() = 0;

select * from view_olap_group15;
                   -- ###### Queries involving REGR_AVGY() function ###### --

CREATE VIEW view_olap_group16(col1,col2) AS 
SELECT sale.vn, TO_CHAR(COALESCE(REGR_AVGY(floor(sale.pn/sale.vn),floor(sale.pn-sale.cn)),0),'99999999.9999999') 
FROM sale,customer
WHERE sale.cn=customer.cn
GROUP BY (),ROLLUP((sale.pn,sale.qty,sale.qty),(sale.qty),(sale.pn),(sale.pn),(sale.prc,sale.pn,sale.dt),(sale.vn)),(sale.pn),(sale.qty),(sale.qty);

select * from view_olap_group16;
                   -- ###### Queries involving REGR_COUNT() function ###### --

CREATE VIEW view_olap_group17(col1,col2,col3,col4,col5,col6,col7) AS 
SELECT sale.qty,sale.cn,sale.vn,sale.pn, 
TO_CHAR(COALESCE(REGR_COUNT(floor(sale.prc),floor(sale.vn/sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(DISTINCT floor(sale.pn+sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(DISTINCT floor(sale.qty)),0),'99999999.9999999')
FROM sale,product,vendor
WHERE sale.pn=product.pn AND sale.vn=vendor.vn
GROUP BY (),sale.qty,sale.cn,sale.vn,sale.pn;

select * from view_olap_group17;
                   -- ###### Queries involving REGR_INTERCEPT() function ###### --

CREATE VIEW view_olap_group18(col1,col2,col3,col4) AS SELECT sale.dt,sale.cn,sale.dt, 
TO_CHAR(COALESCE(REGR_INTERCEPT(floor(sale.qty+sale.pn),floor(sale.pn+sale.vn)),0),'99999999.9999999')
FROM sale,vendor
WHERE sale.vn=vendor.vn
GROUP BY (),CUBE((sale.vn)),CUBE((sale.qty,sale.cn,sale.qty)),sale.dt;

select * from view_olap_group18;
                   -- ###### Queries involving REGR_R2() function ###### --

CREATE VIEW view_olap_group19(col1,col2,col3,col4,col5,col6,col7,col8) AS 
SELECT sale.prc,sale.qty,sale.cn,sale.pn,sale.qty,GROUPING(sale.pn,sale.prc,sale.qty,sale.qty,sale.pn), 
TO_CHAR(COALESCE(REGR_R2(floor(sale.cn),floor(sale.cn*sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(floor(sale.vn)),0),'99999999.9999999')FROM sale,customer,vendor
WHERE sale.cn=customer.cn AND sale.vn=vendor.vn
GROUP BY CUBE((sale.qty,sale.prc)),CUBE((sale.pn,sale.cn),(sale.cn),(sale.pn),(sale.cn,sale.qty),(sale.vn,sale.dt)),
GROUPING SETS((),CUBE((sale.pn,sale.dt,sale.qty),(sale.cn,sale.pn,sale.cn),(sale.vn),(sale.cn,sale.cn,sale.cn),(sale.vn),(sale.dt)),());

select * from view_olap_group19;
                   -- ###### Queries involving REGR_SLOPE() function ###### --

CREATE VIEW view_olap_group20(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14) AS 
SELECT sale.vn,sale.qty,sale.dt,sale.cn,sale.dt,GROUPING(sale.dt,sale.dt), 
TO_CHAR(COALESCE(REGR_SLOPE(floor(sale.pn+sale.prc),floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.cn+sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(DISTINCT floor(sale.prc-sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(DISTINCT floor(sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_POP(floor(sale.qty-sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_POP(floor(sale.prc*sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.qty)),0),'99999999.9999999')
FROM sale,customer,vendor
WHERE sale.cn=customer.cn AND sale.vn=vendor.vn
GROUP BY (sale.pn,sale.dt,sale.cn),(sale.cn),(sale.pn),(sale.dt),(sale.qty),sale.vn;

select * from view_olap_group20;
                   -- ###### Queries involving REGR_SXX() function ###### --

CREATE VIEW view_olap_group21(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10) AS 
SELECT sale.dt,GROUP_ID(), 
TO_CHAR(COALESCE(REGR_SXX(floor(sale.prc),floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.pn+sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(DISTINCT floor(sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.cn/sale.cn)),0),'99999999.9999999')
FROM sale,product,vendor
WHERE sale.pn=product.pn AND sale.vn=vendor.vn
GROUP BY CUBE((sale.dt),(sale.qty,sale.vn,sale.qty)),CUBE((sale.vn,sale.vn),(sale.pn,sale.vn),(sale.vn,sale.qty),(sale.cn,sale.pn),
(sale.cn,sale.pn,sale.dt),(sale.cn,sale.cn));

select * from view_olap_group21;
                   -- ###### Queries involving REGR_SXY() function ###### --

CREATE VIEW view_olap_group22(col1,col2,col3,col4,col5,col6,col7) AS 
SELECT sale.qty,sale.vn,GROUPING(sale.vn), 
TO_CHAR(COALESCE(REGR_SXY(floor(sale.vn+sale.cn),floor(sale.qty-sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(DISTINCT floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_POP(floor(sale.cn*sale.prc)),0),'99999999.9999999')
FROM sale,product
WHERE sale.pn=product.pn
GROUP BY (),CUBE((sale.cn,sale.qty)),ROLLUP((sale.vn),(sale.cn,sale.vn)) HAVING COALESCE(SUM(sale.qty),0) > 61.9921362275182 
OR COALESCE(SUM(sale.qty),0) <> 22.337350780727;

select * from view_olap_group22;
                   -- ###### Queries involving REGR_SYY() function ###### --

CREATE VIEW view_olap_group23(col1,col2,col3,col4) AS SELECT sale.cn,sale.cn, 
TO_CHAR(COALESCE(REGR_SYY(floor(sale.qty),floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.vn-sale.prc)),0),'99999999.9999999')
FROM sale,product
WHERE sale.pn=product.pn
GROUP BY ROLLUP((sale.qty,sale.qty,sale.pn),(sale.pn),(sale.dt,sale.qty),(sale.vn,sale.qty),(sale.dt,sale.qty,sale.cn),(sale.prc,sale.prc,sale.qty));

select * from view_olap_group23;
             -- ###### Queries involving VAR_SAMP() function ###### --

CREATE VIEW view_olap_group24(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15) AS 
SELECT sale.vn,sale.vn,sale.vn,sale.vn,GROUPING(sale.vn,sale.vn,sale.vn), 
TO_CHAR(COALESCE(VAR_SAMP(floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_POP(floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(DISTINCT floor(sale.pn*sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(DISTINCT floor(sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(AVG(DISTINCT floor(sale.vn-sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_SAMP(floor(sale.cn*sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_SAMP(floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(DISTINCT floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(DISTINCT floor(sale.vn)),0),'99999999.9999999')
FROM sale
GROUP BY (),ROLLUP((sale.pn),(sale.vn,sale.pn)),ROLLUP((sale.qty,sale.vn),(sale.prc,sale.pn,sale.qty)) 
HAVING GROUPING(sale.vn,sale.vn) >= 9 AND COALESCE(VAR_POP(sale.vn),0) = 66.2048455138951;

select * from view_olap_group24;
              -- ###### Queries involving VARIANCE() function ###### --

CREATE VIEW view_olap_group25(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11) AS 
SELECT sale.vn,GROUP_ID(), 
TO_CHAR(COALESCE(VARIANCE(floor(sale.qty+sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.pn*sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV_SAMP(floor(sale.vn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_POP(floor(sale.cn+sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.vn-sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(AVG(floor(sale.prc)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VAR_POP(floor(sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(STDDEV(DISTINCT floor(sale.vn/sale.vn)),0),'99999999.9999999')
FROM sale,product,vendor,customer
WHERE sale.pn=product.pn AND sale.vn=vendor.vn AND sale.cn=customer.cn
GROUP BY (),(),sale.vn HAVING GROUP_ID() <> 0;

select * from view_olap_group25;
--
-- STANDARD DATA FOR olap_* TESTS.
--

-- ROW_NUMBER() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) AS SELECT sale.prc,sale.dt,sale.vn,sale.prc,sale.dt, TO_CHAR(COALESCE(ROW_NUMBER() OVER(win1),0),'99999999.9999999'),sale.cn,sale.pn,
TO_CHAR(COALESCE(ROW_NUMBER() OVER(partition by sale.cn,sale.dt,sale.pn,sale.vn order by sale.pn desc,sale.vn desc,sale.vn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(partition by sale.cn,sale.dt,sale.pn,sale.vn order by sale.pn desc,sale.vn desc,sale.vn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win1),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord) sale
WINDOW win1 as (partition by sale.cn,sale.dt,sale.pn,sale.vn order by sale.pn desc,sale.vn desc,sale.vn asc); 
-- mvd 2,7,3,8->6; 2,7,3,8->9; 2,7,3,8->10; 2,7,3,8->11; 2,7,3,8->12; 2,7,3,8->13;

SELECT * FROM view_olap_window1;

-- DENSE_RANK() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window2(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10) AS SELECT sale.pn, TO_CHAR(COALESCE(DENSE_RANK() OVER(win1),0),'99999999.9999999'),sale.dt,sale.cn,sale.vn,
TO_CHAR(COALESCE(DENSE_RANK() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(partition by sale.cn,sale.vn,sale.cn,sale.dt,sale.cn order by sale.cn desc,sale.cn desc,sale.pn desc),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(partition by sale.cn,sale.vn,sale.cn,sale.dt,sale.cn order by sale.cn desc,sale.cn desc,sale.pn desc),0),'99999999.9999999'),
TO_CHAR(COALESCE(DENSE_RANK() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(DENSE_RANK() OVER(partition by sale.cn,sale.vn,sale.cn,sale.dt,sale.cn order by sale.cn desc,sale.cn desc,sale.pn desc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,customer WHERE sale_ord.cn=customer.cn ) sale
WINDOW win1 as (partition by sale.cn,sale.vn,sale.cn,sale.dt,sale.cn order by sale.cn desc,sale.cn desc,sale.pn desc); 
-- mvd 3,4,5,1->2; 3,4,5,1->6; 3,4,5,1->7; 3,4,5,1->8; 3,4,5,1->9; 3,4,5,1->10;

SELECT * FROM view_olap_window2;
-- RANK() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window3(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10) AS SELECT sale.qty,sale.qty,sale.dt,sale.qty,sale.qty,sale.vn, TO_CHAR(COALESCE(RANK() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(partition by sale.dt order by sale.vn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(RANK() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(DENSE_RANK() OVER(partition by sale.dt order by sale.vn asc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product,vendor,customer WHERE sale_ord.pn=product.pn AND sale_ord.vn=vendor.vn AND sale_ord.cn=customer.cn) sale
WINDOW win1 as (partition by sale.dt order by sale.vn asc); -- mvd 3,6->7; 3,6->8; 3,6->9; 3,6->10;

SELECT * FROM view_olap_window3;
-- CUME_DIST() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window4(col1,col2,col3,col4,col5,col6,col7) AS SELECT sale.vn, TO_CHAR(COALESCE(CUME_DIST() OVER(win1),0),'99999999.9999999'),sale.prc,sale.dt,sale.cn,sale.pn,
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win1),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,customer WHERE sale_ord.cn=customer.cn ) sale
WINDOW win1 as (partition by sale.cn,sale.prc,sale.pn,sale.dt,sale.prc,sale.dt order by sale.vn desc); -- mvd 3,4,5,1,6->2; 3,4,5,1,6->7;

SELECT * FROM view_olap_window4;
-- PERCENT_RANK() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window5(col1,col2,col3,col4,col5,col6,col7,col8,col9) AS SELECT sale.cn,sale.qty,sale.qty,sale.dt,sale.cn,sale.qty, TO_CHAR(COALESCE(PERCENT_RANK() OVER(win1),0),'99999999.9999999'),sale.pn,
TO_CHAR(COALESCE(CUME_DIST() OVER(partition by sale.qty,sale.qty,sale.dt order by sale.cn desc,sale.pn desc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord) sale
WINDOW win1 as (partition by sale.qty,sale.qty,sale.dt order by sale.cn desc,sale.pn desc); -- mvd 1,4,2,8->7; 1,4,2,8->9;

SELECT * FROM view_olap_window5;
-- LAG() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window6(col1,col2,col3,col4,col5,col6,col7) AS SELECT sale.pn, TO_CHAR(COALESCE(LAG(cast(floor(sale.cn-sale.pn) as int),cast (floor(sale.pn) as int),NULL) OVER(win1),0),'99999999.9999999'),sale.vn,
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(RANK() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(CUME_DIST() OVER(win1),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product,vendor,customer WHERE sale_ord.pn=product.pn AND sale_ord.vn=vendor.vn AND sale_ord.cn=customer.cn) sale
WINDOW win1 as (partition by sale.pn,sale.vn order by sale.ord, sale.pn asc); -- mvd 3,1->2; 3,1->4; 3,1->5; 3,1->6; 3,1->7;

SELECT * FROM view_olap_window6;

-- LEAD() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window7(col1,col2,col3,col4,col5,col6,col7) AS SELECT sale.qty,sale.cn, TO_CHAR(COALESCE(LEAD(cast(floor(sale.vn) as int),cast (floor(sale.cn/sale.qty) as  int),NULL) OVER(win1),0),'99999999.9999999'),sale.vn,sale.pn,
TO_CHAR(COALESCE(RANK() OVER(partition by sale.vn order by sale.ord, sale.cn desc,sale.pn desc),0),'99999999.9999999'),
TO_CHAR(COALESCE(LEAD(cast(floor(sale.qty) as int),cast (floor(sale.vn) as int),NULL) OVER(win2),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord) sale
WINDOW win1 as (partition by sale.vn order by sale.ord, sale.cn desc,sale.pn desc),
win2 as (order by sale.ord, sale.vn asc); -- mvd 2,4,5->3; 2,4,5->6; 4->7;

SELECT * FROM view_olap_window7;
-- COUNT() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window8(col1,col2,col3,col4,col5,col6,col7,col8,col9) AS SELECT sale.dt,sale.cn,sale.dt, TO_CHAR(COALESCE(COUNT(floor(sale.cn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(DENSE_RANK() OVER(win2),0),'99999999.9999999'),sale.qty,sale.pn,
TO_CHAR(COALESCE(LAG(cast(floor(sale.cn) as int),cast (floor(sale.vn) as int),NULL) OVER(win3),0),'99999999.9999999'),sale.vn
FROM (SELECT sale_ord.* FROM sale_ord,product,vendor,customer WHERE sale_ord.pn=product.pn AND sale_ord.vn=vendor.vn AND sale_ord.cn=customer.cn) sale
WINDOW win1 as (),
win2 as (partition by sale.cn,sale.qty,sale.cn,sale.dt order by sale.pn asc),
win3 as (order by sale.ord, sale.vn asc); -- mvd 4->4; 1,2,6,7->5; 9->8;

SELECT * FROM view_olap_window8;
-- MAX() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window9(col1,col2,col3,col4,col5,col6,col7) AS SELECT sale.qty,sale.cn,sale.vn,sale.cn, TO_CHAR(COALESCE(MAX(floor(sale.cn)) OVER(win1),0),'99999999.9999999'),sale.pn,
TO_CHAR(COALESCE(PERCENT_RANK() OVER(partition by sale.vn,sale.pn,sale.pn,sale.cn,sale.vn order by sale.pn desc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,vendor WHERE sale_ord.vn=vendor.vn) sale
WINDOW win1 as (partition by sale.vn,sale.pn,sale.pn,sale.cn,sale.vn order by sale.pn desc); -- mvd 2,3,6->5; 2,3,6->7;

SELECT * FROM view_olap_window9;
-- STDDEV() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window10(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11) AS SELECT sale.pn,sale.vn, TO_CHAR(COALESCE(STDDEV(floor(sale.vn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.prc+sale.qty)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(DENSE_RANK() OVER(win2),0),'99999999.9999999'),sale.cn,
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win3),0),'99999999.9999999'),
TO_CHAR(COALESCE(RANK() OVER(win4),0),'99999999.9999999'),sale.prc,sale.dt,
TO_CHAR(COALESCE(MAX(floor(sale.cn)) OVER(win3),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product WHERE sale_ord.pn=product.pn) sale
WINDOW win1 as (),
win2 as (order by sale.cn asc),
win3 as (order by sale.pn asc),
win4 as (partition by sale.prc,sale.pn,sale.cn,sale.dt order by sale.pn desc); -- mvd 3->3; 3->4; 6->5; 1->7; 9,10,6,1->8; 1->11;

SELECT * FROM view_olap_window10;

-- STDDEV_POP() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window11(col1,col2,col3,col4,col5,col6,col7,col8,col9) AS SELECT sale.vn, TO_CHAR(COALESCE(STDDEV_POP(floor(sale.qty*sale.cn)) OVER(win1),0),'99999999.9999999'),sale.cn,sale.qty,
TO_CHAR(COALESCE(PERCENT_RANK() OVER(partition by sale.vn,sale.qty,sale.cn,sale.cn order by sale.vn desc,sale.vn desc,sale.cn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(CUME_DIST() OVER(partition by sale.vn,sale.qty,sale.cn,sale.cn order by sale.vn desc,sale.vn desc,sale.cn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.pn/sale.qty)) OVER(partition by sale.vn,sale.qty,sale.cn,sale.cn order by sale.vn desc,sale.vn desc,sale.cn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(DENSE_RANK() OVER(partition by sale.vn,sale.qty,sale.cn,sale.cn order by sale.vn desc,sale.vn desc,sale.cn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(RANK() OVER(partition by sale.vn,sale.qty,sale.cn,sale.cn order by sale.vn desc,sale.vn desc,sale.cn asc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,customer,vendor WHERE sale_ord.cn=customer.cn AND sale_ord.vn=vendor.vn) sale
WINDOW win1 as (partition by sale.vn,sale.qty,sale.cn,sale.cn order by sale.vn desc,sale.vn desc,sale.cn asc); 
-- mvd 3,4,1->2; 3,4,1->5; 3,4,1->6; 3,4,1->7; 3,4,1->8; 3,4,1->9;

SELECT * FROM view_olap_window11;
-- SUM() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window12(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) AS SELECT sale.vn,sale.prc, TO_CHAR(COALESCE(SUM(floor(sale.vn*sale.cn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win2),0),'99999999.9999999'),sale.cn,sale.dt,sale.qty,sale.pn,
TO_CHAR(COALESCE(MIN(floor(sale.cn+sale.vn)) OVER(),0),'99999999.9999999'),
TO_CHAR(COALESCE(LAG(cast(floor(sale.qty) as int),cast (floor(sale.qty) as int),NULL) OVER(win3),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.vn)) OVER(),0),'99999999.9999999'),
TO_CHAR(COALESCE(PERCENT_RANK() OVER(win4),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product,vendor,customer WHERE sale_ord.pn=product.pn AND sale_ord.vn=vendor.vn AND sale_ord.cn=customer.cn) sale
WINDOW win1 as (),
win2 as (partition by sale.vn,sale.dt,sale.vn,sale.qty,sale.cn,sale.prc order by sale.pn desc),
win3 as (partition by sale.qty order by sale.ord, sale.pn desc),
win4 as (partition by sale.dt,sale.pn,sale.cn order by sale.vn asc); -- mvd 3->3; 2,5,6,7,1,8->4; 3->9; 7,8->10; 3->11; 5,6,1,8->12;

SELECT * FROM view_olap_window12;
-- VAR_POP() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window13(col1,col2,col3,col4,col5,col6,col7,col8) AS SELECT sale.cn,sale.vn,sale.qty, TO_CHAR(COALESCE(VAR_POP(floor(sale.vn*sale.vn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(DENSE_RANK() OVER(win2),0),'99999999.9999999'),sale.pn,
TO_CHAR(COALESCE(PERCENT_RANK() OVER(win2),0),'99999999.9999999'),
TO_CHAR(COALESCE(RANK() OVER(win2),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product WHERE sale_ord.pn=product.pn) sale
WINDOW win1 as (),
win2 as (order by sale.pn desc); -- mvd 4->4; 6->5; 6->7; 6->8;

SELECT * FROM view_olap_window13;

-- VAR_SAMP() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window14(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) AS SELECT sale.vn,sale.prc,sale.dt,sale.pn,sale.qty,sale.qty, TO_CHAR(COALESCE(VAR_SAMP(floor(sale.qty/sale.cn)) OVER(win1),0),'99999999.9999999'),sale.cn,
TO_CHAR(COALESCE(CUME_DIST() OVER(partition by sale.pn,sale.pn,sale.pn order by sale.cn asc,sale.vn desc),0),'99999999.9999999'),
TO_CHAR(COALESCE(RANK() OVER(partition by sale.pn,sale.pn,sale.pn order by sale.cn asc,sale.vn desc),0),'99999999.9999999'),
TO_CHAR(COALESCE(PERCENT_RANK() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(PERCENT_RANK() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(DENSE_RANK() OVER(win1),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product,vendor WHERE sale_ord.pn=product.pn AND sale_ord.vn=vendor.vn) sale
WINDOW win1 as (partition by sale.pn,sale.pn,sale.pn order by sale.cn asc,sale.vn desc); -- mvd 8,1,4->7; 8,1,4->9; 8,1,4->10; 8,1,4->11; 8,1,4->12; 8,1,4->13;

SELECT * FROM view_olap_window14;
-- CORR() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window15(col1,col2,col3,col4,col5,col6,col7,col8) AS SELECT sale.pn,sale.pn, TO_CHAR(COALESCE(CORR(floor(sale.prc-sale.cn),floor(sale.cn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(LAG(cast(floor(sale.cn) as int),cast (floor(sale.prc+sale.pn) as int),NULL) OVER(win2),0),'99999999.9999999'),sale.dt,sale.cn,sale.vn,
TO_CHAR(COALESCE(RANK() OVER(partition by sale.cn,sale.vn,sale.vn,sale.cn,sale.pn,sale.dt order by sale.ord, sale.pn desc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord) sale
WINDOW win1 as (),
win2 as (partition by sale.cn,sale.vn,sale.vn,sale.cn,sale.pn,sale.dt order by sale.ord, sale.pn desc); -- mvd 3->3; 5,6,7,1->4; 5,6,7,1->8;

SELECT * FROM view_olap_window15;
-- COVAR_POP() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window16(col1,col2,col3,col4,col5,col6,col7,col8) AS SELECT sale.dt,sale.cn,sale.vn,sale.qty,sale.vn, TO_CHAR(COALESCE(COVAR_POP(floor(sale.pn+sale.pn),floor(sale.qty-sale.cn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.pn-sale.prc)) OVER(),0),'99999999.9999999'),
TO_CHAR(COALESCE(CUME_DIST() OVER(win2),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,customer,vendor WHERE sale_ord.cn=customer.cn AND sale_ord.vn=vendor.vn) sale
WINDOW win1 as (),
win2 as (order by sale.cn asc); -- mvd 6->6; 6->7; 2->8;

SELECT * FROM view_olap_window16;
-- COVAR_SAMP() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window17(col1,col2,col3,col4,col5,col6,col7,col8,col9) AS SELECT sale.vn,sale.pn, TO_CHAR(COALESCE(COVAR_SAMP(floor(sale.vn/sale.qty),floor(sale.cn)) OVER(win1),0),'99999999.9999999'),sale.prc,sale.cn,sale.dt,
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(LEAD(cast(floor(sale.pn) as int),cast (floor(sale.qty+sale.qty) as int),NULL) OVER(win2),0),'99999999.9999999'),sale.qty
FROM (SELECT sale_ord.* FROM sale_ord,product,vendor,customer WHERE sale_ord.pn=product.pn AND sale_ord.vn=vendor.vn AND sale_ord.cn=customer.cn) sale
WINDOW win1 as (partition by sale.prc,sale.vn,sale.dt,sale.cn,sale.prc order by sale.cn asc),
win2 as (partition by sale.cn,sale.cn,sale.qty,sale.prc order by sale.ord, sale.cn asc); -- mvd 4,5,6,1->3; 4,5,6,1->7; 4,5,9->8;

SELECT * FROM view_olap_window17;
-- REGR_AVGX() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window18(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10) AS SELECT sale.vn,sale.cn,sale.dt,sale.pn,sale.pn,sale.prc, TO_CHAR(COALESCE(REGR_AVGX(floor(sale.vn*sale.vn),floor(sale.pn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(RANK() OVER(win2),0),'99999999.9999999'),
TO_CHAR(COALESCE(PERCENT_RANK() OVER(win3),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(partition by sale.cn order by sale.pn desc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord) sale
WINDOW win1 as (),
win2 as (order by sale.vn desc),
win3 as (partition by sale.cn order by sale.pn desc); -- mvd 7->7; 1->8; 2,4->9; 2,4->10;

SELECT * FROM view_olap_window18;
-- REGR_AVGY() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window19(col1,col2,col3,col4,col5,col6,col7,col8) AS SELECT sale.vn,sale.dt, TO_CHAR(COALESCE(REGR_AVGY(floor(sale.qty+sale.cn),floor(sale.prc)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(CUME_DIST() OVER(partition by sale.vn,sale.dt order by sale.vn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.cn/sale.qty)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.qty-sale.vn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(partition by sale.vn,sale.dt order by sale.vn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(floor(sale.prc+sale.vn)) OVER(partition by sale.vn,sale.dt order by sale.vn asc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,customer WHERE sale_ord.cn=customer.cn ) sale
WINDOW win1 as (partition by sale.vn,sale.dt order by sale.vn asc); -- mvd 2,1->3; 2,1->4; 2,1->5; 2,1->6; 2,1->7; 2,1->8;

SELECT * FROM view_olap_window19;

-- REGR_COUNT() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window20(col1,col2,col3,col4,col5,col6,col7) AS SELECT sale.qty,sale.cn,sale.vn,sale.vn,sale.cn, TO_CHAR(COALESCE(REGR_COUNT(floor(sale.pn+sale.prc),floor(sale.qty)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win2),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,customer,product WHERE sale_ord.cn=customer.cn AND sale_ord.pn=product.pn) sale
WINDOW win1 as (),
win2 as (order by sale.cn desc); -- mvd 6->6; 2->7;

SELECT * FROM view_olap_window20;
-- REGR_INTERCEPT() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window21(col1,col2,col3,col4,col5,col6,col7,col8,col9) AS SELECT sale.vn,sale.cn,sale.pn,sale.cn,sale.vn,sale.qty, TO_CHAR(COALESCE(REGR_INTERCEPT(floor(sale.vn+sale.cn),floor(sale.vn+sale.prc)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(win2),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(order by sale.cn asc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product WHERE sale_ord.pn=product.pn) sale
WINDOW win1 as (),
win2 as (order by sale.cn asc); -- mvd 7->7; 2->8; 2->9;

SELECT * FROM view_olap_window21;
-- REGR_R2() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window22(col1,col2,col3,col4,col5) AS SELECT sale.qty, TO_CHAR(COALESCE(REGR_R2(floor(sale.pn),floor(sale.prc-sale.pn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.pn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(PERCENT_RANK() OVER(win2),0),'99999999.9999999'),sale.cn
FROM (SELECT sale_ord.* FROM sale_ord) sale
WINDOW win1 as (),
win2 as (order by sale.cn asc); -- mvd 2->2; 2->3; 5->4;

SELECT * FROM view_olap_window22;
-- REGR_SLOPE() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window23(col1,col2,col3,col4,col5,col6,col7) AS SELECT sale.pn, TO_CHAR(COALESCE(REGR_SLOPE(floor(sale.prc-sale.prc),floor(sale.cn/sale.vn)) OVER(win1),0),'99999999.9999999'),sale.cn,sale.vn,sale.qty,
TO_CHAR(COALESCE(PERCENT_RANK() OVER(partition by sale.qty order by sale.cn desc,sale.pn desc,sale.vn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(MIN(floor(sale.cn)) OVER(win1),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product WHERE sale_ord.pn=product.pn) sale
WINDOW win1 as (partition by sale.qty order by sale.cn desc,sale.pn desc,sale.vn asc); -- mvd 3,4,5,1->2; 3,4,5,1->6; 3,4,5,1->7;

SELECT * FROM view_olap_window23;
-- REGR_SXX() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window24(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) AS SELECT sale.dt,sale.qty, TO_CHAR(COALESCE(REGR_SXX(floor(sale.pn),floor(sale.pn)) OVER(win1),0),'99999999.9999999'),sale.prc,sale.cn,sale.vn,sale.pn,
TO_CHAR(COALESCE(CUME_DIST() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(LAG(cast(floor(sale.vn+sale.prc) as int),cast (floor(sale.pn+sale.qty) as int),NULL) OVER(win2),0),'99999999.9999999'),
TO_CHAR(COALESCE(LEAD(cast(floor(sale.qty) as int),cast (floor(sale.qty) as int),NULL) OVER(win3),0),'99999999.9999999'),
TO_CHAR(COALESCE(RANK() OVER(partition by sale.pn,sale.prc,sale.qty order by sale.ord, sale.pn desc),0),'99999999.9999999'),
TO_CHAR(COALESCE(ROW_NUMBER() OVER(partition by sale.pn,sale.prc,sale.qty order by sale.ord, sale.pn desc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,customer WHERE sale_ord.cn=customer.cn ) sale
WINDOW win1 as (partition by sale.qty,sale.qty,sale.vn,sale.prc,sale.dt,sale.dt order by sale.vn asc,sale.cn desc,sale.pn asc),
win2 as (order by sale.ord, sale.pn desc),
win3 as (partition by sale.pn,sale.prc,sale.qty order by sale.ord, sale.pn desc); -- mvd 4,5,1,6,2,7->3; 4,5,1,6,2,7->8; 7->9; 4,2,7->10; 4,2,7->11; 4,2,7->12;

SELECT * FROM view_olap_window24;
-- REGR_SXY() function with NULL OVER() clause in combination with other window functions --

CREATE VIEW view_olap_window25(col1,col2,col3,col4,col5,col6,col7,col8,col9) AS SELECT sale.cn,sale.pn,sale.prc, TO_CHAR(COALESCE(REGR_SXY(floor(sale.vn),floor(sale.vn*sale.cn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(PERCENT_RANK() OVER(win2),0),'99999999.9999999'),sale.dt,sale.qty,
TO_CHAR(COALESCE(CUME_DIST() OVER(win3),0),'99999999.9999999'),sale.vn
FROM (SELECT sale_ord.* FROM sale_ord,product WHERE sale_ord.pn=product.pn) sale
WINDOW win1 as (),
win2 as (partition by sale.dt,sale.qty order by sale.pn asc),
win3 as (partition by sale.pn order by sale.vn asc); -- mvd 4->4; 6,7,2->5; 9,2->8;

SELECT * FROM view_olap_window25;
-- REGR_SYY() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window26(col1,col2,col3,col4,col5,col6,col7,col8) AS SELECT sale.vn,sale.vn, TO_CHAR(COALESCE(REGR_SYY(floor(sale.pn),floor(sale.cn)) OVER(win1),0),'99999999.9999999'),sale.cn,sale.pn,
TO_CHAR(COALESCE(ROW_NUMBER() OVER(partition by sale.pn,sale.vn,sale.pn order by sale.cn desc,sale.pn desc,sale.cn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(DENSE_RANK() OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(PERCENT_RANK() OVER(win1),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,vendor WHERE sale_ord.vn=vendor.vn) sale
WINDOW win1 as (partition by sale.pn,sale.vn,sale.pn order by sale.cn desc,sale.pn desc,sale.cn asc); -- mvd 4,1,5->3; 4,1,5->6; 4,1,5->7; 4,1,5->8;

SELECT * FROM view_olap_window26;
-- FIRST_VALUE() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window27(col1,col2,col3,col4,col5,col6,col7) AS SELECT sale.vn, TO_CHAR(COALESCE(FIRST_VALUE(floor(sale.cn/sale.cn)) OVER(win1),0),'99999999.9999999'),sale.cn,sale.dt,sale.pn,
TO_CHAR(COALESCE(FIRST_VALUE(floor(sale.cn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(FIRST_VALUE(floor(sale.qty+sale.cn)) OVER(partition by sale.dt,sale.vn,sale.pn,sale.cn order by sale.ord, sale.pn asc,sale.cn asc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product WHERE sale_ord.pn=product.pn) sale
WINDOW win1 as (partition by sale.dt,sale.vn,sale.pn,sale.cn order by sale.ord, sale.pn asc,sale.cn asc); -- mvd 3,4,1,5->2; 3,4,1,5->6; 3,4,1,5->7;

SELECT * FROM view_olap_window27;

-- LAST_VALUE() function with OVER() clause having PARTITION BY and ORDER BY (without framing) in combination with other window functions --

CREATE VIEW view_olap_window28(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) AS SELECT sale.pn,sale.pn,sale.vn, TO_CHAR(COALESCE(LAST_VALUE(floor(sale.vn)) OVER(win1),0),'99999999.9999999'),sale.prc,sale.cn,sale.qty,
TO_CHAR(COALESCE(FIRST_VALUE(floor(sale.cn)) OVER(partition by sale.prc,sale.qty order by sale.ord, sale.vn asc,sale.cn desc),0),'99999999.9999999'),
TO_CHAR(COALESCE(LAST_VALUE(floor(sale.vn*sale.cn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(FIRST_VALUE(floor(sale.cn+sale.qty)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(FIRST_VALUE(floor(sale.pn)) OVER(partition by sale.prc,sale.qty order by sale.ord, sale.vn asc,sale.cn desc),0),'99999999.9999999'),
TO_CHAR(COALESCE(FIRST_VALUE(floor(sale.cn)) OVER(partition by sale.prc,sale.qty order by sale.ord, sale.vn asc,sale.cn desc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,product,vendor WHERE sale_ord.pn=product.pn AND sale_ord.vn=vendor.vn) sale
WINDOW win1 as (partition by sale.prc,sale.qty order by sale.ord, sale.vn asc,sale.cn desc); 
-- mvd 5,6,3,7->4; 5,6,3,7->8; 5,6,3,7->9; 5,6,3,7->10; 5,6,3,7->11; 5,6,3,7->12;

SELECT * FROM view_olap_window28;
-- VARIANCE() function with partition by and order by having rows based framing clause in combination with other functions --

CREATE VIEW view_olap_window29(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11) AS SELECT sale.vn,sale.cn, TO_CHAR(COALESCE(VARIANCE(floor(sale.prc+sale.cn)) OVER(win1),0),'99999999.9999999'),sale.dt,sale.pn,
TO_CHAR(COALESCE(DENSE_RANK() OVER(win2),0),'99999999.9999999'),
TO_CHAR(COALESCE(LAG(cast(floor(sale.vn+sale.cn) as int),cast (floor(sale.qty) as int),NULL) OVER(win3),0),'99999999.9999999'),sale.qty,
TO_CHAR(COALESCE(COUNT(floor(sale.vn)) OVER(partition by sale.qty order by sale.ord, sale.vn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.cn-sale.pn)) OVER(partition by sale.qty order by sale.ord, sale.vn asc),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(floor(sale.cn)) OVER(partition by sale.qty order by sale.ord, sale.vn asc),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,customer,product WHERE sale_ord.cn=customer.cn AND sale_ord.pn=product.pn) sale
WINDOW win1 as (partition by sale.vn,sale.dt,sale.cn,sale.vn order by sale.ord, sale.pn desc,sale.cn asc,sale.cn desc rows unbounded preceding ),
win2 as (order by sale.pn desc),
win3 as (partition by sale.qty order by sale.ord, sale.vn asc); -- mvd 2,4,1,5->3; 5->6; 1,8->7; 1,8->9; 1,8->10; 1,8->11;

SELECT * FROM view_olap_window29;
-- LAST_VALUE() function with partition by and order by sale.ord, having rows based framing clause in combination with other functions --

CREATE VIEW view_olap_window30(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10) AS SELECT sale.vn,sale.qty,sale.dt,sale.pn, TO_CHAR(COALESCE(LAST_VALUE(floor(sale.vn+sale.qty)) OVER(win1),0),'99999999.9999999'),sale.prc,
TO_CHAR(COALESCE(LAST_VALUE(floor(sale.qty)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(LAST_VALUE(floor(sale.prc)) OVER(partition by sale.prc order by sale.ord, sale.ord, sale.pn desc rows between current row and unbounded following ),0),'99999999.9999999'),
TO_CHAR(COALESCE(FIRST_VALUE(floor(sale.prc+sale.pn)) OVER(win1),0),'99999999.9999999'),
TO_CHAR(COALESCE(LAST_VALUE(floor(sale.vn*sale.prc)) OVER(win1),0),'99999999.9999999')
FROM (SELECT sale_ord.* FROM sale_ord,customer WHERE sale_ord.cn=customer.cn ) sale
WINDOW win1 as (partition by sale.prc order by sale.ord, sale.ord, sale.pn desc rows between current row and unbounded following );
 -- mvd 6,4->5; 6,4->7; 6,4->8; 6,4->9; 6,4->10;

SELECT * FROM view_olap_window30;
