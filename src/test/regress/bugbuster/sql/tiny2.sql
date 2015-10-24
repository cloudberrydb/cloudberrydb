drop table if exists t5;
drop table if exists t7;
drop table if exists t15;
drop table if exists t20;
drop table if exists t23;
drop table if exists t27;
drop table if exists t28;
drop table if exists t36;
drop table if exists t48;
drop table if exists t51;
drop table if exists t58;
drop table if exists t65;
drop table if exists t70;
drop table if exists t76;
drop table if exists t77;
drop table if exists t79;
drop table if exists t85;
drop table if exists t88;
drop table if exists t89;
drop table if exists t90;
drop table if exists t91;
-- Test: tiny-aggs_schmea-1

create table T5(
	C66 int,
	C67 int,
	C68 int,
	C69 int,
	C70 int,
	C71 int,
	C72 int,
	C73 int,
	C74 int);

create table T7(
	C89 int,
	C90 int,
	C91 int,
	C92 int,
	C93 int,
	C94 int,
	C95 int,
	C96 int,
	C97 int,
	C98 int,
	C99 int,
	C100 int,
	C101 int,
	C102 int,
	C103 int,
	C104 int,
	C105 int,
	C106 int,
	C107 int,
	C108 int,
	C109 int);

create table T15(
	C186 int,
	C187 int,
	C188 int,
	C189 int,
	C190 int,
	C191 int,
	C192 int,
	C193 int,
	C194 int,
	C195 int,
	C196 int,
	C197 int,
	C198 int,
	C199 int);

create table T20(
	C233 int,
	C234 int,
	C235 int,
	C236 int,
	C237 int,
	C238 int,
	C239 int,
	C240 int,
	C241 int,
	C242 int,
	C243 int,
	C244 int,
	C245 int,
	C246 int);

create table T23(
	C270 int,
	C271 int,
	C272 int,
	C273 int,
	C274 int,
	C275 int,
	C276 int,
	C277 int,
	C278 int);

create table T27(
	C319 int,
	C320 int,
	C321 int,
	C322 int,
	C323 int,
	C324 int,
	C325 int,
	C326 int,
	C327 int);

create table T28(
	C328 int,
	C329 int,
	C330 int,
	C331 int,
	C332 int,
	C333 int);

create table T36(
	C419 int,
	C420 int,
	C421 int,
	C422 int,
	C423 int,
	C424 int,
	C425 int,
	C426 int,
	C427 int,
	C428 int,
	C429 int,
	C430 int,
	C431 int,
	C432 int,
	C433 int,
	C434 int,
	C435 int,
	C436 int,
	C437 int);

create table T48(
	C568 int,
	C569 int,
	C570 int,
	C571 int,
	C572 int,
	C573 int,
	C574 int,
	C575 int,
	C576 int,
	C577 int,
	C578 int,
	C579 int,
	C580 int,
	C581 int,
	C582 int,
	C583 int,
	C584 int,
	C585 int);

create table T51(
	C608 int,
	C609 int,
	C610 int,
	C611 int,
	C612 int,
	C613 int,
	C614 int,
	C615 int,
	C616 int,
	C617 int,
	C618 int,
	C619 int,
	C620 int,
	C621 int,
	C622 int,
	C623 int,
	C624 int);

create table T58(
	C698 int,
	C699 int,
	C700 int,
	C701 int,
	C702 int,
	C703 int,
	C704 int,
	C705 int,
	C706 int,
	C707 int,
	C708 int,
	C709 int,
	C710 int,
	C711 int,
	C712 int);

create table T65(
	C783 int,
	C784 int,
	C785 int,
	C786 int,
	C787 int,
	C788 int,
	C789 int,
	C790 int,
	C791 int,
	C792 int,
	C793 int,
	C794 int,
	C795 int,
	C796 int,
	C797 int);

create table T70(
	C839 int,
	C840 int,
	C841 int,
	C842 int,
	C843 int,
	C844 int,
	C845 int,
	C846 int,
	C847 int,
	C848 int,
	C849 int,
	C850 int,
	C851 int,
	C852 int,
	C853 int,
	C854 int,
	C855 int,
	C856 int,
	C857 int,
	C858 int,
	C859 int);

create table T76(
	C938 int,
	C939 int,
	C940 int,
	C941 int,
	C942 int,
	C943 int,
	C944 int,
	C945 int,
	C946 int);

create table T77(
	C947 int,
	C948 int,
	C949 int,
	C950 int,
	C951 int,
	C952 int,
	C953 int,
	C954 int,
	C955 int,
	C956 int,
	C957 int,
	C958 int,
	C959 int,
	C960 int,
	C961 int,
	C962 int,
	C963 int,
	C964 int);

create table T79(
	C979 int,
	C980 int,
	C981 int,
	C982 int,
	C983 int,
	C984 int,
	C985 int);

create table T85(
	C1029 int,
	C1030 int,
	C1031 int,
	C1032 int,
	C1033 int,
	C1034 int,
	C1035 int,
	C1036 int,
	C1037 int,
	C1038 int,
	C1039 int,
	C1040 int,
	C1041 int,
	C1042 int,
	C1043 int,
	C1044 int,
	C1045 int,
	C1046 int);

create table T88(
	C1064 int,
	C1065 int,
	C1066 int,
	C1067 int,
	C1068 int,
	C1069 int,
	C1070 int,
	C1071 int,
	C1072 int,
	C1073 int,
	C1074 int,
	C1075 int);

create table T89(
	C1076 int,
	C1077 int,
	C1078 int);

create table T90(
	C1079 int,
	C1080 int,
	C1081 int,
	C1082 int,
	C1083 int,
	C1084 int,
	C1085 int,
	C1086 int,
	C1087 int,
	C1088 int,
	C1089 int,
	C1090 int,
	C1091 int,
	C1092 int);

create table T91(
	C1093 int,
	C1094 int,
	C1095 int,
	C1096 int,
	C1097 int,
	C1098 int,
	C1099 int,
	C1100 int,
	C1101 int,
	C1102 int,
	C1103 int,
	C1104 int,
	C1105 int,
	C1106 int,
	C1107 int,
	C1108 int,
	C1109 int,
	C1110 int,
	C1111 int,
	C1112 int,
	C1113 int,
	C1114 int);

SELECT 1;



-- Test: tiny-aggs_data_T5
INSERT INTO T5 VALUES ( 1, 4, 1, 3, 2, 5, 4, 5, 2 );

INSERT INTO T5 VALUES ( 4, 2, 5, 2, 2, 3, 3, 2, 5 );

INSERT INTO T5 VALUES ( 4, 3, 5, 4, 3, 4, 5, 1, 2 );

INSERT INTO T5 VALUES ( 3, 4, 2, 3, 1, 5, 3, 4, 5 );

INSERT INTO T5 VALUES ( 1, 3, 2, 1, 5, 4, 2, 4, 5 );

INSERT INTO T5 VALUES ( 2, 1, 3, 2, 1, 3, 5, 5, 4 );

INSERT INTO T5 VALUES ( 5, 4, 4, 4, 2, 3, 4, 3, 1 );

INSERT INTO T5 VALUES ( 3, 5, 4, 5, 2, 2, 5, 2, 1 );

INSERT INTO T5 VALUES ( 5, 4, 5, 5, 2, 3, 1, 4, 4 );

INSERT INTO T5 VALUES ( 1, 3, 1, 4, 1, 5, 5, 5, 3 );

SELECT 1;


-- Test: tiny-aggs_data_T7
INSERT INTO T7 VALUES ( 1, 2, 4, 5, 5, 3, 1, 3, 2, 3, 3, 3, 2, 5, 5, 5, 3, 3, 3, 2, 5 );

INSERT INTO T7 VALUES ( 4, 2, 2, 1, 3, 5, 5, 3, 5, 4, 4, 3, 3, 1, 1, 4, 3, 4, 2, 4, 5 );

INSERT INTO T7 VALUES ( 2, 2, 1, 3, 5, 5, 5, 2, 1, 2, 1, 5, 5, 4, 2, 1, 4, 4, 5, 3, 5 );

INSERT INTO T7 VALUES ( 3, 1, 4, 1, 4, 2, 2, 2, 4, 2, 1, 2, 1, 1, 5, 4, 3, 5, 1, 2, 2 );

INSERT INTO T7 VALUES ( 3, 1, 2, 3, 3, 5, 1, 5, 3, 1, 5, 2, 2, 4, 4, 1, 5, 4, 2, 4, 3 );

INSERT INTO T7 VALUES ( 2, 1, 4, 1, 5, 4, 4, 1, 2, 1, 3, 4, 2, 4, 5, 5, 4, 1, 4, 5, 3 );

INSERT INTO T7 VALUES ( 4, 3, 3, 2, 2, 5, 2, 5, 3, 4, 3, 3, 2, 5, 5, 1, 4, 2, 1, 3, 3 );

INSERT INTO T7 VALUES ( 4, 2, 5, 5, 3, 3, 5, 1, 4, 2, 1, 4, 5, 5, 3, 3, 2, 1, 2, 1, 1 );

INSERT INTO T7 VALUES ( 5, 5, 5, 1, 1, 4, 5, 3, 1, 2, 5, 5, 2, 2, 3, 1, 3, 2, 5, 2, 4 );

INSERT INTO T7 VALUES ( 3, 5, 5, 5, 4, 5, 2, 2, 2, 3, 3, 3, 5, 1, 1, 5, 3, 4, 3, 1, 3 );

SELECT 1;


-- Test: tiny-aggs_data_T15
INSERT INTO T15 VALUES ( 1, 1, 4, 3, 4, 1, 5, 5, 5, 3, 1, 1, 1, 2 );

INSERT INTO T15 VALUES ( 1, 2, 5, 5, 3, 1, 2, 1, 5, 1, 5, 4, 4, 1 );

INSERT INTO T15 VALUES ( 5, 4, 1, 3, 5, 3, 5, 2, 2, 2, 2, 1, 3, 1 );

INSERT INTO T15 VALUES ( 1, 3, 4, 4, 5, 4, 5, 1, 2, 4, 3, 2, 1, 5 );

INSERT INTO T15 VALUES ( 5, 5, 5, 3, 2, 3, 4, 1, 1, 4, 5, 1, 4, 1 );

INSERT INTO T15 VALUES ( 2, 4, 2, 3, 3, 3, 4, 2, 5, 1, 5, 3, 4, 2 );

INSERT INTO T15 VALUES ( 4, 3, 2, 4, 3, 5, 5, 4, 4, 1, 2, 1, 1, 4 );

INSERT INTO T15 VALUES ( 4, 3, 2, 1, 5, 1, 3, 1, 1, 4, 1, 1, 4, 2 );

INSERT INTO T15 VALUES ( 4, 4, 1, 4, 5, 4, 5, 3, 2, 2, 1, 3, 3, 2 );

INSERT INTO T15 VALUES ( 4, 1, 4, 4, 4, 2, 2, 3, 3, 4, 3, 1, 5, 1 );

SELECT 1;


-- Test: tiny-aggs_data_T20
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

SELECT 1;


-- Test: tiny-aggs_data_T23
INSERT INTO T23 VALUES ( 5, 3, 1, 3, 2, 2, 2, 1, 3 );

INSERT INTO T23 VALUES ( 3, 3, 5, 2, 1, 1, 3, 1, 2 );

INSERT INTO T23 VALUES ( 2, 4, 3, 5, 1, 3, 3, 4, 1 );

INSERT INTO T23 VALUES ( 5, 5, 1, 4, 1, 2, 4, 3, 1 );

INSERT INTO T23 VALUES ( 3, 2, 4, 5, 5, 2, 3, 5, 1 );

INSERT INTO T23 VALUES ( 3, 5, 2, 2, 5, 1, 1, 5, 2 );

INSERT INTO T23 VALUES ( 3, 5, 2, 1, 1, 3, 5, 3, 2 );

INSERT INTO T23 VALUES ( 5, 3, 4, 2, 1, 5, 1, 1, 1 );

INSERT INTO T23 VALUES ( 5, 5, 3, 4, 1, 4, 1, 4, 3 );

INSERT INTO T23 VALUES ( 1, 4, 4, 1, 5, 2, 3, 1, 3 );

SELECT 1;


-- Test: tiny-aggs_data_T27
INSERT INTO T27 VALUES ( 2, 1, 4, 4, 4, 1, 1, 3, 4 );

INSERT INTO T27 VALUES ( 3, 1, 3, 3, 1, 2, 3, 1, 1 );

INSERT INTO T27 VALUES ( 5, 2, 1, 2, 4, 5, 3, 3, 5 );

INSERT INTO T27 VALUES ( 4, 3, 3, 3, 4, 5, 5, 1, 1 );

INSERT INTO T27 VALUES ( 3, 3, 2, 1, 2, 1, 3, 2, 3 );

INSERT INTO T27 VALUES ( 3, 3, 2, 4, 3, 2, 3, 5, 5 );

INSERT INTO T27 VALUES ( 5, 1, 3, 3, 5, 1, 2, 5, 3 );

INSERT INTO T27 VALUES ( 1, 4, 3, 4, 5, 5, 2, 4, 2 );

INSERT INTO T27 VALUES ( 5, 4, 1, 3, 5, 5, 3, 1, 4 );

INSERT INTO T27 VALUES ( 5, 3, 1, 5, 2, 3, 5, 1, 1 );

SELECT 1;


-- Test: tiny-aggs_data_T28
INSERT INTO T28 VALUES ( 3, 3, 3, 3, 1, 5 );

INSERT INTO T28 VALUES ( 1, 1, 1, 2, 3, 4 );

INSERT INTO T28 VALUES ( 2, 3, 4, 2, 3, 5 );

INSERT INTO T28 VALUES ( 4, 1, 3, 5, 4, 1 );

INSERT INTO T28 VALUES ( 5, 2, 2, 4, 2, 5 );

INSERT INTO T28 VALUES ( 2, 5, 5, 2, 3, 3 );

INSERT INTO T28 VALUES ( 4, 5, 1, 4, 5, 3 );

INSERT INTO T28 VALUES ( 2, 1, 5, 1, 1, 3 );

INSERT INTO T28 VALUES ( 5, 2, 3, 2, 3, 2 );

INSERT INTO T28 VALUES ( 2, 3, 3, 5, 4, 2 );

SELECT 1;


-- Test: tiny-aggs_data_T36
INSERT INTO T36 VALUES ( 3, 3, 3, 2, 5, 3, 3, 1, 4, 3, 5, 2, 4, 5, 1, 2, 3, 1, 2 );

INSERT INTO T36 VALUES ( 1, 1, 1, 3, 4, 5, 1, 5, 3, 2, 5, 2, 5, 5, 1, 1, 4, 5, 1 );

INSERT INTO T36 VALUES ( 1, 4, 5, 4, 5, 1, 5, 3, 3, 3, 5, 3, 2, 5, 1, 4, 5, 4, 2 );

INSERT INTO T36 VALUES ( 5, 2, 1, 3, 5, 4, 2, 4, 2, 5, 2, 4, 5, 3, 2, 2, 3, 4, 1 );

INSERT INTO T36 VALUES ( 3, 2, 5, 3, 1, 4, 3, 4, 2, 2, 1, 4, 1, 4, 4, 2, 5, 1, 1 );

INSERT INTO T36 VALUES ( 1, 2, 5, 2, 5, 4, 3, 4, 1, 5, 5, 1, 4, 5, 5, 1, 1, 4, 1 );

INSERT INTO T36 VALUES ( 2, 3, 4, 4, 4, 1, 5, 5, 1, 4, 1, 4, 2, 3, 2, 1, 5, 4, 4 );

INSERT INTO T36 VALUES ( 2, 1, 5, 2, 1, 2, 4, 3, 1, 1, 2, 4, 5, 1, 2, 5, 3, 1, 3 );

INSERT INTO T36 VALUES ( 1, 5, 2, 3, 4, 1, 4, 2, 5, 1, 4, 4, 4, 2, 2, 5, 2, 2, 2 );

INSERT INTO T36 VALUES ( 3, 5, 5, 4, 4, 2, 2, 4, 2, 4, 1, 4, 5, 1, 5, 3, 4, 4, 2 );

SELECT 1;


-- Test: tiny-aggs_data_T48
INSERT INTO T48 VALUES ( 1, 5, 2, 4, 2, 4, 4, 3, 4, 3, 5, 2, 1, 3, 2, 4, 3, 4 );

INSERT INTO T48 VALUES ( 3, 2, 1, 5, 4, 4, 3, 4, 2, 3, 3, 5, 1, 2, 3, 5, 2, 2 );

INSERT INTO T48 VALUES ( 3, 4, 4, 4, 1, 3, 2, 4, 2, 2, 5, 4, 5, 4, 5, 4, 1, 4 );

INSERT INTO T48 VALUES ( 3, 2, 1, 5, 2, 4, 5, 4, 1, 4, 2, 3, 1, 3, 4, 5, 5, 2 );

INSERT INTO T48 VALUES ( 4, 3, 1, 4, 5, 3, 3, 4, 2, 2, 3, 1, 1, 4, 1, 1, 4, 4 );

INSERT INTO T48 VALUES ( 4, 2, 3, 4, 3, 3, 3, 4, 4, 3, 1, 1, 3, 5, 3, 4, 3, 1 );

INSERT INTO T48 VALUES ( 2, 4, 1, 4, 4, 2, 4, 3, 1, 1, 5, 5, 1, 3, 4, 2, 5, 2 );

INSERT INTO T48 VALUES ( 5, 5, 3, 2, 4, 3, 1, 1, 2, 5, 4, 1, 4, 5, 5, 1, 5, 5 );

INSERT INTO T48 VALUES ( 1, 2, 3, 2, 4, 4, 4, 3, 5, 3, 5, 1, 1, 1, 3, 4, 4, 5 );

INSERT INTO T48 VALUES ( 4, 5, 5, 3, 3, 4, 2, 4, 2, 5, 3, 2, 2, 3, 5, 5, 4, 5 );

SELECT 1;


-- Test: tiny-aggs_data_T51
INSERT INTO T51 VALUES ( 4, 5, 2, 2, 3, 2, 3, 1, 1, 3, 5, 3, 1, 1, 5, 4, 4 );

INSERT INTO T51 VALUES ( 3, 1, 3, 4, 5, 4, 3, 2, 4, 5, 5, 5, 2, 3, 3, 5, 4 );

INSERT INTO T51 VALUES ( 4, 1, 2, 5, 5, 4, 4, 1, 1, 3, 3, 5, 2, 2, 4, 5, 5 );

INSERT INTO T51 VALUES ( 2, 3, 5, 5, 4, 1, 3, 2, 2, 1, 1, 3, 2, 3, 5, 4, 2 );

INSERT INTO T51 VALUES ( 4, 4, 3, 4, 3, 5, 1, 3, 1, 1, 1, 1, 2, 5, 5, 3, 5 );

INSERT INTO T51 VALUES ( 4, 2, 3, 1, 1, 1, 5, 2, 3, 3, 4, 5, 5, 5, 3, 2, 1 );

INSERT INTO T51 VALUES ( 3, 5, 5, 1, 5, 1, 4, 3, 4, 2, 3, 1, 1, 3, 1, 1, 2 );

INSERT INTO T51 VALUES ( 1, 1, 2, 3, 3, 1, 5, 5, 2, 1, 4, 3, 4, 5, 1, 3, 5 );

INSERT INTO T51 VALUES ( 1, 1, 3, 2, 5, 2, 2, 1, 5, 3, 1, 3, 3, 1, 3, 3, 4 );

INSERT INTO T51 VALUES ( 1, 1, 5, 2, 1, 4, 4, 3, 4, 1, 1, 2, 1, 2, 4, 4, 4 );

SELECT 1;


-- Test: tiny-aggs_data_T58
INSERT INTO T58 VALUES ( 1, 1, 4, 4, 3, 5, 5, 3, 1, 1, 1, 5, 1, 3, 1 );

INSERT INTO T58 VALUES ( 4, 2, 2, 5, 4, 5, 4, 3, 4, 4, 4, 3, 1, 3, 2 );

INSERT INTO T58 VALUES ( 1, 5, 3, 5, 4, 3, 5, 2, 3, 3, 5, 1, 3, 2, 5 );

INSERT INTO T58 VALUES ( 1, 2, 2, 5, 4, 2, 1, 1, 1, 1, 4, 3, 1, 3, 1 );

INSERT INTO T58 VALUES ( 1, 2, 5, 5, 3, 3, 1, 2, 4, 2, 5, 3, 1, 2, 4 );

INSERT INTO T58 VALUES ( 3, 4, 4, 4, 2, 5, 4, 3, 1, 2, 3, 2, 5, 2, 4 );

INSERT INTO T58 VALUES ( 4, 5, 3, 1, 4, 4, 2, 1, 1, 2, 1, 5, 1, 5, 4 );

INSERT INTO T58 VALUES ( 3, 4, 4, 1, 1, 5, 3, 5, 1, 2, 5, 5, 4, 3, 1 );

INSERT INTO T58 VALUES ( 3, 3, 3, 5, 2, 3, 1, 5, 5, 4, 4, 2, 2, 2, 4 );

INSERT INTO T58 VALUES ( 4, 4, 3, 5, 3, 3, 1, 1, 4, 2, 4, 4, 3, 2, 2 );

SELECT 1;


-- Test: tiny-aggs_data_T65
INSERT INTO T65 VALUES ( 1, 4, 3, 3, 5, 1, 1, 2, 5, 5, 5, 4, 5, 2, 4 );

INSERT INTO T65 VALUES ( 2, 5, 3, 3, 3, 3, 5, 4, 4, 1, 2, 5, 5, 5, 4 );

INSERT INTO T65 VALUES ( 3, 3, 1, 2, 3, 2, 4, 3, 2, 2, 5, 5, 5, 5, 3 );

INSERT INTO T65 VALUES ( 1, 5, 5, 3, 3, 1, 2, 2, 1, 5, 1, 1, 2, 4, 5 );

INSERT INTO T65 VALUES ( 4, 4, 5, 3, 3, 4, 2, 2, 5, 4, 4, 2, 2, 5, 1 );

INSERT INTO T65 VALUES ( 5, 1, 1, 1, 3, 1, 2, 2, 5, 1, 1, 4, 1, 4, 2 );

INSERT INTO T65 VALUES ( 3, 5, 4, 5, 1, 4, 3, 1, 4, 3, 3, 1, 3, 1, 4 );

INSERT INTO T65 VALUES ( 1, 1, 5, 1, 2, 1, 3, 1, 4, 5, 2, 3, 1, 2, 3 );

INSERT INTO T65 VALUES ( 3, 5, 3, 4, 3, 5, 5, 4, 4, 1, 5, 4, 2, 4, 4 );

INSERT INTO T65 VALUES ( 5, 3, 3, 2, 2, 3, 1, 5, 2, 1, 4, 5, 5, 4, 2 );

SELECT 1;


-- Test: tiny-aggs_data_T70
INSERT INTO T70 VALUES ( 3, 4, 4, 5, 4, 4, 1, 3, 3, 4, 4, 1, 5, 5, 4, 5, 3, 5, 2, 3, 4 );

INSERT INTO T70 VALUES ( 1, 3, 3, 5, 3, 2, 3, 1, 1, 2, 1, 4, 1, 1, 1, 5, 2, 4, 4, 5, 3 );

INSERT INTO T70 VALUES ( 3, 3, 5, 5, 2, 4, 5, 2, 4, 4, 1, 1, 2, 1, 5, 5, 5, 3, 4, 3, 1 );

INSERT INTO T70 VALUES ( 2, 3, 2, 2, 4, 1, 1, 1, 5, 4, 3, 4, 5, 1, 2, 4, 4, 5, 5, 1, 5 );

INSERT INTO T70 VALUES ( 4, 5, 2, 1, 2, 4, 2, 1, 1, 1, 2, 4, 3, 1, 5, 4, 2, 2, 1, 3, 4 );

INSERT INTO T70 VALUES ( 5, 1, 2, 5, 2, 2, 3, 2, 1, 1, 5, 5, 4, 1, 1, 2, 1, 5, 5, 2, 5 );

INSERT INTO T70 VALUES ( 4, 1, 5, 2, 5, 4, 4, 1, 4, 4, 2, 1, 3, 5, 2, 2, 4, 2, 4, 1, 1 );

INSERT INTO T70 VALUES ( 5, 3, 1, 5, 5, 5, 5, 2, 3, 2, 3, 5, 2, 2, 4, 5, 2, 4, 3, 4, 2 );

INSERT INTO T70 VALUES ( 4, 5, 2, 1, 3, 5, 5, 5, 1, 1, 5, 1, 4, 1, 3, 5, 5, 1, 2, 2, 3 );

INSERT INTO T70 VALUES ( 2, 2, 2, 1, 3, 3, 1, 2, 5, 3, 2, 3, 1, 3, 2, 3, 4, 5, 2, 2, 3 );

SELECT 1;


-- Test: tiny-aggs_data_T76
INSERT INTO T76 VALUES ( 5, 5, 2, 4, 5, 1, 2, 5, 3 );

INSERT INTO T76 VALUES ( 2, 1, 5, 2, 1, 5, 1, 2, 3 );

INSERT INTO T76 VALUES ( 4, 1, 1, 3, 4, 4, 5, 5, 5 );

INSERT INTO T76 VALUES ( 4, 4, 2, 2, 1, 4, 4, 2, 3 );

INSERT INTO T76 VALUES ( 5, 2, 5, 1, 4, 3, 3, 5, 4 );

INSERT INTO T76 VALUES ( 2, 4, 3, 1, 1, 1, 1, 2, 4 );

INSERT INTO T76 VALUES ( 1, 2, 1, 1, 4, 4, 3, 3, 4 );

INSERT INTO T76 VALUES ( 4, 4, 3, 1, 4, 2, 2, 2, 2 );

INSERT INTO T76 VALUES ( 1, 3, 1, 2, 1, 2, 4, 2, 1 );

INSERT INTO T76 VALUES ( 2, 2, 4, 3, 5, 1, 3, 5, 2 );

SELECT 1;


-- Test: tiny-aggs_data_T77
INSERT INTO T77 VALUES ( 5, 1, 4, 4, 5, 2, 2, 2, 5, 5, 5, 3, 3, 4, 4, 3, 1, 2 );

INSERT INTO T77 VALUES ( 5, 1, 3, 5, 2, 2, 5, 2, 4, 5, 5, 4, 5, 3, 3, 2, 1, 2 );

INSERT INTO T77 VALUES ( 2, 5, 4, 2, 5, 5, 3, 5, 4, 3, 2, 5, 4, 4, 3, 4, 4, 5 );

INSERT INTO T77 VALUES ( 2, 4, 5, 1, 3, 1, 5, 3, 2, 3, 4, 4, 1, 3, 3, 4, 5, 4 );

INSERT INTO T77 VALUES ( 2, 1, 5, 2, 3, 1, 2, 3, 5, 1, 5, 3, 3, 5, 4, 1, 5, 5 );

INSERT INTO T77 VALUES ( 5, 5, 2, 5, 5, 5, 2, 4, 3, 5, 5, 3, 1, 5, 1, 3, 3, 4 );

INSERT INTO T77 VALUES ( 1, 2, 2, 2, 5, 5, 4, 5, 5, 5, 1, 2, 3, 3, 2, 5, 1, 2 );

INSERT INTO T77 VALUES ( 4, 2, 3, 3, 1, 5, 1, 3, 5, 4, 1, 1, 4, 1, 5, 3, 2, 5 );

INSERT INTO T77 VALUES ( 5, 1, 3, 2, 5, 3, 5, 2, 2, 3, 4, 2, 5, 4, 4, 4, 5, 4 );

INSERT INTO T77 VALUES ( 1, 5, 2, 5, 2, 5, 3, 1, 4, 4, 4, 4, 3, 1, 4, 4, 4, 4 );

SELECT 1;


-- Test: tiny-aggs_data_T79
INSERT INTO T79 VALUES ( 2, 4, 2, 3, 4, 3, 5 );

INSERT INTO T79 VALUES ( 1, 3, 2, 5, 5, 1, 3 );

INSERT INTO T79 VALUES ( 3, 3, 3, 3, 3, 2, 1 );

INSERT INTO T79 VALUES ( 4, 2, 2, 1, 1, 5, 2 );

INSERT INTO T79 VALUES ( 2, 4, 1, 3, 5, 5, 5 );

INSERT INTO T79 VALUES ( 4, 1, 3, 3, 2, 5, 2 );

INSERT INTO T79 VALUES ( 3, 1, 4, 3, 2, 2, 2 );

INSERT INTO T79 VALUES ( 5, 4, 1, 4, 2, 1, 4 );

INSERT INTO T79 VALUES ( 5, 2, 1, 3, 2, 4, 4 );

INSERT INTO T79 VALUES ( 2, 3, 2, 4, 1, 5, 4 );

SELECT 1;


-- Test: tiny-aggs_data_T85
INSERT INTO T85 VALUES ( 2, 2, 4, 3, 1, 5, 5, 3, 1, 4, 2, 5, 3, 2, 5, 4, 3, 3 );

INSERT INTO T85 VALUES ( 2, 4, 4, 2, 4, 4, 4, 1, 1, 2, 1, 3, 2, 4, 5, 3, 1, 5 );

INSERT INTO T85 VALUES ( 5, 2, 2, 1, 2, 1, 1, 1, 1, 5, 2, 3, 4, 4, 5, 1, 5, 2 );

INSERT INTO T85 VALUES ( 3, 5, 1, 2, 4, 5, 2, 1, 5, 5, 3, 1, 3, 3, 3, 2, 1, 5 );

INSERT INTO T85 VALUES ( 4, 4, 3, 1, 2, 2, 4, 5, 4, 1, 2, 1, 3, 3, 1, 1, 3, 2 );

INSERT INTO T85 VALUES ( 1, 5, 3, 2, 2, 4, 4, 1, 1, 5, 3, 5, 2, 5, 2, 3, 3, 2 );

INSERT INTO T85 VALUES ( 3, 1, 3, 1, 3, 5, 2, 1, 5, 3, 5, 5, 5, 3, 1, 5, 5, 4 );

INSERT INTO T85 VALUES ( 1, 2, 2, 1, 1, 5, 1, 3, 2, 2, 2, 5, 3, 5, 3, 1, 4, 1 );

INSERT INTO T85 VALUES ( 4, 1, 5, 2, 5, 2, 4, 1, 4, 4, 5, 2, 3, 3, 3, 3, 5, 2 );

INSERT INTO T85 VALUES ( 1, 1, 3, 5, 4, 4, 5, 3, 3, 3, 2, 1, 2, 3, 2, 2, 1, 1 );

SELECT 1;


-- Test: tiny-aggs_data_T88
INSERT INTO T88 VALUES ( 3, 4, 1, 5, 5, 1, 4, 2, 1, 2, 4, 4 );

INSERT INTO T88 VALUES ( 2, 1, 1, 3, 1, 4, 1, 5, 5, 4, 2, 3 );

INSERT INTO T88 VALUES ( 5, 4, 3, 2, 1, 5, 5, 1, 4, 2, 1, 4 );

INSERT INTO T88 VALUES ( 5, 1, 3, 3, 2, 2, 1, 2, 5, 4, 1, 5 );

INSERT INTO T88 VALUES ( 4, 5, 3, 5, 4, 5, 2, 3, 5, 5, 4, 1 );

INSERT INTO T88 VALUES ( 3, 4, 3, 1, 1, 4, 1, 3, 2, 2, 5, 2 );

INSERT INTO T88 VALUES ( 1, 2, 4, 3, 1, 4, 2, 1, 2, 2, 1, 5 );

INSERT INTO T88 VALUES ( 5, 3, 1, 3, 1, 3, 3, 4, 4, 1, 4, 2 );

INSERT INTO T88 VALUES ( 5, 1, 3, 3, 5, 1, 3, 4, 4, 1, 3, 1 );

INSERT INTO T88 VALUES ( 1, 1, 1, 2, 5, 5, 1, 1, 5, 1, 5, 3 );

SELECT 1;


-- Test: tiny-aggs_data_T89
INSERT INTO T89 VALUES ( 3, 4, 4 );

INSERT INTO T89 VALUES ( 3, 2, 2 );

INSERT INTO T89 VALUES ( 3, 5, 3 );

INSERT INTO T89 VALUES ( 5, 3, 3 );

INSERT INTO T89 VALUES ( 4, 5, 2 );

INSERT INTO T89 VALUES ( 3, 2, 5 );

INSERT INTO T89 VALUES ( 5, 5, 4 );

INSERT INTO T89 VALUES ( 4, 2, 3 );

INSERT INTO T89 VALUES ( 5, 4, 3 );

INSERT INTO T89 VALUES ( 1, 3, 2 );

SELECT 1;


-- Test: tiny-aggs_data_T90
INSERT INTO T90 VALUES ( 4, 5, 1, 4, 2, 5, 1, 3, 4, 2, 2, 1, 5, 2 );

INSERT INTO T90 VALUES ( 4, 3, 2, 2, 4, 4, 4, 3, 1, 5, 1, 2, 4, 3 );

INSERT INTO T90 VALUES ( 1, 1, 1, 5, 3, 2, 3, 3, 1, 4, 1, 5, 4, 1 );

INSERT INTO T90 VALUES ( 1, 2, 1, 2, 1, 1, 5, 1, 5, 2, 5, 1, 2, 3 );

INSERT INTO T90 VALUES ( 4, 5, 2, 1, 3, 1, 4, 2, 2, 4, 2, 1, 3, 3 );

INSERT INTO T90 VALUES ( 3, 2, 2, 5, 4, 5, 4, 4, 2, 3, 1, 1, 5, 2 );

INSERT INTO T90 VALUES ( 5, 5, 5, 2, 3, 4, 4, 1, 3, 5, 2, 2, 4, 3 );

INSERT INTO T90 VALUES ( 5, 4, 2, 3, 3, 3, 1, 5, 3, 3, 3, 5, 4, 4 );

INSERT INTO T90 VALUES ( 2, 1, 4, 3, 2, 5, 4, 3, 5, 3, 4, 3, 1, 2 );

INSERT INTO T90 VALUES ( 4, 2, 3, 4, 3, 5, 1, 3, 2, 3, 5, 2, 1, 5 );

SELECT 1;


-- Test: tiny-aggs_data_T91
INSERT INTO T91 VALUES ( 1, 2, 3, 5, 2, 5, 4, 2, 3, 5, 4, 3, 4, 2, 4, 4, 5, 2, 3, 3, 1, 5 );

INSERT INTO T91 VALUES ( 3, 2, 5, 4, 5, 1, 2, 4, 3, 4, 4, 5, 3, 5, 2, 3, 2, 1, 1, 2, 1, 2 );

INSERT INTO T91 VALUES ( 4, 3, 5, 1, 3, 2, 4, 3, 1, 2, 1, 5, 3, 5, 5, 4, 2, 5, 1, 5, 2, 3 );

INSERT INTO T91 VALUES ( 3, 5, 2, 1, 5, 4, 1, 3, 4, 4, 1, 4, 2, 5, 2, 2, 5, 5, 1, 3, 3, 4 );

INSERT INTO T91 VALUES ( 4, 5, 4, 4, 4, 2, 5, 4, 4, 2, 1, 4, 3, 5, 3, 1, 4, 1, 3, 4, 2, 1 );

INSERT INTO T91 VALUES ( 5, 5, 3, 2, 3, 5, 2, 4, 2, 5, 4, 5, 1, 2, 4, 3, 3, 4, 1, 4, 3, 4 );

INSERT INTO T91 VALUES ( 5, 3, 2, 2, 4, 1, 2, 5, 3, 1, 2, 5, 5, 5, 2, 4, 5, 3, 4, 3, 1, 1 );

INSERT INTO T91 VALUES ( 1, 5, 2, 2, 5, 1, 2, 5, 2, 2, 2, 2, 4, 5, 4, 4, 1, 2, 3, 2, 4, 3 );

INSERT INTO T91 VALUES ( 3, 1, 3, 4, 1, 1, 5, 4, 4, 2, 1, 1, 1, 3, 5, 2, 3, 5, 5, 1, 5, 1 );

INSERT INTO T91 VALUES ( 3, 4, 1, 1, 2, 2, 1, 2, 4, 4, 3, 2, 1, 1, 3, 3, 5, 3, 4, 5, 1, 5 );

SELECT 1;


-- Test: tiny-derivedtables_queries-127
SELECT
	DT994.C791
	, DT995.C610
FROM
	(
		(
			T27 DT997
		RIGHT OUTER JOIN
			(
				T65 DT994
			INNER JOIN
				T51 DT995
			ON
				DT994.C793 = DT995.C619
			)
		ON
			DT997.C325 < DT995.C620
		)
	INNER JOIN
		T77 DT996
	ON
		DT997.C325 > DT996.C962
	)
WHERE
	(
		(
			DT996.C955 < DT995.C610
		)
		AND
		(
			(
				(
					DT996.C952 = DT996.C954
				)
				AND
				(
					DT994.C797 = DT994.C785
				)
			)
			OR
			(
				(
					DT994.C783 < DT996.C955
				)
				AND
				(
					DT996.C948 < DT997.C321
				)
			)
		)
	)
	OR
	(
		DT997.C326 = DT996.C952
	)
GROUP BY
	DT995.C610
	, DT996.C951
	, DT994.C791
ORDER BY
	DT994.C791
	, DT995.C610
LIMIT 197;


-- Test: tiny-derivedtables_queries-128
SELECT
	DT1000.C2611
	, MIN( DT1001.C1044 )
FROM
	(
		(
			T85 DT1001
		INNER JOIN
			(
				T15 DT1007
			INNER JOIN
				(
				SELECT
					DT999.C850
					, MAX( DT999.C841 )
					, DT998.C573
					, DT998.C581
					, DT999.C841
					, AVG( DT999.C845 )
				FROM
					(
						T48 DT998
					INNER JOIN
						T70 DT999
					ON
						DT998.C585 = DT999.C850
					)
				WHERE
					(
						(
							(
								DT998.C584 < DT999.C857
							)
							AND
							(
								DT998.C584 = DT999.C851
							)
						)
						AND
						(
							(
								DT998.C583 = DT999.C848
							)
							OR
							(
								DT998.C574 = DT998.C576
							)
						)
					)
					OR
					(
						(
							(
								(
									DT998.C584 > DT998.C581
								)
								OR
								(
									DT999.C856 > DT998.C576
								)
							)
							OR
							(
								DT999.C840 = DT999.C848
							)
						)
						AND
						(
							DT999.C847 <> DT998.C582
						)
					)
				GROUP BY
					DT999.C841
					, DT999.C845
					, DT998.C581
					, DT999.C850
					, DT998.C573
				ORDER BY
					DT999.C850
					, MAX( DT999.C841 )
					, DT998.C573
					, DT998.C581
					, DT999.C841
					, AVG( DT999.C845 )
				LIMIT 536
				) AS DT1000 ( C2610, C2611, C2612, C2613, C2614, C2615 ) 
			ON
				DT1007.C188 = DT1000.C2614
			)
		ON
			DT1001.C1029 = DT1000.C2612
		)
	INNER JOIN
		(
		SELECT
			DT1002.C1069
			, DT1002.C1073
			, DT1005.C702
			, DT1003.C435
			, DT1003.C423
			, DT1005.C711
		FROM
			(
				T88 DT1002
			LEFT OUTER JOIN
				(
					(
						T91 DT1004
					INNER JOIN
						T36 DT1003
					ON
						DT1004.C1094 <> DT1003.C434
					)
				INNER JOIN
					T58 DT1005
				ON
					DT1004.C1100 < DT1005.C702
				)
			ON
				DT1002.C1074 = DT1003.C419
			)
		WHERE
			(
				DT1004.C1112 = DT1005.C698
			)
			OR
			(
				(
					DT1004.C1098 < DT1003.C436
				)
				OR
				(
					(
						(
							DT1002.C1072 <> DT1002.C1070
						)
						AND
						(
							DT1004.C1103 <> DT1004.C1096
						)
					)
					AND
					(
						(
							DT1003.C434 = DT1003.C429
						)
						OR
						(
							(
								DT1003.C420 <> DT1005.C701
							)
							AND
							(
								(
									DT1004.C1111 = DT1005.C700
								)
								OR
								(
									DT1004.C1110 = DT1003.C430
								)
							)
						)
					)
				)
			)
		ORDER BY
			DT1002.C1069
			, DT1002.C1073
			, DT1005.C702
			, DT1003.C435
			, DT1003.C423
			, DT1005.C711
		LIMIT 730
		)  DT1006 ( C2616, C2617, C2618, C2619, C2620, C2621 ) 
	ON
		DT1007.C191 = DT1006.C2621
	)
WHERE
	(
		(
			DT1001.C1038 > DT1000.C2610
		)
		AND
		(
			DT1000.C2610 = DT1006.C2618
		)
	)
	AND
	(
		(
			DT1001.C1029 = DT1000.C2612
		)
		AND
		(
			(
				DT1007.C194 <> DT1006.C2619
			)
			OR
			(
				(
					DT1006.C2616 < DT1001.C1031
				)
				AND
				(
					DT1001.C1045 = DT1001.C1041
				)
			)
		)
	)
GROUP BY
	DT1000.C2611
	, DT1001.C1044
ORDER BY
	DT1000.C2611
	, MIN( DT1001.C1044 )
LIMIT 610;


-- Test: tiny-queries_innerjoins.xml-35
SELECT
	DT215.C1081
	, DT214.C275
	, DT216.C938
	, DT215.C1086
	, DT214.C276
	, DT215.C1084
	, DT215.C1091
	, DT214.C278
	, DT216.C943
	, DT214.C273
	, DT215.C1079
	, DT214.C272
	, DT215.C1085
	, DT215.C1088
	, DT215.C1090
	, DT216.C945
FROM
	(
		(
			(
				T76 DT216
			INNER JOIN
				T90 DT215
			ON
				DT216.C946 <> DT215.C1091
			)
		INNER JOIN
			T89 DT217
		ON
			DT215.C1082 = DT217.C1077
		)
	INNER JOIN
		T23 DT214
	ON
		DT215.C1092 > DT214.C273
	)
WHERE
	(
		DT214.C270 = DT215.C1081
	)
	OR
	(
		DT215.C1083 = DT215.C1091
	)
ORDER BY
	DT215.C1081
	, DT214.C275
	, DT216.C938
	, DT215.C1086
	, DT214.C276
	, DT215.C1084
	, DT215.C1091
	, DT214.C278
	, DT216.C943
	, DT214.C273
	, DT215.C1079
	, DT214.C272
	, DT215.C1085
	, DT215.C1088
	, DT215.C1090
	, DT216.C945
LIMIT 297;


-- Test: tiny-queries_outerjoins-22
SELECT
	DT170.C572
	, DT167.C243
	, DT170.C576
	, DT167.C246
	, DT168.C980
	, DT170.C568
	, DT170.C578
	, DT168.C979
FROM
	(
		(
			T20 DT167
		INNER JOIN
			(
				T88 DT169
			INNER JOIN
				T48 DT170
			ON
				DT169.C1068 <> DT170.C568
			)
		ON
			DT167.C237 = DT169.C1074
		)
	RIGHT OUTER JOIN
		T79 DT168
	ON
		DT169.C1074 > DT168.C985
	)
WHERE
	(
		(
			DT170.C572 < DT168.C981
		)
		AND
		(
			(
				DT168.C985 < DT168.C984
			)
			OR
			(
				DT169.C1075 = DT167.C236
			)
		)
	)
	AND
	(
		DT170.C582 > DT169.C1066
	)
ORDER BY
	DT170.C572
	, DT167.C243
	, DT170.C576
	, DT167.C246
	, DT168.C980
	, DT170.C568
	, DT170.C578
	, DT168.C979
LIMIT 479;


-- Test: tiny-queries_mix-52
SELECT
	DT700.C93
	, DT702.C332
	, DT700.C108
	, MIN( DT702.C332 )
FROM
	(
		(
			T7 DT700
		INNER JOIN
			T5 DT701
		ON
			DT700.C97 < DT701.C73
		)
	RIGHT OUTER JOIN
		T28 DT702
	ON
		DT700.C94 = DT702.C332
	)
WHERE
	(
		DT702.C332 <> DT701.C68
	)
	AND
	(
		(
			(
				DT700.C89 = DT700.C106
			)
			AND
			(
				DT700.C101 <> DT700.C92
			)
		)
		AND
		(
			DT701.C68 < DT700.C96
		)
	)
GROUP BY
	DT700.C108
	, DT702.C332
	, DT700.C93
ORDER BY
	DT700.C93
	, DT702.C332
	, DT700.C108
	, MIN( DT702.C332 )
LIMIT 284;

drop table if exists t5;
drop table if exists t7;
drop table if exists t15;
drop table if exists t20;
drop table if exists t23;
drop table if exists t27;
drop table if exists t28;
drop table if exists t36;
drop table if exists t48;
drop table if exists t51;
drop table if exists t58;
drop table if exists t65;
drop table if exists t70;
drop table if exists t76;
drop table if exists t77;
drop table if exists t79;
drop table if exists t85;
drop table if exists t88;
drop table if exists t89;
drop table if exists t90;
drop table if exists t91;


