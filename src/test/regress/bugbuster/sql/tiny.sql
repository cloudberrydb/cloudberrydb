drop table if exists t15;
drop table if exists t27;
drop table if exists t36;
drop table if exists t48;
drop table if exists t51;
drop table if exists t58;
drop table if exists t65;
drop table if exists t70;
drop table if exists t77;
drop table if exists t85;
drop table if exists t88;
drop table if exists t91;
-- Test: tiny-aggs_schmea-1

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


