DROP TABLE IF EXISTS part_tbl_upd_del;

-- check gdd is on
show gp_enable_global_deadlock_detector;

CREATE TABLE part_tbl_upd_del (a int, b int, c int) PARTITION BY RANGE(b) (START(1) END(2) EVERY(1));
INSERT INTO part_tbl_upd_del SELECT i, 1, i FROM generate_series(1,10)i;

1: BEGIN;
1: DELETE FROM part_tbl_upd_del;
-- on QD, there's a lock on the root and the target partition
1: SELECT 1 FROM pg_locks WHERE relation='part_tbl_upd_del_1_prt_1'::regclass::oid AND gp_segment_id = -1;
1: SELECT 1 FROM pg_locks WHERE relation='part_tbl_upd_del'::regclass::oid AND gp_segment_id = -1;
1: ROLLBACK;


1: BEGIN;
1: UPDATE part_tbl_upd_del SET c = 1 WHERE c = 1;
-- on QD, there's a lock on the root and the target partition
1: SELECT 1 FROM pg_locks WHERE relation='part_tbl_upd_del_1_prt_1'::regclass::oid AND gp_segment_id = -1;
1: SELECT 1 FROM pg_locks WHERE relation='part_tbl_upd_del'::regclass::oid AND gp_segment_id = -1;
1: ROLLBACK;

1: BEGIN;
1: DELETE FROM part_tbl_upd_del_1_prt_1;
-- since the delete operation is on leaf part, there will be a lock on QD
1: SELECT 1 FROM pg_locks WHERE relation='part_tbl_upd_del_1_prt_1'::regclass::oid AND gp_segment_id = -1 AND mode='RowExclusiveLock';
1: ROLLBACK;


1: BEGIN;
1: UPDATE part_tbl_upd_del_1_prt_1 SET c = 1 WHERE c = 1;
-- since the update operation is on leaf part, there will be a lock on QD
1: SELECT 1 FROM pg_locks WHERE relation='part_tbl_upd_del_1_prt_1'::regclass::oid AND gp_segment_id = -1 AND mode='RowExclusiveLock';
1: ROLLBACK;
