-- TEST 1: block checkpoint on segments

-- pause the 2PC after setting inCommit flag
! gpfaultinjector -f twophase_transaction_commit_prepared -m async -y suspend -o 0 -H ALL -r primary;

-- trigger a 2PC, and it will block at commit;
2: checkpoint;
2: begin;
2: create table t_commit_transaction_block_checkpoint (c int) distributed by (c);
2&: commit;

-- do checkpoint on dbid 3 (segment 1) in utility mode, and it should block
3U&: checkpoint;

-- resume the 2PC after setting inCommit flag
! gpfaultinjector -f twophase_transaction_commit_prepared -m async -y reset -o 0 -H ALL -r primary;
2<:
3U<:

-- TEST 2: block checkpoint on master

-- pause the CommitTransaction right before persistent table cleanup after
-- notifyCommittedDtxTransaction()
! gpfaultinjector -f transaction_commit_pass1_from_drop_in_memory_to_drop_pending -m async -y suspend -o 0 -s 1;

-- trigger a 2PC, and it will block at commit;
2: checkpoint;
2: begin;
2: drop table t_commit_transaction_block_checkpoint;
2&: commit;

-- do checkpoint on dbid 1 (master) in utility mode, and it should block
1U&: checkpoint;

-- resume the 2PC
! gpfaultinjector -f transaction_commit_pass1_from_drop_in_memory_to_drop_pending -m async -y reset -o 0 -s 1;
2<:
1U<:

