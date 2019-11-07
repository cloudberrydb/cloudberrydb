# Verify that pg_notification_queue_usage correctly reports a non-zero result,
# after submitting notifications while another connection is listening for
# those notifications and waiting inside an active transaction.

session "listener"
step "listen"	{ LISTEN a; }
step "begin"	{ BEGIN; }
teardown		{ ROLLBACK; UNLISTEN *; }

session "notifier"
step "check"	{ SELECT pg_notification_queue_usage() > 0 AS nonzero; }
# GPDB: use more rows than in upstream, because Greenplum is compiled with a
# larger SLRU block size, and we need to fill at least one page to make
# pg_notification_queue_usage() non-zero.
step "notify"	{ SELECT count(pg_notify('a', s::text)) FROM generate_series(1, 10000) s; }

permutation "listen" "begin" "check" "notify" "check"
