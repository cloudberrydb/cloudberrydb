-- start_matchsubs
-- m/^\S+ (.*):.*:.*-(\[\S+\]):-/
-- s/^\S+ (.*):.*:.*-(\[\S+\]):-/TIMESTAMP $1:HOST:USER-$2:-/
-- end_matchsubs

\! echo 'EXPANSION_PREPARE_STARTED:<path> to inputfile' > $MASTER_DATA_DIRECTORY/gpexpand.status
\! gpcheckcat
\! gpconfig -r gp_debug_linger
\! gppkg --query no-such-package
\! rm $MASTER_DATA_DIRECTORY/gpexpand.status
