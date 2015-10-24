/*
 * cdbbackup.h
 *
 */

#ifndef CDBBACKUP_H
#define CDBBACKUP_H

/*
 * External declarations
 */
extern Datum gp_backup_launch__(PG_FUNCTION_ARGS);
extern Datum gp_restore_launch__(PG_FUNCTION_ARGS);
extern Datum gp_read_backup_file__(PG_FUNCTION_ARGS);
extern Datum gp_write_backup_file__(PG_FUNCTION_ARGS);

#endif   /* CDBBACKUP_H */
