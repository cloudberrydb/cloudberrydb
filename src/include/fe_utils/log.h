/*-------------------------------------------------------------------------
 *
 * log.h
 *	  Interface for a logging tools.
 *-------------------------------------------------------------------------
 */
#ifndef LOG_H
#define LOG_H

#include "c.h"

#define LEVEL_COUNT 5
#define MAX_FILE_NAME_LEN 200
#define DIRECTORY_PRIVILEGE 0744
#define MAX_TIMESTAMP_LENGTH 32

typedef enum cbdb_log_level
{
    CBDB_LOG_DEBUG = 0,
    CBDB_LOG_INFO,
    CBDB_LOG_WARN,
    CBDB_LOG_ERROR,
    CBDB_LOG_FATAL,
} cbdb_log_level;

#define cbdb_log_debug(format, ...) \
    cbdb_log(CBDB_LOG_DEBUG, __FILE__, __LINE__, format, ##__VA_ARGS__)
    
#define cbdb_log_info(format, ...) \
    cbdb_log(CBDB_LOG_INFO, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define cbdb_log_warning(format, ...) \
    cbdb_log(CBDB_LOG_WARN, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define cbdb_log_error(format, ...) \
    cbdb_log(CBDB_LOG_ERROR, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define cbdb_log_fatal(format, ...) \
    cbdb_log(CBDB_LOG_FATAL, __FILE__, __LINE__, format, ##__VA_ARGS__)

extern void cbdb_log(cbdb_log_level level, const char *file, int line, const char *format, ...) pg_attribute_printf(4, 5);

extern bool cbdb_set_log_file(const char* dir_name, const char* file_name);

extern bool cbdb_set_max_log_file_line(int max_line);

extern bool cbdb_set_log_level(cbdb_log_level level);

#endif // LOG_H
