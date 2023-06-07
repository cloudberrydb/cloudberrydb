/*-------------------------------------------------------------------------
 *
 * log.c
 *	  A simple tool for logging that does not depend on postgres 
 *    process information
 *-------------------------------------------------------------------------
 */
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include "postgres.h"
#include "common/fe_memutils.h"
#include "fe_utils/log.h"

#define LOG_FILE_SUFFIX "csv"
#define LOG_FILE_LENGTH 2048

static cbdb_log_level log_level;
static FILE *log_file;
static int cur_line_num; // number of rows recorded in the current log file
static int max_line = 5000; // Maximum number of rows that can be recorded in each log file

static char *log_file_name = NULL;
static char *cur_dir_name = NULL; // The folder where the log file resides

static const char* s_level[LEVEL_COUNT] = 
{
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "FATAL"
};

/* When the number of log lines recorded in the log file exceeds max_line, this function 
   is called for processing. This function will reopen a new file for future writes and 
   close the file that was previously in use and command (add the current timestamp)
*/
static
void rotate()
{
    char timestamp[MAX_TIMESTAMP_LENGTH];
    time_t ticks;
    struct tm *ptm = NULL;
    char pre_filename[MAX_FILE_NAME_LEN];
    char modifed_filename[MAX_FILE_NAME_LEN];
    
    fclose(log_file);
    log_file = NULL;
    ticks = time(NULL);
    ptm = localtime(&ticks);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d-%H-%M-%S", ptm);

    snprintf(pre_filename, MAX_FILE_NAME_LEN, "%s/%s.%s", cur_dir_name, log_file_name, LOG_FILE_SUFFIX);
    snprintf(modifed_filename, MAX_FILE_NAME_LEN, "%s/%s.%s.%s", cur_dir_name, log_file_name, timestamp, LOG_FILE_SUFFIX);
    if (rename(pre_filename, modifed_filename) != 0)
    {
        printf("rename log file failed: %s -> %s\n", pre_filename, modifed_filename);
        return;
    }
    log_file = fopen(pre_filename, "a");
    if (!log_file)
    {
        printf("Failed to open a new fts file\n");
        return;
    }
    cur_line_num = 0;
    return;
}


void
cbdb_log(cbdb_log_level level, const char* file, int line, const char* format, ...)
{
    struct tm *ptm = NULL;
    char date[MAX_TIMESTAMP_LENGTH];
    char timestamp[MAX_TIMESTAMP_LENGTH];
    struct timeval tv;
    int len = 0;
    const char *fmt = "%s,%s,%s,line:%d";

    if (log_level > level)
        return;

    if (!log_file)
    {
        printf("fts log file is not opened\n");
        return;
    }

    gettimeofday(&tv, NULL);
    ptm = localtime(&tv.tv_sec);
    strftime(date, sizeof(date), "%Y-%m-%d %H:%M:%S", ptm);
    snprintf(timestamp, MAX_TIMESTAMP_LENGTH, "%s.%06ld", date, tv.tv_usec);

    // record timestamp, file name, and line num
    len = snprintf(NULL, 0, fmt, timestamp, s_level[level], file, line);
    if (len > 0)
    {
        char buffer[1000];
        snprintf(buffer, len + 1, fmt, timestamp, s_level[level], file, line);
        buffer[len] = 0;
        fprintf(log_file, "%s,", buffer);
    }

    // record log information
    va_list arg_ptr;
    va_start(arg_ptr, format);
    len = vsnprintf(NULL, 0, format, arg_ptr);
    va_end(arg_ptr);
    if (len > 0)
    {
        char buffer[1000];
        va_start(arg_ptr, format);
        vsnprintf(buffer, len + 1, format, arg_ptr);
        va_end(arg_ptr);
        buffer[len] = 0;
        fprintf(log_file, "%s\n", buffer);   
    }

    cur_line_num ++;
    fflush(log_file);
       
    if (max_line > 0 && cur_line_num >= max_line)
        rotate();
}

bool
cbdb_set_log_file(const char* dir_name, const char* file_name)
{
    char full_path[LOG_FILE_LENGTH];
    int file_length = 0;
    
    if (dir_name == NULL)
    {
        printf("log dir name should not be null\n");
        return false;
    }

    if (file_name == NULL)
    {
        printf("log file name should not be null\n");
        return false;
    }

    // avoid memory leaks caused by multiple assignments
    if (cur_dir_name)
    {
        free(cur_dir_name);
        cur_dir_name = NULL;
    }

    if (log_file_name)
    {
        free(log_file_name);
        log_file_name = NULL;
    }

    if (log_file)
    {
        fclose(log_file);
        log_file = NULL;
    }

    if (access(dir_name, F_OK) != 0)
    {
        if (mkdir(dir_name, DIRECTORY_PRIVILEGE) != 0)
        {
            printf("create log directory failed: %s\n", dir_name);
            return false;
        }
    }

    cur_dir_name = strdup(dir_name);
    log_file_name = strdup(file_name);

    file_length = snprintf(full_path, sizeof(full_path), "%s/%s.%s", cur_dir_name, log_file_name, LOG_FILE_SUFFIX);
    if (file_length > LOG_FILE_LENGTH - 2)
    {
        printf("log file path length exceeded max length: %d\n", LOG_FILE_LENGTH -2);
        return false;
    }
    
    log_file = fopen(full_path, "a");
    if (!log_file)
    {
        printf("open log file failed: %s\n", full_path);
        return false;
    }
    return true;
}

bool
cbdb_set_max_log_file_line(int max_line_)
{
    max_line = max_line_ <= 0 ? 5000 : max_line_;
    return true;
}

bool
cbdb_set_log_level(cbdb_log_level level)
{
    log_level = level;
    return true;
}