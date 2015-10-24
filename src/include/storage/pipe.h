#ifndef PIPE_H
#define PIPE_H

#include <pthread.h>
#include <postgres.h>

#define PIPE_BUFFER_SIZE	(1<<15)

typedef struct
{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int head;
    int tail;
    int eof;
    char buffer[PIPE_BUFFER_SIZE];
}pipe_t;

extern pipe_t*pipe_init(pipe_t*thiz);
extern void pipe_close(pipe_t*thiz);
extern void pipe_write(pipe_t*thiz,const char*buffer,int size);
extern void pipe_done_writing(pipe_t*thiz);
/* pipe_read will read size or until eof. */
extern int pipe_read(pipe_t*thiz,char*buffer,int size);

#endif
