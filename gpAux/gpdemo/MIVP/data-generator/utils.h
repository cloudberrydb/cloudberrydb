#ifndef __cplusplus

#ifndef bool
typedef char bool;
#endif

#ifndef true
#define true    ((bool) 1)
#endif

#ifndef false
#define false   ((bool) 0)
#endif
#endif   /* not C++ */

#define MIN(a,b) (a < b ? a : b)
#define MAX(a,b) (a > b ? a : b)

#define ISLEAP(a) ( ((a%4==0) && ! (a%100==0)) || (a%400==0) )
#define LEAPS {31,29,31,30,31,30,31,31,30,31,30,31}
#define NONLEAPS {31,28,31,30,31,30,31,31,30,31,30,31}

typedef struct
{
  	char 	**list;
  	int 		chunksize;
  	int 		alloc_size;
  	int 		used_size;
  	char 	**pos;

} Stringlist;


typedef struct
{
        char   *bytes;
        int             chunksize;
        int             alloc_size;
        int             used_size;
        char   *pos;
} bytebuffer;

//Memory management function (prototype) for a line buffer
void bytebuffer_reset(bytebuffer *dest);
void bytebuffer_free(bytebuffer *linebuf);
void bytebuffer_load(char *source, int source_size, bytebuffer *dest);
bool alloc_more_bytebuffer(bytebuffer *bbuf, int min_size);

bool alloc_more_stringlist(Stringlist *slist, int min_size);

float randomrange(float min, float max);
