#ifndef DATALAKE_BUFFER_H
#define DATALAKE_BUFFER_H

#include <stdio.h>
#include <vector>
#include <set>
#include <iostream>


namespace Datalake {
namespace Internal {

typedef struct datalakeByteBuffer {
    unsigned char* buffer;
    int buffer_length;
    int pos;
    int used_size;
    int begin_size;
    datalakeByteBuffer() {
        buffer = NULL;
        buffer_length = 0;
        pos = 0;
        used_size = 0;
        begin_size = 0;
    }
}datalakeByteBuffer;

datalakeByteBuffer* create_datalake_bytebuffer();

bool alloc_datalake_bytebuffer(datalakeByteBuffer* buff, int size);

datalakeByteBuffer* resize_datalake_bytebuffer(datalakeByteBuffer* source);

void release_datalake_bytebuffer(datalakeByteBuffer* buff);

}
}


#endif