#include "datalakeBuffer.h"

extern "C"
{
// #include "../../common.h"
#include "src/datalake_def.h"
}

namespace Datalake {
namespace Internal {

datalakeByteBuffer* create_datalake_bytebuffer() {
    datalakeByteBuffer *ptr = (datalakeByteBuffer*)palloc0(sizeof(datalakeByteBuffer));
    if (external_table_debug) {
        elog(LOG, "Datalake Log, create_datalake_buffer.");
    }
    return ptr;
}

bool alloc_datalake_bytebuffer(datalakeByteBuffer* buff, int size) {
    if (buff == NULL || size <= 0) {
        return false;
    }
    buff->buffer = (unsigned char*)palloc(size);
    buff->buffer_length = size;
    buff->pos = 0;
    buff->used_size = 0;
    if (external_table_debug) {
        elog(LOG, "Datalake Log, alloc_datalake_buffer size %d", size);
    }
    return true;
}

datalakeByteBuffer* resize_datalake_bytebuffer(datalakeByteBuffer* source) {
    if (source == NULL) {
        return NULL;
    }
    int resize = source->buffer_length * 2;
    datalakeByteBuffer *dest = create_datalake_bytebuffer();
    alloc_datalake_bytebuffer(dest, resize);
    memcpy(dest->buffer, source->buffer, source->used_size);
    dest->used_size = source->used_size;
    dest->pos = source->pos;
    release_datalake_bytebuffer(source);
    if (external_table_debug) {
        elog(LOG, "Datalake Log, resize_datalake_buffer resize %d.", resize);
    }
    return dest;
}

void release_datalake_bytebuffer(datalakeByteBuffer* buff) {
    if (buff) {
        if (buff->buffer) {
            pfree(buff->buffer);
            buff->buffer = NULL;
        }
        pfree(buff);
        buff = NULL;
    }
    if (external_table_debug) {
        elog(LOG, "Datalake Log, release_datalake_buffer.");
    }
}

}
}