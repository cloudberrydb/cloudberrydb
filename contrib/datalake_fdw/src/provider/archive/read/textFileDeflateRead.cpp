#include "textFileDeflateRead.h"



// extern "C"
// {
// #include "src/gpossext.h"
// }

namespace Datalake {
namespace Internal {


textFileDeflateRead::textFileDeflateRead() {
    pos = 0;
    path = "";
    state = FILE_UNDEF;
    readFinish = false;
    resizeZlibChunkBuffer(ZLIB_CHUNK_SIZE);
    resizeInputBuffer(ZLIB_CHUNK_SIZE);
    resizeOutputBuffer(ZLIB_CHUNK_SIZE);
}

textFileDeflateRead::~textFileDeflateRead() {
    release_datalake_bytebuffer(zlibChunkBuffer);
    release_datalake_bytebuffer(inputBuffer);
    release_datalake_bytebuffer(outputBuffer);
    zlibChunkBuffer = NULL;
    inputBuffer = NULL;
    outputBuffer = NULL;
}

void textFileDeflateRead::resizeZlibChunkBuffer(int size) {
    zlibChunkBuffer = create_datalake_bytebuffer();
    alloc_datalake_bytebuffer(zlibChunkBuffer, size);
    if (external_table_debug) {
        elog(LOG, "Datalake Log, zlib text file resize zlibChunkBuffer %d", size);
    }
}

void textFileDeflateRead::resizeInputBuffer(int size) {
    inputBuffer = create_datalake_bytebuffer();
    alloc_datalake_bytebuffer(inputBuffer, size);
    if (external_table_debug) {
        elog(LOG, "Datalake Log, zlib text file resize inputBuffer %d", size);
    }
}

void textFileDeflateRead::resizeOutputBuffer(int size) {
    outputBuffer = create_datalake_bytebuffer();
    alloc_datalake_bytebuffer(outputBuffer, size);
    if (external_table_debug) {
        elog(LOG, "Datalake Log, zlib text file resize outputBuffer %d", size);
    }
}

void textFileDeflateRead::open(ossFileStream ossFile, std::string fileName, readOption options) {
    stream = ossFile;
    readFinish = false;
    memset(&strm, 0, sizeof(strm));
    openFile(stream, fileName.c_str(), setStreamFlag(options));
    path = fileName;
    state = FILE_OPEN;
    if (external_table_debug) {
        elog(LOG, "Datalake Log, open deflate text file %s successful.", fileName.c_str());
    }
    int ret = inflateInit(&strm);
    if (ret != Z_OK) {
        elog(ERROR, "Datalake Error, inflateInit failed! when read file %s", fileName.c_str());
    }
}


int64_t textFileDeflateRead::read(void* buffer, size_t length) {
    if (readFinish) {
        return 0;
    }

    int bufferLeftBytes = length;
    int bufferPos = 0;
    while(bufferLeftBytes > 0) {
        if (outputBuffer->pos >= outputBuffer->used_size) {
            outputBuffer->pos = 0;
            outputBuffer->used_size = getNext();
            if (outputBuffer->used_size == 0) {
                break;
            }
        }

        int leftBytes = 0;
        int outputLeftBytes = outputBuffer->used_size - outputBuffer->pos;
        if (outputLeftBytes < bufferLeftBytes) {
            leftBytes = outputLeftBytes;
        } else {
            leftBytes = bufferLeftBytes;
        }
        memcpy((char*)buffer + bufferPos, outputBuffer->buffer + outputBuffer->pos, leftBytes);
        bufferPos += leftBytes;
        outputBuffer->pos += leftBytes;
        bufferLeftBytes -= leftBytes;
    }
    return bufferPos;
}

int64_t textFileDeflateRead::fillInputBuffer(int length) {
    if (length > inputBuffer->buffer_length) {
        datalakeByteBuffer* tmp = resize_datalake_bytebuffer(inputBuffer);
        inputBuffer = tmp;
    }
    int nread = readFile(stream, inputBuffer->buffer, length);
    if (nread <= 0) {
        elog(ERROR, "Datalake Error, deflate text file %s read block header 4 bytes failed! %s.",
            path.c_str(), gopherGetLastError());
    }
    if (external_table_debug) {
        elog(LOG, "Datalake Log, read %d in file %s.", nread, path.c_str());
    }
    return nread;
}

int64_t textFileDeflateRead::getNext() {
    int64_t uncompress_len = 0;
    int uncompress_pos = 0;
    bool until_next = false;
    do {
        if (pos >= fileLength) {
            /* read over */
            readFinish = true;
            return 0;
        }
        int nread = fillInputBuffer(ZLIB_CHUNK_SIZE);
        if (nread == 0) {
            break;
        }
        pos += nread;
        strm.avail_in = nread;
        strm.next_in = inputBuffer->buffer;
        do {
            strm.next_out = zlibChunkBuffer->buffer;
            strm.avail_out = ZLIB_CHUNK_SIZE;
            int ret = inflate(&strm, Z_NO_FLUSH);
            switch(ret) {
                case Z_NEED_DICT:
                case Z_DATA_ERROR:
                case Z_MEM_ERROR: {
                    close();
                    elog(ERROR, "Datalake Error, Zlib read file %s Zlib Error Code %d.",
                        path.c_str(), ret);
                }
                default:
                    break;
            }

            unsigned have = ZLIB_CHUNK_SIZE - strm.avail_out;
            uncompress_len += have;
            if (uncompress_len >= outputBuffer->buffer_length) {
                datalakeByteBuffer* tmp = resize_datalake_bytebuffer(outputBuffer);
                outputBuffer = tmp;
            }
            memcpy(outputBuffer->buffer + uncompress_pos, zlibChunkBuffer->buffer, have);
            uncompress_pos += have;
        } while (strm.avail_out == 0);
    } while(until_next);
    return uncompress_len;
}

void textFileDeflateRead::close() {
    if (state == FILE_OPEN) {
        inflateEnd(&strm);
        closeFile(stream);
        if (external_table_debug) {
            elog(LOG, "Datalake Log, close deflate file %s successful.", path.c_str());
        }
    }
}

}
}