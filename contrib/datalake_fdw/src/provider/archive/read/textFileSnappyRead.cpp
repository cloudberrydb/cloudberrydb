#include "textFileSnappyRead.h"
#include <snappy-c.h>
#include <snappy.h>

// extern "C"
// {
// #include "src/gpossext.h"
// }

namespace Datalake {
namespace Internal {

textFileSnappyRead::textFileSnappyRead() {
    pos = 0;
    blockLength = 0;
    uncompress_length_in_block = 0;
    path = "";
    state = FILE_UNDEF;
    readFinish = false;
}

textFileSnappyRead::~textFileSnappyRead() {
    if (inputBuffer.buffer) {
        pfree(inputBuffer.buffer);
        inputBuffer.buffer = NULL;
    }
    if (outputBuffer.buffer) {
        pfree(outputBuffer.buffer);
        outputBuffer.buffer = NULL;
    }
}

void textFileSnappyRead::open(ossFileStream ossFile, std::string fileName, readOption options) {
    stream = ossFile;
    readFinish = false;
    openFile(stream, fileName.c_str(), setStreamFlag(options));
    path = fileName;
    state = FILE_OPEN;
    originalBlockSize = 0;
    if (external_table_debug) {
        elog(LOG, "Datalake Log, open snappy text file %s successful.", fileName.c_str());
    }
}

int64_t textFileSnappyRead::read(void* buffer, size_t length) {
    if (readFinish) {
        return 0;
    }

    int bufferLeftBytes = length;
    int bufferPos = 0;
    while(bufferLeftBytes > 0) {
        if (outputBuffer.pos >= outputBuffer.used_size) {
            outputBuffer.pos = 0;
            outputBuffer.used_size = getNext();
            if (outputBuffer.used_size == 0) {
                break;
            }
        }

        int leftBytes = 0;
        int outputLeftBytes = outputBuffer.used_size - outputBuffer.pos;
        if (outputLeftBytes < bufferLeftBytes) {
            leftBytes = outputLeftBytes;
        } else {
            leftBytes = bufferLeftBytes;
        }
        memcpy((char*)buffer + bufferPos, outputBuffer.buffer + outputBuffer.pos, leftBytes);
        bufferPos += leftBytes;
        outputBuffer.pos += leftBytes;
        bufferLeftBytes -= leftBytes;
    }
    return bufferPos;
}

int64_t textFileSnappyRead::fillInputBuffer(int length) {
    if (length > inputBuffer.buffer_length) {
        int resize = length * 2;
        resizeInputBuffer(resize);
    }
    int nread = readFile(stream, inputBuffer.buffer, length);
    if (nread <= 0) {
        elog(ERROR, "Datalake Error, snappy text file %s read block header 4 bytes failed! %s.",
            path.c_str(), gopherGetLastError());
    }
    if (external_table_debug) {
        elog(LOG, "Datalake Log, read %d in file %s.", nread, path.c_str());
    }
    return nread;
}

void textFileSnappyRead::resizeInputBuffer(int size) {
    if (inputBuffer.buffer != NULL) {
        pfree(inputBuffer.buffer);
    }
    int resize = size * 2;
    inputBuffer.buffer = (char*)palloc(resize);
    inputBuffer.buffer_length = resize;
    inputBuffer.pos = 0;
    inputBuffer.used_size = 0;
    if (external_table_debug) {
        elog(LOG, "Datalake Log, snappy text file resize inputBuffer %d", size);
    }
}

void textFileSnappyRead::resizeOutputBuffer(int size) {
    if (outputBuffer.buffer != NULL) {
        pfree(outputBuffer.buffer);
    }
    int resize = size * 2;
    outputBuffer.buffer = (char*)palloc(resize);
    outputBuffer.buffer_length = resize;
    outputBuffer.pos = 0;
    outputBuffer.used_size = 0;
    if (external_table_debug) {
        elog(LOG, "Datalake Log, snappy text file resize outputBuffer %d", size);
    }
}

void textFileSnappyRead::seekIntextfile(int64_t posn) {
    int ret = seekFile(stream, posn);
    if (ret == -1) {
        elog(ERROR, "Datalake Error, snappy text file %s seek failed! detail %s.",
            path.c_str(), gopherGetLastError());
    }

    if (external_table_debug) {
        elog(LOG, "Datalake Log, seek %d in file %s.", ret, path.c_str());
    }
}

int64_t textFileSnappyRead::getNext() {
    int64_t uncompress_len = 0;
    bool until_next = false;
    do {
        if (pos >= fileLength) {
            /* read over */
            readFinish = true;
            return 0;
        }
        seekIntextfile(pos);
        /* Check if we are the beginning of a block */
        if (originalBlockSize == 0) {
            int nread = fillInputBuffer(sizeof(int));
            if (nread == 0) {
                break;
            }
            originalBlockSize = convertBigEndianToInt(inputBuffer.buffer);
            pos += 4;
            if (originalBlockSize == 0) {
                // EOF if originalBlockSize is 0
                // This will occur only when decompressing previous compressed empty file
                /* read over */
                if (external_table_debug) {
                    elog(LOG, "External table, file %s originalBlockSize is 0. "
                        "This will occur only when decompressing previous compressed empty file.",
                            path.c_str());
                }
                readFinish = true;
                return 0;
            }
        }

        if (uncompress_length_in_block >= blockLength) {
            int nread = fillInputBuffer(sizeof(int));
            if (nread == 0) {
                break;
            }

            if ((nread < 4) && (pos + nread >= fileLength)) {
                /* read over */
                readFinish = true;
                return 0;
            }

            blockLength = convertBigEndianToInt(inputBuffer.buffer);
            pos += 4;
        }

        int compressed_len = convertBigEndianToInt(inputBuffer.buffer);
        int nread = fillInputBuffer(compressed_len);
        if (nread == 0) {
            break;
        }

        size_t outlen = 0;
        snappy_status status = snappy_uncompressed_length((inputBuffer.buffer), compressed_len, &outlen);
        if (status != SNAPPY_OK) {
            elog(ERROR, "Datalake Error, snappy text file %s failed! cannot uncompress buffer.", path.c_str());
        }

        if (originalBlockSize != (int64_t)outlen) {
            /* snappy block format is:
             * originalBlockSize(int32B) + compressblockLength(int32B) + compressdata
             * so we can use originalBlockSize do checksum with uncompressbufferlength.
             */
            elog(ERROR, "External Error, read snappy file %s checksum failed!."
                "current position %ld, originalBlockSize %ld, compressblockLength %ld. "
                "uncompressbufferlength %ld not match originalBlockSize.",
                path.c_str(), pos, originalBlockSize, blockLength, outlen);
        } else {
            originalBlockSize = 0;
        }

        if (outlen == 0) {
            /* Not sure if there is a problem here?? */
            if (inputBuffer.buffer_length > outputBuffer.buffer_length) {
                resizeOutputBuffer(inputBuffer.buffer_length);
            }
            memcpy(outputBuffer.buffer, inputBuffer.buffer, compressed_len);
            uncompress_len = compressed_len;
            pos += compressed_len;
            uncompress_length_in_block += compressed_len;
            break;
        } else if ((int)outlen > outputBuffer.buffer_length) {
            resizeOutputBuffer(outlen);
        }

        status = snappy_uncompress((const char*)(inputBuffer.buffer), compressed_len, outputBuffer.buffer, &outlen);
        if (status != SNAPPY_OK) {
            elog(ERROR, "Datalake Error, snappy text file %s failed! cannot uncompress buffer.",
                    path.c_str());
        }
        uncompress_length_in_block += outlen;
        uncompress_len += outlen;
        pos += compressed_len;
        break;
    } while(until_next);
    return uncompress_len;
}

void textFileSnappyRead::close() {
    if (state == FILE_OPEN) {
        closeFile(stream);
        if (external_table_debug) {
            elog(LOG, "Datalake Log, close snappy text file %s successful.", path.c_str());
        }
    }
}

int textFileSnappyRead::convertBigEndianToInt(char *buffer) {
  int value = 0;
  for (unsigned int i = 0; i < sizeof(int); i++) {
    value = (value << 8) | (unsigned char)buffer[i];
  }
  return value;
}



}
}