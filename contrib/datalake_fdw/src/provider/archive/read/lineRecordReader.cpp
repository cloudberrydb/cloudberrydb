#include "lineRecordReader.h"
// #include "src/Provider/Provider.h"
#include "src/provider/provider.h"
extern "C"
{
#include "utils/elog.h"
// #include "src/log.h"
}

namespace Datalake {
namespace Internal {

void lineRecordReader::updateSplit(metaInfo info, bool finallyBlock) {
    start = info.rangeOffset;
    if (pos == 0) {
        pos = start;
    }
    end = info.rangeOffsetEnd;
    maxLineLength = info.blockSize;
    path = info.fileName;
    maxBytesToConsume = info.blockSize;
    fileLength = info.fileLength;
    finalBlock = finallyBlock;
    // for multi-delimiter
    delPosn = 0;
}

void lineRecordReader::updateNewReader(textFileInput* reader) {
    this->reader = reader;
}

bool lineRecordReader::defaultNext(int &length) {
    while(true) {
        int nread = readLine();
        if (nread == -1) {
            /* read block over. need extra fetch a line */
            extra_fetch_line = true;
            continue;
        } else if (nread == -2) {
            /* file read over */
            length = 0;
            return false;
        }
        pos += nread;
        length += nread;
        if (state == LINE_RECORD_FOUND_DELIMTER) {
            /* we found delimiter so curBytesConsumed finish set zero. */
            curBytesConsumed = 0;
            if (extra_fetch_line) {
                /* fetch final line record */
                extra_fetch_line = false;
                state = LINE_RECORD_FETCH_FINAL_LINE;
            }
            return true;
        }
        break;
    }
    return false;
}

bool lineRecordReader::customNext(int &length) {
    while(true) {
        int nread = readLine();
        if (nread == -1) {
            if (finalBlock) {
                /* if final block need extra fetch two line */
                extra_fetch_line_num = 2;
                if (external_table_debug) {
                    elog(LOG, "external table, file %s read final block current postition %ld,\
						 current block start position %ld end position %ld.",
						 path.c_str(), pos, start, end);
                }
                continue;
            } else {
                /* block read over. fetch next block */
                state = LINE_RECORD_FETCH_FINAL_LINE;
                length = 0;
                return false;
            }
        } else if (nread == -2) {
            /* file read over */
            length = 0;
            return false;
        }
        pos += nread;
        length += nread;
        if (state == LINE_RECORD_FOUND_DELIMTER) {
            /* we found delimiter so curBytesConsumed finish set zero. */
            curBytesConsumed = 0;
            if (extra_fetch_line_num > 0) {
                if (external_table_debug) {
                    elog(LOG, "external table, file %s read final block current postition %ld \
                        		extra fetch number %d.",
                        		path.c_str(), pos, extra_fetch_line_num);
                }
                extra_fetch_line_num--;
            }
            if (extra_fetch_line_num == 0) {
                /* fetch final line record */
                state = LINE_RECORD_FETCH_FINAL_LINE;
                extra_fetch_line_num = -1;
            }
            return true;
        } else if (state == LINE_CUSTOM_RECORD_INCOMPLETE_DELIMITER) {
            return false;
        }
        break;
    }
    return false;
}

bool lineRecordReader::next(int &length) {
    if (recordDelimiter) {
        return customNext(length);
    } else {
        return defaultNext(length);
    }
}


int64_t lineRecordReader::readLine() {
    if (recordDelimiter) {
        return readCustomLine();
    } else {
        return readDefaultLine();
    }
}

int64_t lineRecordReader::fillBuffer() {
    int64_t leftBytes = 0;
    if (extra_fetch_line || (extra_fetch_line_num > 0)) {
        leftBytes = buffer->buffer_length;
    } else {
        leftBytes = end - pos >= buffer->buffer_length ? buffer->buffer_length : (end - pos);
    }

    int nread = reader->read(buffer->buffer, leftBytes);
    buffer->pos = 0;
    buffer->used_size = nread;
    buffer->begin_size = pos;
    buffer->buffer[nread] = '\0';
    return nread;
}

int64_t lineRecordReader::readDefaultLine() {
    int newLineLength = 0;
    int64_t bytesConsumed = 0;
    char LF = '\n';
    char CR = '\r';
    bool prevCharCR = false;

    if (pos >= end) {
        /**
         * After the current block is fully consumed, there are three possible
         * scenarios:
         * 1. The current block is completely processed, and there is more data
         *    available in the file or stream. In this case, the system needs to
         *    read the next block to continue processing.
         *
         * 2. The current block is fully processed, and there is no more data
         *    available in the file or stream. This indicates that the end of the
         *    file or stream has been reached, and the processing loop can be
         *    terminated.
         *
         * 3. The current block is partially processed, and there is a need to
         *    fetch additional data to complete the processing. This could happen if
         *    the delimiter or end of line is not found within the current block,
         *    and we need to fetch more data to search for the delimiter in the
         *    next block.
         */
        if (extra_fetch_line) {
            /* line record read block over. but we need fetch next line. */
            state = LINE_RECORD_EXTRA_FETCH_LINE;
        } else if (end == fileLength) {
            state = LINE_RECORD_FETCH_FINAL_LINE;
            return -2;
        } else {
            state = LINE_RECORD_READ_BLOCK_OVER;
            return -1;
        }
    }

    /* read data into memroyBuffer. */
    if (buffer->pos >= buffer->used_size) {
        int64_t nread = fillBuffer();
        if (nread == 0) {
            state = LINE_RECORD_READ_BLOCK_OVER;
            return -1;
        }
    }

    /**
     * We set the read limit to ensure that the maximum return size per
     * read does not exceed 64KB. This is because the buffer used for
     * the copy operation is set to 64KB.
     */
    int leftBytes = readLimit + buffer->pos <= buffer->used_size ?
            readLimit : (buffer->used_size - buffer->pos);

    /* loop found delimiter. */
    bufferStartPosn = buffer->pos;
    for(int i = 0; i < leftBytes; i++) {
        if (buffer->buffer[buffer->pos] == LF) {
            newLineLength = (prevCharCR) ? 2 : 1;
            buffer->pos++;
            break;
        }
        if (prevCharCR) {
            newLineLength = 1;
            break;
        }
        prevCharCR = (buffer->buffer[buffer->pos] == CR);
        buffer->pos++;
    }
    int readLength = buffer->pos - bufferStartPosn;

    if (prevCharCR && newLineLength == 0) {
        --readLength;
    }

    bytesConsumed += readLength;
    curBytesConsumed += bytesConsumed;

    /* We do not allow the delimiter to remain unfound within a block. */
    if (curBytesConsumed > maxBytesToConsume && !(state == LINE_RECORD_EXTRA_FETCH_LINE)) {
        elog(ERROR, "External table, lineRecordReader in file %s consumed %ld "
            "bytes more than max consume bytes %ld.", path.c_str(), curBytesConsumed,
            maxBytesToConsume);
    }

    if (newLineLength == 0) {
        state = LINE_RECORD_NOT_FOUND_DELIMTER;
    } else {
        state = LINE_RECORD_FOUND_DELIMTER;
    }

    return bytesConsumed;
}

int64_t lineRecordReader::readCustomLine() {

    int64_t bytesConsumed = 0;

    if (pos >= end) {
        if (end == fileLength) {
            state = LINE_RECORD_FETCH_FINAL_LINE;
            return -2;
        } if (extra_fetch_line_num) {
            /* line record read block over. but we need fetch next line. */
            state = LINE_RECORD_EXTRA_FETCH_LINE;
        } else {
            state = LINE_RECORD_READ_BLOCK_OVER;
            return -1;
        }
    }

    /* read data into memroyBuffer. */
    if (buffer->pos >= buffer->used_size) {
        int64_t nread = fillBuffer();
        if (nread == 0) {
            state = LINE_RECORD_READ_BLOCK_OVER;
            return -1;
        }
    }

    int leftBytes = readLimit + buffer->pos <= buffer->used_size ?
        readLimit : (buffer->used_size - buffer->pos);

    bufferStartPosn = buffer->pos;

    bool found_delimiter = false;
    int64_t bufferLeftBytes = buffer->pos + leftBytes;
    for (;buffer->pos < bufferLeftBytes; buffer->pos++) {
        if (buffer->buffer[buffer->pos] == recordDelimiter[delPosn]) {
            delPosn++;
            if (delPosn >= (int)strlen(recordDelimiter)) {
                buffer->pos++;
                found_delimiter = true;
                break;
            }
        } else if (delPosn != 0) {
            delPosn = 0;
            state = LINE_CUSTOM_RECORD_INCOMPLETE_DELIMITER;
        }
    }

    int readLength = buffer->pos - bufferStartPosn;

    bytesConsumed += readLength;
    curBytesConsumed += bytesConsumed;

    /* We do not allow the delimiter to remain unfound within a block. */
    if (curBytesConsumed > maxBytesToConsume && !(state == LINE_RECORD_EXTRA_FETCH_LINE)) {
        ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
						errmsg("External Error, lineRecordReader in file %s consumed %ld "
            "bytes more than max consume bytes %ld. current pos %ld.", path.c_str(), curBytesConsumed,
            maxBytesToConsume, pos + readLength)));
    }

    if (found_delimiter) {
        state = LINE_RECORD_FOUND_DELIMTER;
    } else if (state == LINE_CUSTOM_RECORD_INCOMPLETE_DELIMITER) {

    } else {
        state = LINE_RECORD_NOT_FOUND_DELIMTER;
    }

    return bytesConsumed;
}

char* lineRecordReader::reverse_strstr(const char* haystack, const char* needle) {
    int haystack_len = strlen(haystack);
    int needle_len = strlen(needle);

    for (int i = haystack_len - needle_len; i >= 0; --i) {
        if (memcmp(haystack + i, needle, needle_len) == 0) {
            return (char*)(haystack + i);
        }
    }
    return NULL;
}

int64_t lineRecordReader::fetchPreviousBlockLastDelimiter() {
    int64_t previsousEnd = start;
    int64_t previsousBegin = start - 32*1024*1024;
    int blockSize = 8*1024*1024;
    int64_t tmpPosn = previsousEnd;

    bool lastDelimiter = false;
    int64_t lastPosn = 0;
    while(tmpPosn > previsousBegin) {
        tmpPosn -= blockSize;
        reader->seek(tmpPosn);
        fillBuffer();
        char* last = reverse_strstr(buffer->buffer, recordDelimiter);
        if (last == NULL) {
            continue;
        } else {
            int relativePosn = last - buffer->buffer;
            lastPosn = tmpPosn + relativePosn;
            lastDelimiter = true;
        }
    }

    if (!lastDelimiter) {
        elog(ERROR, "External Error, %s block end postion %ld not "
            "found previous block last delimiter.", path.c_str(), previsousEnd);
    }
    return lastPosn;
}

int64_t lineRecordReader::fetchLoopBeginBlockDelimiter(int64_t previousLastDelimiterPosn) {
    int64_t loopBeginPosn = start;
    int64_t loopEndPosn = end;
    int64_t blockSize = 8*1024*1024;
    int64_t tmpPosn = previousLastDelimiterPosn;
    int64_t firstPons = 0;
    bool foundDelimter = false;
    reader->seek(tmpPosn);
    while(tmpPosn < loopEndPosn) {
        tmpPosn += blockSize;
        int64_t nread = fillBuffer();
        if (nread == 0) {
            break;
        }
        char *result = strstr(buffer->buffer, recordDelimiter);
        if (result == NULL) {
            continue;
        } else {
            int relativePosn = result - buffer->buffer;
            firstPons = tmpPosn + relativePosn;
            foundDelimter = true;
        }
    }

    if (!foundDelimter) {
        elog(ERROR, "External Error, %s block begin postion %ld not "
            "found first delimiter.", path.c_str(), loopBeginPosn);
    }
    return firstPons;
}

int64_t lineRecordReader::processCrossBlockPosition() {
    int64_t crossBlockPson = 0;
    if (loopBegin) {
        loopBegin = false;
        int64_t previousLastDelimiterPosn = fetchPreviousBlockLastDelimiter();
        crossBlockPson = fetchLoopBeginBlockDelimiter(previousLastDelimiterPosn);
        reader->seek(start);
        buffer->pos = 0;
        buffer->used_size = 0;
    }
    return crossBlockPson;
}

void lineRecordReader::resetLineRecordReader() {
    start = 0;
    end = 0;
    pos = 0;
    curBytesConsumed = 0;
    path = "";
    reader = NULL;
    state = LINE_RECORD_INIT;
    bufferStartPosn = 0;
    fileLength = 0;
    delPosn = 0;
    extra_fetch_line_num = -1;
    finalBlock = false;
    extra_fetch_line = false;
}

}
}