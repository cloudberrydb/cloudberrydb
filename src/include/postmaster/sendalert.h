#ifndef _SENDALERT_H
#define _SENDALERT_H

#include "postmaster/syslogger.h"

#define MAX_ALERT_STRING 127

extern int send_alert(const GpErrorData * errorData);

extern int send_alert_from_chunks(const PipeProtoChunk *chunk, const PipeProtoChunk * saved_chunks_in);

extern char *get_str_from_chunk(CSVChunkStr *chunkstr, const PipeProtoChunk *saved_chunks);

#endif 
