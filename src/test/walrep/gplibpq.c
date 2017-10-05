/*-------------------------------------------------------------------------
 *
 * Small tests for gp_libpqwalreceiver
 *
 *-----------------------------------------------------------------------*/

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/xlog_internal.h"
#include "replication/walprotocol.h"
#include "replication/walreceiver.h"
#include "cdb/cdbappendonlyam.h"
#include "funcapi.h"

PG_MODULE_MAGIC;

#define NAPTIME_PER_CYCLE 100	/* max sleep time between cycles (100ms) */

static struct
{
	XLogRecPtr	Write;
	XLogRecPtr	Flush;
} LogstreamResult;

static void test_XLogWalRcvProcessMsg(unsigned char type, char *buf,
									  Size len, XLogRecPtr *logStreamStart);
static void test_XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr);
static void test_XLogWalRcvSendReply(void);
static void test_PrintLog(char *type, XLogRecPtr walPtr,
						  TimestampTz sendTime);

#ifdef USE_SEGWALREP
static int check_ao_record_present(unsigned char type, char *buf, Oid spc_node,
								   Oid db_node, Oid rel_node, Size len,
								   uint64 eof[], uint32 xrecoff, bool aoco);
#endif		/* USE_SEGWALREP */

Datum test_connect(PG_FUNCTION_ARGS);
Datum test_disconnect(PG_FUNCTION_ARGS);
Datum test_receive(PG_FUNCTION_ARGS);
Datum test_send(PG_FUNCTION_ARGS);
Datum test_receive_and_verify(PG_FUNCTION_ARGS);
Datum test_xlog_ao(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(test_connect);
PG_FUNCTION_INFO_V1(test_disconnect);
PG_FUNCTION_INFO_V1(test_receive);
PG_FUNCTION_INFO_V1(test_send);
PG_FUNCTION_INFO_V1(test_receive_and_verify);
PG_FUNCTION_INFO_V1(test_xlog_ao);

static void
string_to_xlogrecptr(text *location, XLogRecPtr *rec)
{
	char *locationstr = DatumGetCString(
		DirectFunctionCall1(textout,
							PointerGetDatum(location)));

	if (sscanf(locationstr, "%X/%X", &rec->xlogid, &rec->xrecoff) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse transaction log location \"%s\"",
						locationstr)));
}

Datum
test_connect(PG_FUNCTION_ARGS)
{
	char *conninfo = TextDatumGetCString(PG_GETARG_DATUM(0));
	text *location = PG_GETARG_TEXT_P(1);
	bool result;
	XLogRecPtr startpoint;

	string_to_xlogrecptr(location, &startpoint);

	result = walrcv_connect(conninfo, startpoint);
	PG_RETURN_BOOL(result);
}

Datum
test_disconnect(PG_FUNCTION_ARGS)
{
	walrcv_disconnect();

	PG_RETURN_BOOL(true);
}

Datum
test_receive(PG_FUNCTION_ARGS)
{
	bool		result;
	unsigned char type;
	char	   *buf;
	int			len;

	result = walrcv_receive(NAPTIME_PER_CYCLE, &type, &buf, &len);

	PG_RETURN_BOOL(result);
}

Datum
test_send(PG_FUNCTION_ARGS)
{
	test_XLogWalRcvSendReply();

	PG_RETURN_BOOL(true);
}

#define NUM_RETRIES 50

Datum
test_receive_and_verify(PG_FUNCTION_ARGS)
{
	text	   *start_location = PG_GETARG_TEXT_P(0);
	text       *end_location = PG_GETARG_TEXT_P(1);
	XLogRecPtr startpoint;
	XLogRecPtr endpoint;
	unsigned char type;
	char   *buf;
	int     len;

	string_to_xlogrecptr(start_location, &startpoint);
	string_to_xlogrecptr(end_location, &endpoint);

	for (int i=0; i < NUM_RETRIES; i++)
	{
		if (walrcv_receive(NAPTIME_PER_CYCLE, &type, &buf, &len))
		{
			XLogRecPtr logStreamStart;
			/* Accept the received data, and process it */
			test_XLogWalRcvProcessMsg(type, buf, len, &logStreamStart);

			/* Compare received everthing from start */
			if (startpoint.xlogid != logStreamStart.xlogid ||
				startpoint.xrecoff != logStreamStart.xrecoff)
			{
				elog(ERROR, "Start point (%X/%X) differs from expected (%X/%X)",
					 logStreamStart.xlogid, logStreamStart.xrecoff,
					 startpoint.xlogid, startpoint.xrecoff);
			}

			/* Compare received everything till end */
			if (endpoint.xlogid != LogstreamResult.Write.xlogid ||
				endpoint.xrecoff != LogstreamResult.Write.xrecoff)
			{
				elog(ERROR, "End point (%X/%X) differs from expected (%X/%X)",
					 LogstreamResult.Write.xlogid, LogstreamResult.Write.xrecoff,
					 endpoint.xlogid, endpoint.xrecoff);
			}

			PG_RETURN_BOOL(true);
		}

		elog(LOG, "walrcv_receive didn't return anything, retry...%d", i);
	}

	PG_RETURN_BOOL(false);
}

/*
 * Accept the message from XLOG stream, and process it.
 */
static void
test_XLogWalRcvProcessMsg(unsigned char type, char *buf,
						  Size len, XLogRecPtr *logStreamStart)
{
	switch (type)
	{
		case 'w':				/* WAL records */
			{
				WalDataMessageHeader msghdr;

				if (len < sizeof(WalDataMessageHeader))
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg_internal("invalid WAL message received from primary")));
				/* memcpy is required here for alignment reasons */
				memcpy(&msghdr, buf, sizeof(WalDataMessageHeader));
				logStreamStart->xlogid = msghdr.dataStart.xlogid;
				logStreamStart->xrecoff = msghdr.dataStart.xrecoff;

				test_PrintLog("wal start records",
							  msghdr.dataStart, msghdr.sendTime);
				test_PrintLog("wal end records",
							  msghdr.walEnd, msghdr.sendTime);

				buf += sizeof(WalDataMessageHeader);
				len -= sizeof(WalDataMessageHeader);

				test_XLogWalRcvWrite(buf, len, msghdr.dataStart);
				break;
			}
		case 'k':				/* Keepalive */
			{
				PrimaryKeepaliveMessage keepalive;

				if (len != sizeof(PrimaryKeepaliveMessage))
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg_internal("invalid keepalive message received from primary")));
				/* memcpy is required here for alignment reasons */
				memcpy(&keepalive, buf, sizeof(PrimaryKeepaliveMessage));

				elog(INFO, "keep alive: %X/%X at %s",
						keepalive.walEnd.xlogid,
						keepalive.walEnd.xrecoff,
						timestamptz_to_str(keepalive.sendTime));
				test_PrintLog("keep alive",
							  keepalive.walEnd, keepalive.sendTime);
				break;
			}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg_internal("invalid replication message type %d",
									 type)));
	}
}

/*
 * Write XLOG data to disk.
 */
static void
test_XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr)
{
	static char		   *recvFilePath = NULL;
	static TimeLineID recvFileTLI = 1;
	static uint32 recvId = 0;
	static uint32 recvSeg = 0;

	while (nbytes > 0)
	{
		int			segbytes;
		int			startoff;
		int			byteswritten;

		if (recvFilePath == NULL || !XLByteInSeg(recptr, recvId, recvSeg))
		{
			if (recvFilePath == NULL)
			{
				recvFilePath = palloc0(MAXFNAMELEN);
			}

			XLByteToSeg(recptr, recvId, recvSeg);
			XLogFileName(recvFilePath, recvFileTLI, recvId, recvSeg);
			elog(DEBUG1, "would open: %s", recvFilePath);
			recvFileTLI = 1;
		}

		startoff = recptr.xrecoff % XLogSegSize;

		if (startoff + nbytes > XLogSegSize)
			segbytes = XLogSegSize - startoff;
		else
			segbytes = nbytes;

		/* Assuming segbytes are written */
		byteswritten = segbytes;

		XLByteAdvance(recptr, byteswritten);

		nbytes -= byteswritten;
		buf += byteswritten;

		LogstreamResult.Write = recptr;
		/* Assuming it's flushed too */
		LogstreamResult.Flush = recptr;
	}
}

static void
test_XLogWalRcvSendReply(void)
{
	char		buf[sizeof(StandbyReplyMessage) + 1];
	TimestampTz now;
	static StandbyReplyMessage reply_message;

	now = GetCurrentTimestamp();

	reply_message.write = LogstreamResult.Write;
	reply_message.flush = LogstreamResult.Flush;
	reply_message.apply = LogstreamResult.Flush;
	reply_message.sendTime = now;

	elog(DEBUG1, "sending: write = %X/%X, flush = %X/%X, apply = %X/%X at %s",
			reply_message.write.xlogid,
			reply_message.write.xrecoff,
			reply_message.flush.xlogid,
			reply_message.flush.xrecoff,
			reply_message.apply.xlogid,
			reply_message.apply.xrecoff,
			timestamptz_to_str(now));

	buf[0] = 'r';
	memcpy(&buf[1], &reply_message, sizeof(StandbyReplyMessage));
	walrcv_send(buf, sizeof(StandbyReplyMessage) + 1);
}

/*
 * Just show the walEnd/sendTime information
 */
static void
test_PrintLog(char *type, XLogRecPtr walPtr,
						   TimestampTz sendTime)
{
	elog(DEBUG1, "%s: %X/%X at %s", type, walPtr.xlogid, walPtr.xrecoff,
		 timestamptz_to_str(sendTime));
}


/*
 * Verify that XLOG records are being generated for AO tables and are getting
 * shipped to the WAL receiver.
 */
Datum
test_xlog_ao(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	int			nattr = 2;
	Datum    values[2];
	bool   nulls[2];
	HeapTuple tuple;

    MemSet(nulls, false, sizeof(nulls));
   
	tupdesc = CreateTemplateTupleDesc(nattr, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "insert_count", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "truncate_count", INT4OID, -1, 0);

	tupdesc = BlessTupleDesc(tupdesc);

	int		  num_found = 0;

#ifdef USE_SEGWALREP
	char         *conninfo = TextDatumGetCString(PG_GETARG_DATUM(0));
	text         *start_location = PG_GETARG_TEXT_P(1);
	Oid           spc_node = PG_GETARG_OID(2);
	Oid           db_node = PG_GETARG_OID(3);
	Oid           rel_node = PG_GETARG_OID(4);
	uint64       *eof = (uint64 *)ARR_DATA_PTR(PG_GETARG_ARRAYTYPE_P(5));
	bool		  aoco = PG_GETARG_BOOL(6);
	XLogRecPtr    startpoint;
	unsigned char type;
	char         *buf;
	int           len;
	uint32         xrecoff;

	string_to_xlogrecptr(start_location, &startpoint);
	xrecoff = startpoint.xrecoff;


	if (!walrcv_connect(conninfo, startpoint))
		elog(ERROR, "could not connect");

	for (int i = 0; i < NUM_RETRIES; i++)
	{
		if (walrcv_receive(NAPTIME_PER_CYCLE, &type, &buf, &len))
		{
			num_found = check_ao_record_present(type, buf, spc_node, db_node,
												rel_node, len, eof, xrecoff, aoco);
			break;
		}
		else
			elog(LOG, "walrcv_receive didn't return anything, retry...%d", i);
	}

	walrcv_disconnect();
#endif		/* USE_SEGWALREP */

	values[0] = Int32GetDatum(num_found);
	values[1] = Int32GetDatum(0);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

#ifdef USE_SEGWALREP
/*
 * Verify that AO/AOCO XLOG record is present in buf.
 * Returns the number of AO/AOCO XLOG records found in buf.
 */
static int
check_ao_record_present(unsigned char type, char *buf, Oid spc_node,
						Oid db_node, Oid rel_node, Size len, uint64 eof[100],
						uint32 xrecoff, bool aoco)
{
	WalDataMessageHeader msghdr;
	uint32               i = 0;
	int                  num_found = 0;
	int 				 segfilenum = 0;

	if (type != 'w')
		return false;

	if (len < sizeof(WalDataMessageHeader))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg_internal("invalid WAL message received from primary")));

	Assert(buf != NULL);

	/* memcpy is required here for alignment reasons */
	memcpy(&msghdr, buf, sizeof(WalDataMessageHeader));

	test_PrintLog("wal start record", msghdr.dataStart, msghdr.sendTime);
	test_PrintLog("wal end record", msghdr.walEnd, msghdr.sendTime);

	buf += sizeof(WalDataMessageHeader);
	len -= sizeof(WalDataMessageHeader);

	/* process the xlog records one at a time and check if it is an AO/AOCO record */
	while (i < len)
	{
		XLogRecord 		   *xlrec = (XLogRecord *)(buf + i);
		XLogPageHeaderData *hdr = (XLogPageHeaderData *)xlrec;
		XLogContRecord     *contrecord;
		uint32 			    avail_in_block = XLOG_BLCKSZ - ((xrecoff + i) % XLOG_BLCKSZ);

		elog(DEBUG1, "len/offset:%u/%u, avail_in_block = %u",
			 xlrec->xl_tot_len, i, avail_in_block);

		if (hdr->xlp_magic == XLOG_PAGE_MAGIC)
		{
			/*
			 * If we encounter a page header, check if it is a continuation
			 * record and skip the page header and the remaining data to move
			 * on to the next xlog record. If it is not a continuation record,
			 * skip only the page header and move on to the next record.
			 */
			if (hdr->xlp_info & XLP_FIRST_IS_CONTRECORD)
			{
				contrecord = (XLogContRecord *)((char *)hdr + XLogPageHeaderSize(hdr));
				elog(DEBUG1, "remaining length of record = %u", contrecord->xl_rem_len);
				i += MAXALIGN(XLogPageHeaderSize(hdr) + SizeOfXLogContRecord +
							  contrecord->xl_rem_len);
			}
			else
				i += XLogPageHeaderSize(hdr);
		}
		else if (xlrec->xl_rmid == RM_APPEND_ONLY_ID)
		{
			xl_ao_insert *xlaoinsert = palloc0(xlrec->xl_tot_len);

			if (xlrec->xl_tot_len > avail_in_block)
			{
				/*
				 * The AO record is split across two pages and needs to be
				 * reassembled
				 */
				char aobuf[XLOG_BLCKSZ];
				memcpy(aobuf, xlrec, avail_in_block);
				elog(DEBUG1, "copied partial record of %u bytes", avail_in_block);
				contrecord = (XLogContRecord *)((char *)xlrec + avail_in_block +
							  XLogPageHeaderSize(hdr));
				memcpy(aobuf + avail_in_block, ((char *)contrecord + SizeOfXLogContRecord),
					   contrecord->xl_rem_len);
				memcpy(xlaoinsert, XLogRecGetData(aobuf), xlrec->xl_tot_len);
				i += avail_in_block +
					 MAXALIGN(XLogPageHeaderSize(hdr) + SizeOfXLogContRecord + contrecord->xl_rem_len);
			}
			else
			{
				memcpy(xlaoinsert, XLogRecGetData(xlrec), sizeof(xl_ao_insert));
				i += MAXALIGN(xlrec->xl_tot_len);
			}

			if (aoco)
				segfilenum = AOTupleId_MultiplierSegmentFileNum * num_found + 1;
			else
				segfilenum = 1;

			if (xlaoinsert->node.spcNode == spc_node &&
				xlaoinsert->node.dbNode == db_node &&
				xlaoinsert->node.relNode == rel_node &&
				xlaoinsert->segment_filenum == segfilenum &&
				xlaoinsert->offset == 0 &&
				xlrec->xl_len - SizeOfAOInsert == eof[num_found])
				num_found++;
			else
			{
				elog(INFO, "Expected values: relfile %u/%u/%u segfile/offset %u/%u eof %lu",
					 spc_node, db_node, rel_node, segfilenum, 0, eof[num_found]);
				elog(INFO, "Actual values: relfile %u/%u/%u segfile/offset %u/%lu eof %lu",
					 xlaoinsert->node.spcNode, xlaoinsert->node.dbNode,
					 xlaoinsert->node.relNode, xlaoinsert->segment_filenum,
					 xlaoinsert->offset, xlrec->xl_len - SizeOfAOInsert);
			}
		}
		else
		{
			/*
			 * Either the entire record is too long to fit into the current
			 * page or there is not enough space remaining in the current page
			 * to write the XLogHeader. So skip the remaining bytes to move
			 * onto the next page.
			 */
			if (xlrec->xl_tot_len > avail_in_block || avail_in_block < SizeOfXLogRecord)
				i += avail_in_block;
			else
				i += MAXALIGN(xlrec->xl_tot_len);
		}
	}
	return num_found;
}
#endif		/* USE_SEGWALREP */
