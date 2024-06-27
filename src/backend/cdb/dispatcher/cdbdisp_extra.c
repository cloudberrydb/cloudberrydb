#include "postgres.h"

#include "cdb/cdbdisp_extra.h"
#include "libpq/pqformat.h"
#include "utils/hsearch.h"


static HTAB *ExtraDispTable = NULL;

typedef struct ExtraDispEntry
{
	char 		extraDispName[EXTRADISPNAME_MAX_LEN];
	PackFunc	packFunc;
	UnpackFunc	unpackFunc;
} ExtraDispEntry;

void
RegisterExtraDispatch(const char *extraDispName, PackFunc packFunc, UnpackFunc unpackFunc)
{
	ExtraDispEntry *entry;
	bool			found;

	if (ExtraDispTable == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = EXTRADISPNAME_MAX_LEN;
		ctl.entrysize = sizeof(ExtraDispEntry);

		ExtraDispTable = hash_create("extra dispatch info", 8, &ctl,
								HASH_ELEM | HASH_STRINGS);
	}

	if (strlen(extraDispName) >= EXTRADISPNAME_MAX_LEN)
		elog(ERROR, "extra dispatch name is too long");

	entry = (ExtraDispEntry *) hash_search(ExtraDispTable,
										   extraDispName,
										   HASH_ENTER, &found);
	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("extra dispatch name \"%s\" already exists",
						extraDispName)));

	entry->packFunc = packFunc;
	entry->unpackFunc = unpackFunc;
}

/* Return packaged messages. each message has the same format:
 * ("%d%s\0%s", totalLen, name, payload). totalLen is the message
 * length not including itself. name is the name of this message,
 * a following '\0' marks the end. payload is the main body of the
 * message.
 */
char *
PackExtraMsgs(int *len)
{
	HASH_SEQ_STATUS status;
	ExtraDispEntry *hentry;
	char		  **payloads;
	int			   *lengths;
	int				payloadLen;
	char		  **names;
	int				nameLen;
	char		   *total;
	int				totalLen;
	char		   *pos;
	int				tmp;
	int				n;
	int				i;

	if (!ExtraDispTable)
	{
		*len = 0;
		return NULL;
	}

	n = hash_get_num_entries(ExtraDispTable);
	payloads = (char **) palloc(n * sizeof(char *));
	lengths = (int *) palloc(n * sizeof(int));
	names = (char **) palloc(n * sizeof(char *));

	i = 0;
	totalLen = 0;
	hash_seq_init(&status, ExtraDispTable);
	while ((hentry = (ExtraDispEntry *) hash_seq_search(&status)) != NULL)
	{
		payloads[i] = (*(hentry->packFunc))(lengths + i);
		names[i] = hentry->extraDispName;
		totalLen += sizeof(int) + strlen(names[i]) + 1 + *(lengths + i);
		i++;
	}
	Assert(i = n);

	total = palloc(totalLen);
	pos = total;

	for(i=0; i < n; i++)
	{
		payloadLen = *(lengths + i);
		nameLen = strlen(names[i]);

		/* lenth */
		tmp = htonl(payloadLen + nameLen + 1);
		memcpy(pos, &tmp, sizeof(tmp));
		pos += sizeof(tmp);

		/* name */
		memcpy(pos, names[i], nameLen + 1);
		pos += nameLen + 1;

		/* payload */
		memcpy(pos, payloads[i], payloadLen);
		pos += payloadLen;

		pfree(payloads[i]);
	}

	Assert(pos - total == totalLen);

	pfree(names);
	pfree(payloads);
	pfree(lengths);

	*len = totalLen;
	return total;
}

void
UnPackExtraMsgs(StringInfo inputMsgs)
{
	ExtraDispEntry *entry;
	const char 	   *name;
	const char 	   *payload;
	int				payloadLen;
	int 			totalLen;
	bool			found;
	int				n;
	int				i;

	if (!ExtraDispTable)
		return;

	n = hash_get_num_entries(ExtraDispTable);
	i = n;

	while (inputMsgs->cursor < inputMsgs->len)
	{
		totalLen = pq_getmsgint(inputMsgs, 4);
		name = pq_getmsgstring(inputMsgs);
		payloadLen = totalLen - strlen(name) - 1;
		payload = pq_getmsgbytes(inputMsgs, payloadLen);

		entry = (ExtraDispEntry *) hash_search(ExtraDispTable,
											name,
											HASH_FIND, &found);
		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("extra dispatch %s not found", name)));

		(*(entry->unpackFunc))(payload, payloadLen);
		i--;
	}
	if (i != 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				errmsg("extra dispatch count mismatch, registered %d, get %d", n, i)));
}
