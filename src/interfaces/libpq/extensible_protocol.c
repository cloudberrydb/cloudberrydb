/*
 * extensible_protocol.c
 * 	Support for extensible protocol types
 *
 *
 * Copyright (c) 2023, Cloudberry Database, HashData Technology Limited.
 *
 * IDENTIFICATION
 * 	src/interfaces/libpq/extensible_protocol.c
 */
#ifndef FRONTEND

#include "postgres.h"

#include "extensible_protocol.h"
#include "utils/hsearch.h"

static HTAB *extensible_protocol_types = NULL;

typedef struct
{
    char	extprotocoltype;
    const void* extprotocolmethods;
} ExtensibleProtocolEntry;

/*
 * An internal function to register a new protocol
 */
static void
RegisterExtensibleProtocolEntry(const char* extprotocoltype,
								const void *extprotocolmethods)
{
	ExtensibleProtocolEntry *entry;
	bool	found;

	if (extensible_protocol_types == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(ExtensibleProtocolEntry);
		ctl.entrysize = sizeof(ExtensibleProtocolEntry);

		extensible_protocol_types = hash_create("Extensible Protocol Hash", 100, &ctl,
					  					HASH_ELEM | HASH_STRINGS);
	}

	entry = (ExtensibleProtocolEntry *) hash_search(extensible_protocol_types,
											extprotocoltype,
											HASH_ENTER, &found);

	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("extensible protocol type \"%s\" already exists",
						extprotocoltype)));

	entry->extprotocolmethods = extprotocolmethods;
}

/*
 * Register a new type of protocol.
 */
void
RegisterExtensibleProtocolMethods(const ExtensibleProtocolMethods *methods)
{
	char *extprotocoltype = malloc(2 * sizeof(char));
	extprotocoltype[0] = methods->extprotocoltype;
	extprotocoltype[1] = '\0';

	RegisterExtensibleProtocolEntry(extprotocoltype,
				 				methods);
}

/*
 * An internal routine to get an ExtensibleProtocolEntry by the given identifier
 */
static const void *
GetExtensibleProtocolEntry(const char* extprotocoltype, bool missing_ok)
{
	ExtensibleProtocolEntry *entry = NULL;

	if (extensible_protocol_types != NULL)
		entry = (ExtensibleProtocolEntry *) hash_search(extensible_protocol_types,
												extprotocoltype,
												HASH_FIND, NULL);
	if (!entry)
	{
		if (missing_ok)
			return NULL;
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("ExtensibleProtocolMethods \"%s\" was not registered",
					extprotocoltype)));
	}

	return entry->extprotocolmethods;
}

/*
 * Get the methods for a given type of extensible protocol.
 */
const ExtensibleProtocolMethods *
GetExtensibleProtocolMethods(const char extprotocoltype, bool missing_ok)
{
	char *extprotocoltype_text = malloc(2 * sizeof(char));
	extprotocoltype_text[0] = extprotocoltype;
	extprotocoltype_text[1] = '\0';


	return (const ExtensibleProtocolMethods *)
		GetExtensibleProtocolEntry(extprotocoltype_text,
			     				missing_ok);
}

/*
 * An internal function to unregister a protocol
 */
static const void*
UnregisterExtensibleProtocolEntry(const char extprotocoltype, bool missing_ok)
{
	ExtensibleProtocolEntry *entry;

	if (extensible_protocol_types == NULL)
	{
		if (missing_ok)
			return NULL;
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("ExtensibleProtocolMethods \"%c\" was not registered",
					extprotocoltype)));
	}

	entry = (ExtensibleProtocolEntry *) hash_search(extensible_protocol_types,
						 					&extprotocoltype,
						 					HASH_REMOVE, NULL);

	if (!entry)
	{
		if (missing_ok)
			return NULL;
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("ExtensibleProtocolMethods \"%c\" was not registered",
					extprotocoltype)));
	}

	return entry->extprotocolmethods;
}

/*
 * Unregister a protoco type
 */
void
UnegisterExtensibleProtocolMethods(const ExtensibleProtocolMethods *methods)
{
	UnregisterExtensibleProtocolEntry(methods->extprotocoltype,
								methods);
}

#endif