/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_HashMap_priv_h
#define __pljava_HashMap_priv_h

#include "pljava/PgObject_priv.h"
#include "pljava/Iterator.h"
#include "pljava/HashMap.h"

#ifdef __cplusplus
extern "C" {
#endif

/*****************************************************************
 * Private section of the HashMap class
 *
 * @author Thomas Hallgren
 *****************************************************************/

struct HashKeyClass_;
typedef struct HashKeyClass_* HashKeyClass;

struct HashKeyClass_
{
	struct PgObjectClass_ extendedClass;

	/*
	 * Return the hashCode of self.
	 */
	uint32 (*hashCode)(HashKey self);

	/*
	 * Return true if self is equal to other.
	 */
	bool (*equals)(HashKey self, HashKey other);

	/*
	 * Create a copy of self in MemoryContext ctx.
	 */
	HashKey (*clone)(HashKey self, MemoryContext ctx);
};

struct HashKey_
{
	HashKeyClass m_class;
};

/*
 * HashKey for Oid.
 */
struct OidKey_
{
	struct HashKey_ HashKey_extension;
	
	Oid key;
};
typedef struct OidKey_* OidKey;

/*
 * HashKey for an Opaque pointer, uses the pointer itself
 * as the hash value.
 */
struct OpaqueKey_
{
	struct HashKey_ HashKey_extension;
	
	void* key;
};
typedef struct OpaqueKey_* OpaqueKey;

/*
 * HashKey for strings.
 */
struct StringKey_
{
	struct HashKey_ HashKey_extension;

	/* We preserve the computed hashcode here.
	 */
	uint32 hash;

	const char* key;
};
typedef struct StringKey_* StringKey;

/*
 * Default clone method. Allocates a new instance in the given MemoryContext
 * and copies the orginial HashKey using memcpy and the size stated in the
 * class.
 */
extern HashKey _HashKey_clone(HashKey self, MemoryContext ctx);

/*
 * Allocate a HashKeyClass for instances of a specific class.
 */
extern HashKeyClass HashKeyClass_alloc(const char* className, Size instanceSize, Finalizer finalizer);

struct HashMap_
{
	struct PgObject_ PgObject_extension;
	Entry* table;
	uint32 tableSize;
	uint32 size;
};

struct Entry_
{
	struct PgObject_ PgObject_extension;
	HashKey key;
	void*   value;
	Entry   next;
};

#define Entry_create(ctx) ((Entry)PgObjectClass_allocInstance(s_EntryClass, ctx))

#define HASHSLOT(self, key) (HashKey_hashCode(key) % self->tableSize)

#ifdef __cplusplus
} /* end of extern "C" declaration */
#endif
#endif
