/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "pljava/HashMap_priv.h"

HashKey HashKey_clone(HashKey self, MemoryContext ctx)
{
	return self->m_class->clone(self, ctx);
}

bool HashKey_equals(HashKey self, HashKey other)
{
	return self->m_class->equals(self, other);
}

uint32 HashKey_hashCode(HashKey self)
{
	return self->m_class->hashCode(self);
}

HashKey _HashKey_clone(HashKey self, MemoryContext ctx)
{
	Size sz = ((PgObjectClass)self->m_class)->instanceSize;
	HashKey clone = (HashKey)MemoryContextAlloc(ctx, sz);
	memcpy(clone, self, sz);
	return clone;
}

HashKeyClass HashKeyClass_alloc(const char* className, Size instanceSize, Finalizer finalizer)
{
	HashKeyClass self = (HashKeyClass)MemoryContextAlloc(TopMemoryContext, sizeof(struct HashKeyClass_));
	PgObjectClass_init((PgObjectClass)self, className, instanceSize, finalizer);
	self->clone = _HashKey_clone;
	return self;
}

static HashKeyClass s_OidKeyClass;
static HashKeyClass s_StringKeyClass;
static HashKeyClass s_OpaqueKeyClass;

static PgObjectClass s_EntryClass;
static PgObjectClass s_HashMapClass;

/*
 * We use the Oid itself as the hashCode.
 */
static uint32 _OidKey_hashCode(HashKey self)
{
	return (uint32)((OidKey)self)->key;
}

/*
 * Compare with another HashKey.
 */
static bool _OidKey_equals(HashKey self, HashKey other)
{
	return other->m_class == self->m_class /* Same class */
		&& ((OidKey)self)->key == ((OidKey)other)->key;
}

static void OidKey_init(OidKey self, Oid keyVal)
{
	((HashKey)self)->m_class = s_OidKeyClass;
	self->key = keyVal;
}

/*
 * Create a copy of this StringKey
 */
static HashKey _StringKey_clone(HashKey self, MemoryContext ctx)
{
	HashKey clone = _HashKey_clone(self, ctx);
	((StringKey)clone)->key = MemoryContextStrdup(ctx, ((StringKey)self)->key);
	return clone;
}

/*
 * Compare with another HashKey.
 */
static bool _StringKey_equals(HashKey self, HashKey other)
{
	return other->m_class == self->m_class /* Same class */
		&& strcmp(((StringKey)self)->key, ((StringKey)other)->key) == 0;
}

static void _StringKey_finalize(PgObject self)
{
	pfree((char*)((StringKey)self)->key);
}

/*
 * Returns a hash code for this string. The hash code for a
 * string object is computed as
 *
 * s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
 *
 * (The hash value of the empty string is zero.)
 *
 * @return  a hash code value for this object.
 */
static uint32 _StringKey_hashCode(HashKey self)
{
	const char* val = ((StringKey)self)->key;
	uint32 h = ((StringKey)self)->hash;
	if(h == 0)
	{
		uint32 c;
		while((c = *val++) != 0)
			h = 31 * h + c;
        ((StringKey)self)->hash = h;
	}
	return h;
}

static void StringKey_init(StringKey self, const char* keyVal)
{
	((HashKey)self)->m_class = s_StringKeyClass;
	self->key = keyVal; /* a private copy is made when the key is made permanent */
	self->hash = 0;
}

/*
 * We use the Oid itself as the hashCode.
 */
static uint32 _OpaqueKey_hashCode(HashKey self)
{
	Ptr2Long p2l;
	p2l.longVal = 0L; /* ensure that the rest is zeroed out */
	p2l.ptrVal = ((OpaqueKey)self)->key;
	return (uint32)(p2l.longVal >> 3);
}

/*
 * Compare with another HashKey.
 */
static bool _OpaqueKey_equals(HashKey self, HashKey other)
{
	return other->m_class == self->m_class /* Same class */
		&& ((OpaqueKey)self)->key == ((OpaqueKey)other)->key;
}

static void OpaqueKey_init(OpaqueKey self, void* keyVal)
{
	((HashKey)self)->m_class = s_OpaqueKeyClass;
	self->key = keyVal;
}

/*
 * An Entry that holds the binding between the
 * key and an associated value.
 */
static PgObjectClass s_EntryClass;

HashKey Entry_getKey(Entry self)
{
	return self->key;
}

void* Entry_getValue(Entry self)
{
	return self->value;
}

void* Entry_setValue(Entry self, void* value)
{
	void* old = self->value;
	self->value = value;
	return old;
}

static void _Entry_finalize(PgObject self)
{
	PgObject_free((PgObject)((Entry)self)->key);
}

static void HashMap_rehash(HashMap self, uint32 newCapacity)
{
	Entry* oldTable = self->table;
	uint32 top = self->tableSize;
	uint32 idx;

	Entry* newTable = (Entry*)MemoryContextAlloc(GetMemoryChunkContext(self), newCapacity * sizeof(Entry));
	memset(newTable, 0, newCapacity * sizeof(Entry));
	self->table = newTable;
	self->tableSize = newCapacity;

	/* Move all old slots to the new table
	 */
	for(idx = 0; idx < top; ++idx)
	{
		Entry e = oldTable[idx];
		while(e != 0)
		{
			Entry eNext = e->next;
			HashKey eKey = e->key;
			uint32 slotNo = HASHSLOT(self, eKey);
			e->next = newTable[slotNo];
			newTable[slotNo] = e;
			e = eNext;
		}
	}
	pfree(oldTable);
}

void HashMap_clear(HashMap self)
{
	/* Delete all Entries before deleting the table and
	 * self.
	 */
	if(self->size > 0)
	{
		Entry* table = self->table;
		uint32 top = self->tableSize;
		uint32 idx;
		for(idx = 0; idx < top; ++idx)
		{
			Entry e = table[idx];
			table[idx] = 0;
			while(e != 0)
			{
				Entry eNext = e->next;
				PgObject_free((PgObject)e);
				e = eNext;
			}
		}
		self->size = 0;
	}
}

static void _HashMap_finalize(PgObject self)
{
	/* Delete all Entries before deleting the table and
	 * self.
	 */
	HashMap_clear((HashMap)self);
	pfree(((HashMap)self)->table);
}

HashMap HashMap_create(uint32 initialCapacity, MemoryContext ctx)
{
	HashMap self;
	if(ctx == 0)
		ctx = CurrentMemoryContext;

	self = (HashMap)PgObjectClass_allocInstance(s_HashMapClass, ctx);

	if(initialCapacity < 13)
		initialCapacity = 13;

	self->table = (Entry*)MemoryContextAlloc(ctx, initialCapacity * sizeof(Entry));
	memset(self->table, 0, initialCapacity * sizeof(Entry));
	self->tableSize = initialCapacity;
	self->size = 0;
	return self;
}

Iterator HashMap_entries(HashMap self)
{
	return Iterator_create(self);
}

void* HashMap_get(HashMap self, HashKey key)
{
	Entry slot;
	slot = self->table[HASHSLOT(self, key)];
	while(slot != 0)
		{
		if(HashKey_equals(slot->key, key))
			break;
		slot = slot->next;
		}
	return (slot == 0) ? 0 : slot->value;
}

void* HashMap_put(HashMap self, HashKey key, void* value)
{
	void* old = 0;
	uint32 slotNo = HASHSLOT(self, key);
	Entry  slot = self->table[slotNo];
	while(slot != 0)
	{
		if(HashKey_equals(slot->key, key))
			break;
		slot = slot->next;
	}
	if(slot == 0)
	{
		uint32 currSz = self->size;
		MemoryContext ctx = GetMemoryChunkContext(self);
		if((currSz + currSz / 2) > self->tableSize)
			{
			/* Always double the size. It gives predictable repositioning
			 * of entries (a necessity if an Iteator is created some
			 * time in the future.
			 */
			HashMap_rehash(self, self->tableSize * 2);
			slotNo = HASHSLOT(self, key);
			}
		slot = Entry_create(ctx);
		slot->key   = HashKey_clone(key, ctx);	/* Create a private copy of the key */
		slot->value = value;
		slot->next  = self->table[slotNo];
		self->table[slotNo] = slot;
		self->size++;
	}
	else
	{
		old = slot->value;
		slot->value = value;
	}
	return old;
}

void* HashMap_remove(HashMap self, HashKey key)
{
	PgObject old = 0;
	uint32 slotNo = HASHSLOT(self, key);
	Entry slot = self->table[slotNo];
	while(slot != 0)
	{
		if(HashKey_equals(slot->key, key))
			break;
		slot = slot->next;
	}
	if(slot != 0)
	{
		/* Find the preceding slot and create a bypass for the
		 * found slot, i.e. disconnect it.
		 */
		Entry prev = self->table[slotNo];
		if(slot == prev)
			self->table[slotNo] = slot->next;
		else
		{
			while(prev->next != slot)
				prev = prev->next;
			prev->next = slot->next;
		}
		old = slot->value;
		self->size--;
		PgObject_free((PgObject)slot);
	}
	return old;
}

void* HashMap_getByOid(HashMap self, Oid oid)
{
	struct OidKey_ oidKey;
	OidKey_init(&oidKey, oid);
	return HashMap_get(self, (HashKey)&oidKey);
}

void* HashMap_getByOpaque(HashMap self, void* opaque)
{
	struct OpaqueKey_ opaqueKey;
	OpaqueKey_init(&opaqueKey, opaque);
	return HashMap_get(self, (HashKey)&opaqueKey);
}

void* HashMap_getByString(HashMap self, const char* key)
{
	struct StringKey_ stringKey;
	StringKey_init(&stringKey, key);
	return HashMap_get(self, (HashKey)&stringKey);
}

void* HashMap_putByOid(HashMap self, Oid oid, void* value)
{
	struct OidKey_ oidKey;
	OidKey_init(&oidKey, oid);
	return HashMap_put(self, (HashKey)&oidKey, value);
}

void* HashMap_putByOpaque(HashMap self, void* opaque, void* value)
{
	struct OpaqueKey_ opaqueKey;
	OpaqueKey_init(&opaqueKey, opaque);
	return HashMap_put(self, (HashKey)&opaqueKey, value);
}

void* HashMap_putByString(HashMap self, const char* key, void* value)
{
	struct StringKey_ stringKey;
	StringKey_init(&stringKey, key);
	return HashMap_put(self, (HashKey)&stringKey, value);
}

void* HashMap_removeByOid(HashMap self, Oid oid)
{
	struct OidKey_ oidKey;
	OidKey_init(&oidKey, oid);
	return HashMap_remove(self, (HashKey)&oidKey);
}

void* HashMap_removeByOpaque(HashMap self, void* opaque)
{
	struct OpaqueKey_ opaqueKey;
	OpaqueKey_init(&opaqueKey, opaque);
	return HashMap_remove(self, (HashKey)&opaqueKey);
}

void* HashMap_removeByString(HashMap self, const char* key)
{
	struct StringKey_ stringKey;
	StringKey_init(&stringKey, key);
	return HashMap_remove(self, (HashKey)&stringKey);
}

uint32 HashMap_size(HashMap self)
{
	return self->size;
}

extern void Iterator_initialize(void);
extern void HashMap_initialize(void);
void HashMap_initialize(void)
{
	Iterator_initialize();

	s_EntryClass    = PgObjectClass_create("Entry", sizeof(struct Entry_), _Entry_finalize);
	s_HashMapClass  = PgObjectClass_create("HashMap", sizeof(struct HashMap_), _HashMap_finalize);

	s_OidKeyClass = HashKeyClass_alloc("OidKey", sizeof(struct OidKey_), 0);
	s_OidKeyClass->hashCode = _OidKey_hashCode;
	s_OidKeyClass->equals   = _OidKey_equals;

	s_OpaqueKeyClass = HashKeyClass_alloc("OpaqueKey", sizeof(struct OpaqueKey_), 0);
	s_OpaqueKeyClass->hashCode = _OpaqueKey_hashCode;
	s_OpaqueKeyClass->equals   = _OpaqueKey_equals;

	s_StringKeyClass = HashKeyClass_alloc("StringKey", sizeof(struct StringKey_), _StringKey_finalize);
	s_StringKeyClass->hashCode = _StringKey_hashCode;
	s_StringKeyClass->equals   = _StringKey_equals;
	s_StringKeyClass->clone    = _StringKey_clone;
}
