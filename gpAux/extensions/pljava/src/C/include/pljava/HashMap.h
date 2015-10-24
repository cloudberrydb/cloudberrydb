/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_HashMap_h
#define __pljava_HashMap_h

#include "pljava/PgObject.h"

#ifdef __cplusplus
extern "C" {
#endif

/*************************************************************
 * The HashMap class. Maintains mappings between HashKey instances
 * and values. The mappings are stored in hash bucket chains
 * containing Entry instances.
 * 
 * All Entries and HashKeys will be allocated using the same
 * MemoryContext as the one used when creating the HashMap.
 * A HashKey used for storing (HashKey_put) is cloned so that
 * the HashMap maintains its own copy.
 * 
 * @author Thomas Hallgren
 *
 *************************************************************/

struct HashKey_;
typedef struct HashKey_* HashKey;

struct Iterator_;
typedef struct Iterator_* Iterator;

struct Entry_;
typedef struct Entry_* Entry;

struct HashMap_;
typedef struct HashMap_* HashMap;

/*
 * Creates a new HashMap with an initial capacity. If ctx is NULL, CurrentMemoryContext
 * will be used.
 */
extern HashMap HashMap_create(uint32 initialCapacity, MemoryContext ctx);

/*
 * Clears the HashMap.
 */
extern void HashMap_clear(HashMap self);

/*
 * Returns an iterator that iterates over the entries of
 * this HashMap.
 */
extern Iterator HashMap_entries(HashMap self);

/*
 * Returns the object stored using the given key or NULL if no
 * such object can be found.
 */
extern void* HashMap_get(HashMap self, HashKey key);

/*
 * Returns the object stored using the given null
 * terminated string or NULL if no such object can be found.
 */
extern void* HashMap_getByString(HashMap self, const char* key);

/*
 * Returns the object stored using the given Oid or NULL
 * if no such object can be found.
 */
extern void* HashMap_getByOid(HashMap self, Oid key);

/*
 * Returns the object stored using the given Opaque pointer or NULL
 * if no such object can be found.
 */
extern void* HashMap_getByOpaque(HashMap self, void* key);

/*
 * Stores the given value under the given key. If
 * an old value was stored using this key, the old value is returned.
 * Otherwise this method returns NULL.
 * This method will make a private copy of the key argument.
 */
extern void* HashMap_put(HashMap self, HashKey key, void* value);

/*
 * Stores the given value under the given null terminated string. If
 * an old value was stored using this key, the old value is returned.
 * Otherwise this method returns NULL.
 */
extern void* HashMap_putByString(HashMap self, const char* key, void* value);

/*
 * Stores the given value under the given Oid. If an old value
 * was stored using this key, the old value is returned. Otherwise
 * this method returns NULL.
 */
extern void* HashMap_putByOid(HashMap self, Oid key, void* value);

/*
 * Stores the given value under the given Opaque pointer. If an old value
 * was stored using this key, the old value is returned. Otherwise
 * this method returns NULL.
 */
extern void* HashMap_putByOpaque(HashMap self, void* key, void* value);

/*
 * Removes the value stored under the given key. The the old value
 * (if any) is returned.
 */
extern void* HashMap_remove(HashMap self, HashKey key);

/*
 * Removes the value stored under the given key. The the old value
 * (if any) is returned. The key associated with the value is deleted.
 */
extern void* HashMap_removeByString(HashMap self, const char* key);

/*
 * Removes the value stored under the given key. The the old value
 * (if any) is returned.
 */
extern void* HashMap_removeByOid(HashMap self, Oid key);

/*
 * Removes the value stored under the given key. The the old value
 * (if any) is returned.
 */
extern void* HashMap_removeByOpaque(HashMap self, void* key);

/*
 * Returns the number of entries currently in the HashMap
 */
extern uint32 HashMap_size(HashMap self);

/*************************************************************
 * An instance of the Entry class holds a mapping between one
 * HashKey and its associated value.
 *************************************************************/

/*
 * Returns the value of the Entry.
 */
extern void* Entry_getValue(Entry self);

/*
 * Assigns a new value to the Entry. Returns the old value.
 */
extern void* Entry_setValue(Entry self, void* value);

/*
 * Returns the key of the Entry. Can be used for removal.
 */
extern HashKey Entry_getKey(Entry self);

/*************************************************************
 * The HashKey is an abstract class. Currently, three different
 * implementations are used. Oid, Opaque (void*), and String.
 *************************************************************/

/*
 * Clone the key. The clone is allocated in the given context.
 */
extern HashKey HashKey_clone(HashKey self, MemoryContext ctx);

/*
 * Return true if the key is equal to another key.
 */
extern bool HashKey_equals(HashKey self, HashKey other);

/*
 * Return the hash code for the key
 */
extern uint32 HashKey_hashCode(HashKey self);

#ifdef __cplusplus
} /* end of extern "C" declaration */
#endif
#endif
