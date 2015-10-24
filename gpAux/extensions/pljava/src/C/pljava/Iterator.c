/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "pljava/HashMap_priv.h"
#include "pljava/Iterator.h"

struct Iterator_
{
	struct PgObject_ PgObject_extension;
	HashMap source;
	uint32  sourceTableSize;
	uint32  currentBucket;
	Entry   nextEntry;
};

static PgObjectClass s_IteratorClass;

Iterator Iterator_create(HashMap source)
{
	Iterator self = (Iterator)PgObjectClass_allocInstance(s_IteratorClass, GetMemoryChunkContext(source));
	self->source = source;
	self->sourceTableSize = source->tableSize;
	self->currentBucket = 0;
	self->nextEntry = 0;
	return self;
}

static Entry Iterator_peekNext(Iterator self)
{
	uint32 tableSize = self->source->tableSize;
	if(tableSize != self->sourceTableSize)
	{
		/* Rehash during Iteration. We can't continue.
		 */
		self->nextEntry = 0;
	}
	else if(self->nextEntry == 0)
	{
		/* Go to next bucket
		 */
		Entry* table = self->source->table;
		while(self->currentBucket < tableSize)
		{
			Entry nxt = table[self->currentBucket];
			if(nxt != 0)
			{
				self->nextEntry = nxt;
				break;
			}
			self->currentBucket++;
		}
	}
	return self->nextEntry;
}

bool Iterator_hasNext(Iterator self)
{
	return Iterator_peekNext(self) != 0;
}

Entry Iterator_next(Iterator self)
{
	Entry nxt = Iterator_peekNext(self);
	if(nxt != 0)
	{
		Entry nxtNxt = nxt->next;
		if(nxtNxt == 0)
			/*
			 * Leave this bucket.
			 */
			self->currentBucket++;
		self->nextEntry = nxtNxt;
	}
	return nxt;
}

extern void Iterator_initialize(void);
void Iterator_initialize(void)
{
	s_IteratorClass = PgObjectClass_create("Iterator", sizeof(struct Iterator_), 0);
}

