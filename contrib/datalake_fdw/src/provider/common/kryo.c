#include "postgres.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "utils/memutils.h"
#include "kryo.h"
#include "kryo_input.h"

#define NAME         -1
#define IS_NULL       0
#define REF          -1
#define NO_REF       -2
#define NOT_NULL      1
#define KRYO_NULL     0

static void referenceResolverReference(Kryo *kryo, KryoDatum object);
static Registration *getRegistration(ClassResolver *resolver, char *type);
static KryoDatum readObject(Kryo *kryo, KryoInput *input, Registration *registration);
static Registration *readClass(ClassResolver *resolver, KryoInput *input);
static KryoDatum integerRead(Kryo *kryo, char *type, KryoInput *input);
static KryoDatum arrayRead(Kryo *kryo, char *type, KryoInput *input);
static KryoDatum fieldsRead(Kryo *kryo, char *type, KryoInput *input);
static KryoDatum stringRead(Kryo *kryo, char *type, KryoInput *input);
static KryoDatum readObjectOrNull(Kryo *kryo, KryoInput *input, Registration *registration);
static KryoDatum floatRead(Kryo *kryo, char *type, KryoInput *input);
static KryoDatum booleanRead(Kryo *kryo, char *type, KryoInput *input);
static KryoDatum longRead(Kryo *kryo, char *type, KryoInput *input);
static KryoDatum doubleRead(Kryo *kryo, char *type, KryoInput *input);

typedef struct RegistrationHashEntry
{
	int         nameId;
	char        type[NAMEDATALEN];
} RegistrationHashEntry;

typedef struct FieldDescription
{
	KryoType type;
	const char *name;
	bool usePersistentType;
	bool canBeNull;
} FieldDescription;

static Registration registrations[] = {
	{"[Lorg.apache.hudi.common.model.DeleteRecord;", -1, arrayRead},
	{"[Lorg.apache.hudi.common.model.HoodieKey;", -1, arrayRead},
	{"org.apache.hudi.common.model.DeleteRecord", -1, fieldsRead},
	{"org.apache.hudi.common.model.HoodieKey", -1, fieldsRead},
	{"Integer", 0, integerRead},
	{"String", 1, stringRead},
	{"Float", 2, floatRead},
	{"Boolean", 3, booleanRead},
	{"Long", 7, longRead},
	{"Double", 8, doubleRead},
};

static FieldDescription hoodieKeyFields[] = {
	{KRYO_STRING, "recordKey", false, true},
	{KRYO_STRING, "partitionPath", false, true}
};

static FieldDescription deleteRecordFields[] = {
	{KRYO_ARRAY, "hoodieKey", true, false},
	{KRYO_ANY, "orderingVal", true, false}
};

static inline void
kryoDatumInit(KryoDatum datum, KryoType type)
{
	datum->type = type;
}

KryoDatum
kryoNewString(char *str, int size)
{
	KryoStringDatum *datum = palloc(sizeof(KryoStringDatum));

	datum->s = palloc(size + 1);
	if (str)
		memcpy(datum->s, str, size);
	datum->s[size] = '\0';
	datum->size = size;

	kryoDatumInit(&datum->obj, KRYO_STRING);
	return &datum->obj;
}

static KryoDatum
kryoNewInt32(int32_t i)
{
	KryoInt32Datum *datum = palloc(sizeof(KryoInt32Datum));

	datum->i32 = i;
	kryoDatumInit(&datum->obj, KRYO_INT32);

	return &datum->obj;
}

static KryoDatum
kryoNewInt64(int64_t l)
{
	KryoInt64Datum *datum = palloc(sizeof(KryoInt64Datum));

	datum->i64 = l;
	kryoDatumInit(&datum->obj, KRYO_INT64);

	return &datum->obj;
}

static KryoDatum
kryoNewFloat(float f)
{
	return NULL;
}

static KryoDatum
kryoNewDouble(double d)
{
	return NULL;
}

static KryoDatum
kryoNewBoolean(int8_t i)
{
	KryoBooleanDatum *datum = palloc(sizeof(KryoBooleanDatum));

	datum->i = i;
	kryoDatumInit(&datum->obj, KRYO_BOOLEAN);

	return &datum->obj;
}

KryoDatum
kryoNewArray(int size)
{
	KryoArrayDatum *datum = palloc(sizeof(KryoArrayDatum));

	datum->elements = palloc(sizeof(KryoDatum) * size);
	datum->size = size;

	kryoDatumInit(&datum->obj, KRYO_ARRAY);
	return &datum->obj;
}

static KryoDatum
integerRead(Kryo *kryo, char *type, KryoInput *input)
{
	int value = readIntegerOptimizePositive(input, false);
	return kryoNewInt32(value);
}

static KryoDatum
stringRead(Kryo *kryo, char *type, KryoInput *input)
{
	return readString(input);
}

static KryoDatum
floatRead(Kryo *kryo, char *type, KryoInput *input)
{
	float value = readFloat(input);
	return kryoNewFloat(value);
}

static KryoDatum
booleanRead(Kryo *kryo, char *type, KryoInput *input)
{
	bool value = readBoolean(input);
	return kryoNewBoolean(value);
}

static KryoDatum
longRead(Kryo *kryo, char *type, KryoInput *input)
{
	int64_t value = readLongOptimizePositive(input, false);
	return kryoNewInt64(value);
}

static KryoDatum
doubleRead(Kryo *kryo, char *type, KryoInput *input)
{
	double value = readDouble(input);
	return kryoNewDouble(value);
}

static KryoDatum
arrayRead(Kryo *kryo, char *type, KryoInput *input)
{
	int i;
	KryoDatum datum = NULL;
	Registration *registration;
	int length = readIntegerOptimizePositive(input, true);

	if (length > 1)
	{
		datum = kryoNewArray(length - 1);
		referenceResolverReference(kryo, datum);
	}

	for (i = 0; i < length - 1; i++)
	{
		registration = readClass(kryo->resolver, input);
		if (registration == NULL)
		{
			kryoDatumToArray(datum)->elements[i] = NULL;
			continue;
		}

		kryoDatumToArray(datum)->elements[i] = readObject(kryo, input, registration);
	}

	return datum;
}

static FieldDescription *
getObjectFields(char *type, int *size)
{
	if (pg_strcasecmp("org.apache.hudi.common.model.DeleteRecord", type) == 0)
	{
		*size = lengthof(deleteRecordFields);
		return deleteRecordFields;
	}

	*size = lengthof(hoodieKeyFields);
	return hoodieKeyFields;
}

static char *
convertTypeToJavaName(KryoType type)
{
	switch(type)
	{
		case KRYO_STRING:
			return "String";
		case KRYO_INT32:
			return "Integer";
		case KRYO_INT64:
			return "Long";
		case KRYO_FLOAT:
			return "Float";
		case KRYO_BOOLEAN:
			return "Boolean";
		case KRYO_DOUBLE:
			return "Douoble";
		default:
			elog(ERROR, "unsupported kryo type: \"%d\"", type);
	}
}

static KryoDatum
fieldRead(Kryo *kryo, KryoInput *input, KryoType type, bool usePersistentType, bool canBeNull)
{
	Registration *registration;

	if (usePersistentType)
	{
		registration = readClass(kryo->resolver, input);
		if (registration == NULL)
			return NULL;

		return readObject(kryo, input, registration);
	}

	registration = getRegistration(kryo->resolver, convertTypeToJavaName(type));
	if (registration == NULL)
		return NULL;

	return canBeNull ? readObjectOrNull(kryo, input, registration) : readObject(kryo, input, registration);
}

static KryoDatum
fieldsRead(Kryo *kryo, char *type, KryoInput *input)
{
	int i;
	int length;
	KryoDatum datum;
	FieldDescription *descFields;

	descFields = getObjectFields(type, &length);

	datum = kryoNewArray(length);
	referenceResolverReference(kryo, datum);

	for (i = 0; i < length; i++)
	{
		kryoDatumToArray(datum)->elements[i] = fieldRead(kryo,
														 input,
														 descFields[i].type,
														 descFields[i].usePersistentType,
														 descFields[i].canBeNull);
	}

	return datum;
}

static int
referenceResolverGetNextReadId(Kryo *kryo)
{
	int id = list_length(kryo->seenObjects);

	kryo->seenObjects = lappend(kryo->seenObjects, NULL);
	return id;
}

static void
referenceResolverSetReadObject(Kryo *kryo, int id, KryoDatum object)
{
	list_nth_replace(kryo->seenObjects, id, object);
}

static KryoDatum
referenceResolverGetReadObject(Kryo *kryo, int id)
{
	return list_nth(kryo->seenObjects, id);
}

static bool
referenceResolverUseReferences(char *type)
{
	if (pg_strcasecmp(type, "Integer") == 0 ||
		pg_strcasecmp(type, "Float") == 0 ||
		pg_strcasecmp(type, "Boolean") == 0 ||
		pg_strcasecmp(type, "Long") == 0 ||
		pg_strcasecmp(type, "Byte") == 0 ||
		pg_strcasecmp(type, "Character") == 0 ||
		pg_strcasecmp(type, "Short") == 0 ||
		pg_strcasecmp(type, "Double") == 0)
		return false;

	return true;
}

static void
referenceResolverReference(Kryo *kryo, KryoDatum object)
{
	if (object != NULL)
	{
		int id = llast_int(kryo->readReferenceIds);
		kryo->readReferenceIds = list_delete_int(kryo->readReferenceIds, id);

		if (id != NO_REF)
			referenceResolverSetReadObject(kryo, id, object);
	}
}

static ClassResolver *
createClassResolver(void)
{
	ClassResolver *resolver = palloc0(sizeof(ClassResolver));

	return resolver;
}

static void
destroyClassResolver(ClassResolver *resolver)
{
	if (resolver->nameIdToClass)
		hash_destroy(resolver->nameIdToClass);

	pfree(resolver);
}

static Registration *
getRegistration(ClassResolver *resolver, char *type)
{
	int i;

	for (i = 0; i < lengthof(registrations); i++)
	{
		if (pg_strcasecmp(type, registrations[i].type) == 0)
			return &registrations[i];
	}

	return NULL;
}

static Registration *
getRegistrationById(ClassResolver *resolver, int classID)
{
	int i;

	for (i = 0; i < lengthof(registrations); i++)
	{
		if (registrations[i].id == classID)
			return &registrations[i];
	}

	return NULL;
}

static Registration *
readName(ClassResolver *resolver, KryoInput *input)
{
	bool found;
	RegistrationHashEntry *entry;
	int nameId = readIntegerOptimizePositive(input, true);

	if (resolver->nameIdToClass == NULL)
	{
		HASHCTL  hashCtl;

		MemSet(&hashCtl, 0, sizeof(hashCtl));
		hashCtl.keysize = sizeof(int);
		hashCtl.entrysize = sizeof(RegistrationHashEntry);
		hashCtl.hash = int32_hash;

		resolver->nameIdToClass = hash_create("KryoResolverTable", 10, &hashCtl, HASH_ELEM | HASH_FUNCTION);
	}

	entry = (RegistrationHashEntry *) hash_search(resolver->nameIdToClass, &nameId, HASH_ENTER, &found);
	if (!found)
	{
		KryoDatum datum = readString(input);
		memset(entry->type, 0, sizeof(entry->type));
		memcpy(entry->type, kryoDatumToString(datum)->s, kryoDatumToString(datum)->size);
	}

	return getRegistration(resolver, entry->type);
}

static Registration *
readClass(ClassResolver *resolver, KryoInput *input)
{
	Registration *registration;
	int classID = readIntegerOptimizePositive(input, true);

	switch (classID)
	{
		case IS_NULL:
			return NULL;

		case NAME + 2: /* offset for NAME and NULL. */
			return readName(resolver, input);
	}

	if (classID == resolver->memoizedClassId)
		return resolver->memoizedClassIdValue;

	registration = getRegistrationById(resolver, classID - 2);
	if (registration == NULL)
		elog(ERROR, "encountered unregistered class ID %d", classID - 2);

	resolver->memoizedClassId = classID;
	resolver->memoizedClassIdValue = registration;
	return registration;
}

Kryo*
createKryo(void)
{
	Kryo *kryo = palloc0(sizeof(Kryo));

	kryo->depth = 0;
	kryo->maxDepth = 65535;
	kryo->resolver = createClassResolver();

	return kryo;
}

void
destroyKryo(Kryo *kryo)
{
	destroyClassResolver(kryo->resolver);
	pfree(kryo);
}

static void
beginObject(Kryo *kryo)
{
	if (kryo->depth == kryo->maxDepth)
		elog(ERROR, "max depth exceeded: %d", kryo->depth);

	kryo->depth++;
}

/*
 * Returns REF if a reference to a previously read object was read,
 * which is stored in {@link #readObject}. Returns a tack size (> 0)
 * if a reference ID has been put on the stack.
 */
static int
readReferenceOrNull(Kryo *kryo, KryoInput *input, char *type, bool maybeNull)
{
	int id;
	bool referencesSupported = referenceResolverUseReferences(type);

	if (maybeNull)
	{
		id = readIntegerOptimizePositive(input, true);
		if (id == KRYO_NULL)
		{
			kryo->readObject = NULL;
			return REF;
		}

		if (!referencesSupported)
		{
			kryo->readReferenceIds = lappend_int(kryo->readReferenceIds, NO_REF);
			return list_length(kryo->readReferenceIds);
		}
	}
	else
	{
		if (!referencesSupported)
		{
			kryo->readReferenceIds = lappend_int(kryo->readReferenceIds, NO_REF);
			return list_length(kryo->readReferenceIds);
		}

		id = readIntegerOptimizePositive(input, true);
	}

	if (id == NOT_NULL)
	{
		/* first time object has been encountered. */
		id = referenceResolverGetNextReadId(kryo);
		kryo->readReferenceIds = lappend_int(kryo->readReferenceIds, id);
		return list_length(kryo->readReferenceIds);
	}

	/* the id is an object reference. */
	id -= 2; /* - 2 because 0 and 1 are used for NULL and NOT_NULL. */
	kryo->readObject = referenceResolverGetReadObject(kryo, id);
	return REF;
}

KryoDatum
readClassAndObject(Kryo *kryo, KryoInput *input)
{
	int stackSize;
	KryoDatum datum;
	Registration *registration;

	beginObject(kryo);

	registration = readClass(kryo->resolver, input);
	if (registration == NULL)
		return NULL;

	stackSize = readReferenceOrNull(kryo, input, registration->type, false);
	if (stackSize == REF)
		return kryo->readObject;

	datum = registration->readFn(kryo, registration->type, input);
	if (stackSize == list_length(kryo->readReferenceIds))
		referenceResolverReference(kryo, datum);

	kryo->depth--;
	return datum;
}

static KryoDatum
readObjectInternal(Kryo *kryo, KryoInput *input, Registration *registration, bool maybeNull)
{
	int stackSize;
	KryoDatum datum;

	beginObject(kryo);

	stackSize = readReferenceOrNull(kryo, input, registration->type, maybeNull);
	if (stackSize == REF)
		return kryo->readObject;

	datum = registration->readFn(kryo, registration->type, input);
	if (stackSize == list_length(kryo->readReferenceIds))
		referenceResolverReference(kryo, datum);

	kryo->depth--;
	return datum;
}

static KryoDatum
readObject(Kryo *kryo, KryoInput *input, Registration *registration)
{
	return readObjectInternal(kryo, input, registration, false);
}

static KryoDatum
readObjectOrNull(Kryo *kryo, KryoInput *input, Registration *registration)
{
	return readObjectInternal(kryo, input, registration, true);
}

Datum
createDatumByValue(Oid attType, KryoDatum value)
{
	Datum  datum;

	switch (attType)
	{
		case BOOLOID:
			datum = BoolGetDatum(kryoDatumToBoolean(value)->i);
			break;
		case INT4OID:
			datum = Int32GetDatum(kryoDatumToInt32(value)->i32);
			break;
		case INT8OID:
			datum = Int64GetDatum(kryoDatumToInt64(value)->i64);
			break;
		case FLOAT4OID:
			datum = Float4GetDatum(kryoDatumToFloat(value)->f);
			break;
		case FLOAT8OID:
			datum = Float8GetDatum(kryoDatumToDouble(value)->d);
			break;
		case TEXTOID:
			{
				bytea *bytes = (bytea *) palloc(kryoDatumToString(value)->size + VARHDRSZ);
				SET_VARSIZE(bytes, kryoDatumToString(value)->size + VARHDRSZ);
				memcpy(VARDATA(bytes), kryoDatumToString(value)->s, kryoDatumToString(value)->size);
				datum = PointerGetDatum(bytes);
			}
			break;
		default:
			elog(ERROR, "does not support type:\"%u\" as part of preCombineField", attType);
	}

	return datum;
}
