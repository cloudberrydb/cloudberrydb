#ifndef KRYO_H
#define KRYO_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils/hsearch.h"

struct List;
struct Kryo;
struct KryoInput;

typedef enum KryoType
{
	KRYO_STRING,
	KRYO_BYTES,
	KRYO_INT32,
	KRYO_INT64,
	KRYO_FLOAT,
	KRYO_DOUBLE,
	KRYO_BOOLEAN,
	KRYO_ARRAY,
	KRYO_ANY
} KryoType;

typedef struct KryoObject
{
	KryoType type;
} KryoObject;

typedef KryoObject *KryoDatum;

typedef struct KryoStringDatum
{
	KryoObject obj;
	char *s;
	int size;
} KryoStringDatum;

typedef struct KryoBytesDatum
{
	KryoObject obj;
	char *bytes;
	int size;
} KryoBytesDatum;

typedef struct KryoInt32Datum
{
	KryoObject obj;
	int32_t i32;
} KryoInt32Datum;

typedef struct KryoInt64Datum
{
	KryoObject obj;
	int64_t i64;
} KryoInt64Datum;

typedef struct KryoFloatDatum
{
	KryoObject obj;
	float f;
} KryoFloatDatum;

typedef struct KryoDoubleDatum
{
	KryoObject obj;
	double d;
} KryoDoubleDatum;

typedef struct KryoBooleanDatum
{
	KryoObject obj;
	int8_t i;
} KryoBooleanDatum;

typedef struct KryoArrayDatum
{
	KryoObject  obj;
	KryoDatum  *elements;
	int         size;
} KryoArrayDatum;

#define container_of(ptr_, type_, member_)  \
	((type_ *)((char *)ptr_ - (size_t)&((type_ *) 0)->member_))

#define kryoDatumToString(datum_)    (container_of(datum_, KryoStringDatum, obj))
#define kryoDatumToBytes(datum_)     (container_of(datum_, KryoBytesDatum, obj))
#define kryoDatumToInt32(datum_)     (container_of(datum_, KryoInt32Datum, obj))
#define kryoDatumToInt64(datum_)     (container_of(datum_, KryoInt64Datum, obj))
#define kryoDatumToFloat(datum_)     (container_of(datum_, KryoFloatDatum, obj))
#define kryoDatumToDouble(datum_)    (container_of(datum_, KryoDoubleDatum, obj))
#define kryoDatumToBoolean(datum_)   (container_of(datum_, KryoBooleanDatum, obj))
#define kryoDatumToArray(datum_)     (container_of(datum_, KryoArrayDatum, obj))

typedef KryoDatum (*kryoSerializerRead) (struct Kryo *kryo, char *type, struct KryoInput *input);

typedef struct Registration
{
	char       *type;
	int         id;
	kryoSerializerRead readFn;
} Registration;

typedef struct ClassResolver
{
	HTAB *nameIdToClass;
	int memoizedClassId;
	Registration *memoizedClassIdValue;
} ClassResolver;

typedef struct Kryo
{
	int depth;
	int maxDepth;
	ClassResolver *resolver;
	List *seenObjects;
	KryoDatum readObject;
	List *readReferenceIds;
} Kryo;

KryoDatum kryoNewString(char *str, int size);
KryoDatum kryoNewArray(int size);
Kryo *createKryo(void);
void destroyKryo(Kryo *kryo);
KryoDatum readClassAndObject(Kryo *kryo, struct KryoInput *input);
Datum createDatumByValue(Oid attType, KryoDatum value);

#endif // KRYO_READER_H
