#ifndef KRYO_INPUT_H
#define KRYO_INPUT_H

typedef struct KryoInput
{
	int  position;
	int  capacity;
	int  limit;
	uint8_t *buffer;
} KryoInput;

KryoInput *createKryoInput(uint8_t *buffer, int length);
void destroyKryoInput(KryoInput *input);
int read(KryoInput *input, uint8_t *buffer, int length);
char readByte(KryoInput *input);
void readBytes(KryoInput *input, void *bytes, int length);
int16_t readShort(KryoInput *input);
int32_t readInteger(KryoInput *input);
uint32_t readUInteger(KryoInput *input);
int32_t readIntegerOptimizePositive(KryoInput *input, bool optimizePositive);
int64_t readLong(KryoInput *input);
int64_t readLongOptimizePositive(KryoInput *input, bool optimizePositive);
uint64_t readULong(KryoInput *input);
float readFloat(KryoInput *input);
float readFloatWithPrecision(KryoInput *input, float precision, bool optimizePositive);
double readDouble(KryoInput *input);
double readDoubleWithPrecision(KryoInput *input, double precision, bool optimizePositive);
bool readBoolean(KryoInput *input);
KryoDatum readString(KryoInput *input);

#endif // KRYO_INPUT_H
