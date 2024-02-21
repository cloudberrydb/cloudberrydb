#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "kryo.h"
#include "kryo_input.h"

KryoInput *
createKryoInput(uint8_t *buffer, int length)
{
	KryoInput *input = palloc0(sizeof(KryoInput));

	input->position = 0;
	input->capacity = length;
	input->limit = length;
	input->buffer = buffer;

	return input;
}

void
destroyKryoInput(KryoInput *input)
{
	pfree(input);
}

/* 
 * return the number of bytes remaining, but not more than optional,
 * or -1 if the EOS was reached and the buffer is empty
 */
static int
optional(KryoInput *input, int optional)
{
	int remaining = input->limit - input->position;

	if (remaining >= optional)
		return optional;

	optional = MIN(optional, input->capacity);
	return (remaining == 0) ? -1 : MIN(remaining, optional);
}

/* return the number of bytes remaining */
static int
require(KryoInput *input, int required)
{
	int remaining = input->limit - input->position;

	if (remaining < required)
		elog(ERROR, "buffer too small: capacity: %d, required: %d", input->capacity, required);

	return remaining;
}

static void
internalReadBytes(KryoInput *input, void *bytes, int length)
{
	int copyCount = MIN(input->limit - input->position, length);
	
	while (true)
	{
		memcpy(bytes, input->buffer + input->position, copyCount);
		input->position += copyCount;
		length -= copyCount;
		
		if (length == 0)
			break;
		
		bytes = (uint8_t *) bytes + copyCount;
		copyCount = MIN(length, input->capacity);
		require(input, copyCount);
	}
}


static uint32_t
readUIntegerSlow(KryoInput *input)
{
	/* the buffer is guaranteed to have at least 1 byte. */
	uint8_t b = input->buffer[input->position++];
	uint32_t result = b & 0x7F;

	if ((b & 0x80) != 0)
	{
		require(input, 1);
		b = input->buffer[input->position++];
		result |= (b & 0x7F) << 7;

		if ((b & 0x80) != 0)
		{
			require(input, 1);
			b = input->buffer[input->position++];
			result |= (b & 0x7F) << 14;

			if ((b & 0x80) != 0)
			{
				require(input, 1);
				b = input->buffer[input->position++];
				result |= (b & 0x7F) << 21;

				if ((b & 0x80) != 0)
				{
					require(input, 1);
					b = input->buffer[input->position++];
					result |= (b & 0x7F) << 28;
				}
			}
		}
	}
	
	return result;
}

static uint64_t
readULongSlow(KryoInput *input)
{
	/* the buffer is guaranteed to have at least 1 byte. */
	uint8_t b = input->buffer[input->position++];
	uint64_t result = b & 0x7F;

	if ((b & 0x80) != 0)
	{
		require(input, 1);
		b = input->buffer[input->position++];
		result |= (b & 0x7F) << 7;

		if ((b & 0x80) != 0)
		{
			require(input, 1);
			b = input->buffer[input->position++];
			result |= (b & 0x7F) << 14;

			if ((b & 0x80) != 0)
			{
				require(input, 1);
				b = input->buffer[input->position++];
				result |= (b & 0x7F) << 21;

				if ((b & 0x80) != 0)
				{
					require(input, 1);
					b = input->buffer[input->position++];
					result |= (uint64_t)(b & 0x7F) << 28;

					if ((b & 0x80) != 0)
					{
						require(input, 1);
						b = input->buffer[input->position++];
						result |= (uint64_t)(b & 0x7F) << 35;

						if ((b & 0x80) != 0)
						{
							require(input, 1);
							b = input->buffer[input->position++];
							result |= (uint64_t)(b & 0x7F) << 42;

							if ((b & 0x80) != 0)
							{
								require(input, 1);
								b = input->buffer[input->position++];
								result |= (uint64_t)(b & 0x7F) << 49;

								if ((b & 0x80) != 0)
								{
									require(input, 1);
									b = input->buffer[input->position++];
									result |= (uint64_t)b << 56;
								}
							}
						}
					}
				}
			}
		}
	}

	return result;
}

static KryoDatum
readAsciiSlow(KryoInput *input)
{
	int i;
	int j;
	int charCount;
	int charSize;
	uint8_t b;
	uint16_t *chars;

	input->position--; /* re-read the first byte. */

	/* copy chars currently in buffer. */
	charCount = input->limit - input->position;
	charSize = MAX(charCount * 2, 32);
	chars = palloc(charSize * sizeof(uint16_t));
	
	for (i = input->position, j = 0; i < input->limit; ++i, ++j)
		chars[j] = input->buffer[i];

	input->position = input->limit;
	
	/* copy additional chars one by one. */
	while (true)
	{
		require(input, 1);
		b = input->buffer[input->position++];

		if (charCount == charSize)
		{
			charSize = charCount * 2;
			chars = repalloc(chars, charSize * sizeof(uint16_t));
		}

		if ((b & 0x80) == 0x80)
		{
			chars[charCount++] = b & 0x7F;
			break;
		}
		
		chars[charCount++] = b;
	}

	return kryoNewString((char *) chars, charCount * 2);
}

static KryoDatum
readAscii(KryoInput *input)
{
	int end = input->position;
	int start = end - 1;
	uint8_t b;
	KryoDatum result;

	do
	{
		if (end == input->limit)
			return readAsciiSlow(input);

		b = input->buffer[end++];
	}
	while ((b & 0x80) == 0);

	input->buffer[end - 1] &= 0x7F; /* mask end of ascii bit. */
	result = kryoNewString(((char *) input->buffer) + start, end - start);
	input->buffer[end -1] |= 0x80;
	input->position = end;

	return result;
}

static int
readUtf8Length(KryoInput *input, uint8_t b)
{
	uint32_t result = b & 0x3F; /* mask all but first 6 bits. */

	if ((b & 0x40) != 0) /* bit 7 means another byte, bit 8 means UTF8. */
	{
		b = input->buffer[input->position++];
		result |= (b & 0x7F) << 6;

		if ((b & 0x80) != 0)
		{
			b = input->buffer[input->position++];
			result |= (b & 0x7F) << 13;

			if ((b & 0x80) != 0)
			{
				b = input->buffer[input->position++];
				result |= (b & 0x7F) << 20;

				if ((b & 0x80) != 0)
				{
					b = input->buffer[input->position++];
					result |= (b & 0x7F) << 27;
				}
			}
		}
	}
	
	return result;
}

static int
readUtf8LengthSlow(KryoInput *input, uint8_t b)
{
	uint32_t result = b & 0x3F; /* mask all but first 6 bits. */

	if ((b & 0x40) != 0) /* bit 7 means another byte, bit 8 means UTF8. */
	{
		require(input, 1);
		b = input->buffer[input->position++];
		result |= (b & 0x7F) << 6;

		if ((b & 0x80) != 0)
		{
			require(input, 1);
			b = input->buffer[input->position++];
			result |= (b & 0x7F) << 13;

			if ((b & 0x80) != 0)
			{
				require(input, 1);
				b = input->buffer[input->position++];
				result |= (b & 0x7F) << 20;

				if ((b & 0x80) != 0)
				{
					require(input, 1);
					b = input->buffer[input->position++];
					result |= (b & 0x7F) << 27;
				}
			}
		}
	}

	return result;
}

static void
readUtf8Slow(KryoInput *input, uint16_t *chars, int index, int length)
{
	while (index < length)
	{
		if (input->position == input->limit)
			require(input, 1);

		uint16_t b = input->buffer[input->position++] & 0xFF;

		switch (b >> 4)
		{
			case 0:
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
				chars[index] = b;
				break;

			case 12:
			case 13:
				if (input->position == input->limit)
					require(input, 1);

				chars[index] = ((b & 0x1F) << 6 | (input->buffer[input->position++] & 0x3F));
				break;

			case 14:
				{
					uint16_t e, f, g;

					require(input, 2);
					e = (b & 0x0F) << 12;
					f = (input->buffer[input->position++] & 0x3F) << 6;
					g = input->buffer[input->position++] & 0x3F;
					chars[index] = e | f | g;
				}
				break;
		}

		index++;
	}
}

static void
readUtf8(KryoInput *input, uint16_t *chars, int length)
{
	/* try to read 7 bit ASCII chars. */
	int available = require(input, 1);
	int charIndex = 0;
	int count = MIN(available, length);
	int position = input->position;

	while (charIndex < count)
	{
		char b = input->buffer[position++];

		if (b < 0)
		{
			position--;
			break;
		}

		chars[charIndex++] = b;
	}

	input->position = position;

	/* if buffer didn't hold all chars or any were not ASCII, use slow path for remainder. */
	if (charIndex < length)
		readUtf8Slow(input, chars, charIndex, length);
}

/* returns the number of bytes read or -1 if no more bytes are available */
int
read(KryoInput *input, uint8_t *buffer, int length)
{
	int startingCount = length;
	int copyCount = MIN(input->limit - input->position, length);
	
	while (true)
	{
		memcpy(buffer, input->buffer + input->position, copyCount);
		input->position += copyCount;
		length -= copyCount;

		if (length == 0)
			break;

		buffer = (uint8_t *) buffer + copyCount;
		copyCount = optional(input, length);
		
		if (copyCount == -1)
		{
			/* end of data. */
			if (startingCount == length)
				return -1;

			break;
		}

		if (input->position == input->limit)
			break;
	}

	return startingCount - length;
}

char
readByte(KryoInput *input)
{
	require(input, 1);
	return input->buffer[input->position++];
}

void
readBytes(KryoInput *input, void *bytes, int length)
{
	internalReadBytes(input, bytes, length);
}

int16_t
readShort(KryoInput *input)
{
	int16_t value;

	require(input, 2);
	value = ntohs(*(int16_t*)(input->buffer + input->position));
	input->position += 2;

	return value;
}

int32_t
readInteger(KryoInput *input)
{
	int32_t value;

	require(input, 4);
	value = ntohl(*(int32_t *)(input->buffer + input->position));
	input->position += 4;

	return value;
}

uint32_t
readUInteger(KryoInput *input)
{
	uint8_t b;
	uint32_t result;

	if (require(input, 1) < 5)
		return readUIntegerSlow(input);

	b = input->buffer[input->position++];
	result = b & 0x7F;

	if ((b & 0x80) != 0)
	{
		b = input->buffer[input->position++];
		result |= (b & 0x7F) << 7;

		if ((b & 0x80) != 0)
		{
			b = input->buffer[input->position++];
			result |= (b & 0x7F) << 14;

			if ((b & 0x80) != 0)
			{
				b = input->buffer[input->position++];
				result |= (b & 0x7F) << 21;

				if ((b & 0x80) != 0)
				{
					b = input->buffer[input->position++];
					result |= (b & 0x7F) << 28;
				}
			}
		}
	}

	return result;
}

int32_t
readIntegerOptimizePositive(KryoInput *input, bool optimizePositive)
{
	uint32_t temp = readUInteger(input);
	int32_t result = *(int32_t *)&temp;

	if (!optimizePositive)
		result = ((result >> 1) ^ -(result & 1));
	
	return result;
}

int64_t
readLong(KryoInput *input)
{
	int position;

	require(input, 8);
	position = input->position;
	input->position += 8;

	return (int64_t)input->buffer[position + 0] << 56
					| (int64_t)(input->buffer[position + 1] & 0xFF) << 48
					| (int64_t)(input->buffer[position + 2] & 0xFF) << 40
					| (int64_t)(input->buffer[position + 3] & 0xFF) << 32
					| (int64_t)(input->buffer[position + 4] & 0xFF) << 24
					| (input->buffer[position + 5] & 0xFF) << 16
					| (input->buffer[position + 6] & 0xFF) << 8
					| (input->buffer[position + 7] & 0xFF);
}

uint64_t
readULong(KryoInput *input)
{
	 uint8_t b;
	 uint64_t result;

	if (require(input, 1) < 9)
		return readULongSlow(input);
	
	b = input->buffer[input->position++];
	result = b & 0x7F;
	
	if ((b & 0x80) != 0)
	{
		b = input->buffer[input->position++];
		result |= (b & 0x7F) << 7;
		
		if ((b & 0x80) != 0)
		{
			b = input->buffer[input->position++];
			result |= (b & 0x7F) << 14;
			
			if ((b & 0x80) != 0)
			{
				b = input->buffer[input->position++];
				result |= (b & 0x7F) << 21;
				
				if ((b & 0x80) != 0)
				{
					b = input->buffer[input->position++];
					result |= (int64_t)(b & 0x7F) << 28;
					
					if ((b & 0x80) != 0)
					{
						b = input->buffer[input->position++];
						result |= (int64_t)(b & 0x7F) << 35;
						
						if ((b & 0x80) != 0)
						{
							b = input->buffer[input->position++];
							result |= (int64_t)(b & 0x7F) << 42;
							
							if ((b & 0x80) != 0)
							{
								b = input->buffer[input->position++];
								result |= (int64_t)(b & 0x7F) << 49;
								
								if ((b & 0x80) != 0)
								{
									b = input->buffer[input->position++];
									result |= (int64_t)b << 56;
								}
							}
						}
					}
				}
			}
		}
	}
	
	return result;
}

int64_t
readLongOptimizePositive(KryoInput *input, bool optimizePositive)
{
	uint64_t temp = readULong(input);
	int64_t result = *(int64_t *)&temp;
	
	if (!optimizePositive)
		result = (result >> 1) ^ -(result & 1);

	return result;
}

float
readFloat(KryoInput *input)
{
	int32_t value = readInteger(input);
	return *(float *)&value;
}

float
readFloatWithPrecision(KryoInput *input, float precision, bool optimizePositive)
{
	int32_t value = readIntegerOptimizePositive(input, optimizePositive);
	return value / precision;
}

double
readDouble(KryoInput *input)
{
	int64_t value = readLong(input);
	return *(double *)&value;
}

double
readDoubleWithPrecision(KryoInput *input, double precision, bool optimizePositive)
{
	int64_t value = readLongOptimizePositive(input, optimizePositive);
	return value / precision;
}

bool
readBoolean(KryoInput *input)
{
	require(input, 1);
	return input->buffer[input->position++] == 1;
}

KryoDatum
readString(KryoInput *input)
{
	int charCount;
	int available = require(input, 1);
	uint8_t b = input->buffer[input->position++];

	if ((b & 0x80) == 0)
		return readAscii(input); /* ascii */

	/* Null, empty, or UTF8. */
	charCount = (available >= 5) ? readUtf8Length(input, b) : readUtf8LengthSlow(input, b);

	switch (charCount)
	{
		case 0:
			return NULL;

		case 1:
			return kryoNewString(NULL, 0);
	}

	charCount--;
	
	/* reserve memory for char buffer */
	uint16_t *chars = malloc((charCount + 1) * sizeof(uint16_t));
	
	readUtf8(input, chars, charCount);
	chars[charCount] = 0;

	return kryoNewString((char *) chars, charCount *2);
}
