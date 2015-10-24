#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "../varlena.c"

#define MEMORY_LIMIT 8 /* 8 bytes memory limit */

#ifdef USE_ASSERT_CHECKING
void
_ExceptionalCondition( )
{
     PG_RE_THROW();
}
#endif

/*
 * Checks if the small strings that fit in memory fails assertion.
 */
void
test__find_memory_limited_substring__small_string(void **state)
{
	int subStringByteLength = -1;
	int subStringCharLength = -1;
	int totalByteLength = MEMORY_LIMIT;
	char *strStart = 0xabcdefab;

#ifdef USE_ASSERT_CHECKING
    expect_any(ExceptionalCondition,conditionName);
    expect_any(ExceptionalCondition,errorType);
    expect_any(ExceptionalCondition,fileName);
    expect_any(ExceptionalCondition,lineNumber);
    will_be_called_with_sideeffect(ExceptionalCondition,&_ExceptionalCondition,NULL);

    /* Test if within memory-limit strings cause assertion failure */
	PG_TRY();
	{
		find_memory_limited_substring(strStart, totalByteLength, MEMORY_LIMIT, &subStringByteLength, &subStringCharLength);
		assert_true(false);
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();
#endif
}

/*
 * Checks if null input string causes assertion failure.
 */
void
test__find_memory_limited_substring__null_string(void **state)
{
	int subStringByteLength = -1;
	int subStringCharLength = -1;
	int totalByteLength = MEMORY_LIMIT + 1;
	char *strStart = NULL;

#ifdef USE_ASSERT_CHECKING
    expect_any(ExceptionalCondition,conditionName);
    expect_any(ExceptionalCondition,errorType);
    expect_any(ExceptionalCondition,fileName);
    expect_any(ExceptionalCondition,lineNumber);
    will_be_called_with_sideeffect(ExceptionalCondition,&_ExceptionalCondition,NULL);

    /* Test if null strings cause assertion failure */
	PG_TRY();
	{
		find_memory_limited_substring(strStart, totalByteLength, MEMORY_LIMIT, &subStringByteLength, &subStringCharLength);
		assert_true(false);
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();
#endif
}

/*
 * Checks if the returned string segments are within memory limit for ascii characters.
 */
void
test__find_memory_limited_substring__ascii_chars_within_memory_limit(void **state)
{
	int subStringByteLength = -1;
	int subStringCharLength = -1;
	int cumulativeLengthConsidered = 0;

	char *strStart = 0xabcdefab;

	int totalByteLength = 25;

	while (cumulativeLengthConsidered < totalByteLength - MEMORY_LIMIT)
	{
		will_return(pg_database_encoding_max_length, 1);
		find_memory_limited_substring(strStart, totalByteLength - cumulativeLengthConsidered, MEMORY_LIMIT, &subStringByteLength, &subStringCharLength);
		cumulativeLengthConsidered += subStringByteLength;
		assert_true(subStringByteLength == MEMORY_LIMIT);
		assert_true(subStringByteLength == subStringCharLength);
	}

#ifdef USE_ASSERT_CHECKING
    expect_any(ExceptionalCondition,conditionName);
    expect_any(ExceptionalCondition,errorType);
    expect_any(ExceptionalCondition,fileName);
    expect_any(ExceptionalCondition,lineNumber);
    will_be_called_with_sideeffect(ExceptionalCondition,&_ExceptionalCondition,NULL);

    /* Test if the left-over string that fits in memory cause assertion failure */
	PG_TRY();
	{
		find_memory_limited_substring(strStart, totalByteLength - cumulativeLengthConsidered, MEMORY_LIMIT, &subStringByteLength, &subStringCharLength);
		assert_true(false);
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();

    expect_any(ExceptionalCondition,conditionName);
    expect_any(ExceptionalCondition,errorType);
    expect_any(ExceptionalCondition,fileName);
    expect_any(ExceptionalCondition,lineNumber);
    will_be_called_with_sideeffect(ExceptionalCondition,&_ExceptionalCondition,NULL);

    /* Test if null strings cause assertion failure */
	PG_TRY();
	{
		find_memory_limited_substring(NULL, totalByteLength, MEMORY_LIMIT, &subStringByteLength, &subStringCharLength);
	}
	PG_CATCH();
	{
		return;
	}
	PG_END_TRY();
	assert_true(false);
#endif
}


/*
 * Checks if the returned string segments are within memory limit for multi-bytes chars.
 */
void
test__find_memory_limited_substring__mb_chars_within_memory_limit(void **state)
{
	int subStringByteLength = -1;
	int subStringCharLength = -1;
	int cumulativeLengthConsidered = 0;

	/* Lengths of the multi-byte characters at different positions */
	int stringByteLengths[] = {3, 3, 3 /* seg1 */, 2, 2, 1, 2 /* seg2 */, 2, 1, 1, 1, 2, /* seg3 */ 5, 4 /* seg4 */, 4};

	/* Total length in terms of number of characters */
	int stringCharLength = sizeof(stringByteLengths) / sizeof(int);

	/* Total byte lengths of all the characters */
	int totalByteLength = 0;
	for (int charIndex = 0; charIndex < stringCharLength; charIndex++)
	{
		totalByteLength += stringByteLengths[charIndex];
	}

	int segmentByteLength = 0; /* Number of bytes in current segment */
	int segmentCharLength = 0; /* Number of characters in current segment */

	/* Length of the char that spilled over from one partition to another */
	int carryoverLength = 0;

	/* Fictitious multi-byte string to segment */
	char *strStart = 0xabcdefab;

	for (int charIndex = 0; charIndex < stringCharLength; charIndex++)
	{
		if (carryoverLength > 0)
		{
			expect_any(pg_mblen, mbstr);
			will_return(pg_mblen, carryoverLength);
			carryoverLength = 0;
		}

		expect_any(pg_mblen, mbstr);
		will_return(pg_mblen, stringByteLengths[charIndex]);
		segmentByteLength += stringByteLengths[charIndex];
		segmentCharLength++;

		if (segmentByteLength > MEMORY_LIMIT)
		{

			will_return(pg_database_encoding_max_length, 6);
			find_memory_limited_substring(strStart, totalByteLength - cumulativeLengthConsidered, MEMORY_LIMIT, &subStringByteLength, &subStringCharLength);
			assert_true(subStringByteLength == (segmentByteLength - stringByteLengths[charIndex]));
			assert_true(subStringCharLength == (segmentCharLength - 1));
			assert_true(subStringByteLength <= MEMORY_LIMIT);
			assert_true(subStringCharLength <= MEMORY_LIMIT);

			cumulativeLengthConsidered += subStringByteLength;

			segmentByteLength = stringByteLengths[charIndex];
			segmentCharLength = 1;
			carryoverLength = stringByteLengths[charIndex];
		}
	}

	/* Now purge any unused pg_mblen call because of the suffix that does not exceed MEMORY_LIMIT */
	for (int partitionCharIndex = 0; partitionCharIndex < segmentCharLength; partitionCharIndex++)
	{
		pg_mblen("a");
	}

#ifdef USE_ASSERT_CHECKING
    expect_any(ExceptionalCondition,conditionName);
    expect_any(ExceptionalCondition,errorType);
    expect_any(ExceptionalCondition,fileName);
    expect_any(ExceptionalCondition,lineNumber);
    will_be_called_with_sideeffect(ExceptionalCondition,&_ExceptionalCondition,NULL);

    /* Test if the left-over string that fits in memory cause assertion failure */
    PG_TRY();
	{
		find_memory_limited_substring(strStart, totalByteLength - cumulativeLengthConsidered, MEMORY_LIMIT, &subStringByteLength, &subStringCharLength);
	}
	PG_CATCH();
	{
		return;
	}
	PG_END_TRY();

	assert_true(false);
#endif
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test__find_memory_limited_substring__small_string),
		unit_test(test__find_memory_limited_substring__null_string),
		unit_test(test__find_memory_limited_substring__ascii_chars_within_memory_limit),
		unit_test(test__find_memory_limited_substring__mb_chars_within_memory_limit)
	};

	return run_tests(tests);
}

