#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "../mcxt.c"

#include "utils/memaccounting.h"

/*
 * Checks if MemoryContextInit() calls MemoryAccounting_Reset()
 */
void 
test__MemoryContextInit__CallsMemoryAccountingReset(void **state)
{
	will_be_called(MemoryAccounting_Reset);
	MemoryContextInit();
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test__MemoryContextInit__CallsMemoryAccountingReset)
	};

	return run_tests(tests);
}
