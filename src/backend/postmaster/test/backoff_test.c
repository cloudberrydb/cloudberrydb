#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../backoff.c"
#include "postgres.h"

#ifdef USE_ASSERT_CHECKING
/* Function passed to the testing framework in order to catch
 * the failed assertion.
 */
void
_ExceptionalCondition( )
{
	PG_RE_THROW();
}

/* Calls _ExceptionalCondition after ExceptionalCondition is
 executed. */
void
_AssertionSetting()
{
	expect_any(ExceptionalCondition,conditionName);
	expect_any(ExceptionalCondition,errorType);
	expect_any(ExceptionalCondition,fileName);
	expect_any(ExceptionalCondition,lineNumber);
	will_be_called_with_sideeffect(ExceptionalCondition,&_ExceptionalCondition,NULL);
}
#endif

/* Tests that calling numProcsPerSegment doesn't change the value of gp_resqueue_priority_cpucores_per_segment. */
void
test__numProcsPerSegment__VerifyImmutableAssignment(void **state)
{
	gp_resqueue_priority_cpucores_per_segment = 1;
	gp_enable_resqueue_priority = 1;

	/* backoffSingleton is required to be non-null for successful execution of numProcsPerSegment. */
	backoffSingleton = 1;
	
	build_guc_variables();

	assert_true(numProcsPerSegment() == 1);
}

#ifdef USE_ASSERT_CHECKING
/* Tests assigning gp_resqueue_priority_cpucores_per_segment a negative
 * number. */
void
test__numProcsPerSegment__NotNegative(void **state)
{
	gp_enable_resqueue_priority = 1;
	backoffSingleton = 1;
	gp_resqueue_priority_cpucores_per_segment = -1;

	build_guc_variables();

	_AssertionSetting();

	/* Catch Mocked Assertion */
	PG_TRY();
	{
		numProcsPerSegment();
	}
	PG_CATCH();
	{
		return;
	}
	PG_END_TRY();
	assert_true(false);
}
#endif

#ifdef USE_ASSERT_CHECKING
/* Tests assigning gp_resqueue_priority_cpucores_per_segment = 0; */
void
test__numProcsPerSegment__NotZero(void **state)
{
	gp_enable_resqueue_priority = 1;
	backoffSingleton = 1;
	gp_resqueue_priority_cpucores_per_segment = 0;

	build_guc_variables();

	_AssertionSetting();

	/* Catch Mocked Assertion */
	PG_TRY();
	{
		numProcsPerSegment();
	}
	PG_CATCH();
	{
		return;
	}
	PG_END_TRY();

	assert_true(false);
	
}
#endif

int
main(int argc, char* argv[]) {
        cmockery_parse_arguments(argc, argv);

        const UnitTest tests[] = {
        		unit_test(test__numProcsPerSegment__VerifyImmutableAssignment)
#ifdef USE_ASSERT_CHECKING			
			, unit_test(test__numProcsPerSegment__NotNegative)
                        , unit_test(test__numProcsPerSegment__NotZero)
#endif  
	};
        return run_tests(tests);
}


