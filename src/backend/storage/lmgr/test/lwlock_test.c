#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "../lwlock.c"

/* Returns true if passed in proc is found in the waiters list */
bool
FindProcInWaitersList(LWLockId lockId, PGPROC *proc)
{
	LWLock myLock = LWLockArray[lockId].lock;

	PGPROC *curr = myLock.head;
	while(curr)
	{
		if (curr == proc)
			return true;
		curr = curr->lwWaitLink;
	}
	return false;
}

/*
 * Unit test for LWLockCancelWait(). Tests revolve mostly
 * around current PGPROC (MyProc) working on LWLock waiters linked list.
 */
void
test__LWLockCancelWait(void **state)
{
	LWLockPadded myLWLockPaddedArray[5];
	PGPROC proc0, proc1, proc2, proc3;

	LWLockArray = &myLWLockPaddedArray;
	memset(LWLockArray, 0, sizeof(myLWLockPaddedArray));

	MyProc = &proc2;

	/*-----------------------------------------------------------------*/
	/* -- Case 1: No waiters -- */

	// Create setup
	lwWaitingLockId = 1;
	MyProc->lwWaiting = true;
	LWLockArray[lwWaitingLockId].lock.head =
			LWLockArray[lwWaitingLockId].lock.tail = NULL;

	LWLockWaitCancel();

	// Expect not to be on the waiters list
	assert_true(!FindProcInWaitersList(lwWaitingLockId, MyProc));

	/*-----------------------------------------------------------------*/
	/* -- Case 2: myProc->end (myProc is both head and tail) -- */

	// Create setup
	MyProc->lwWaiting = true;
	MyProc->lwWaitLink = NULL;
	LWLockArray[lwWaitingLockId].lock.head =
			LWLockArray[lwWaitingLockId].lock.tail = MyProc;

	LWLockWaitCancel();

	// Expect head and tail to be set to null
	assert_true(!FindProcInWaitersList(lwWaitingLockId, MyProc));
	assert_true(!LWLockArray[lwWaitingLockId].lock.head);
	assert_true(!LWLockArray[lwWaitingLockId].lock.tail);
	assert_true(!MyProc->lwWaitLink);

	/*-----------------------------------------------------------------*/
	/* -- Case 3: myProc->proc1->proc3->end -- */

	// Create setup
	MyProc->lwWaiting = true;
	LWLockArray[lwWaitingLockId].lock.head = MyProc;
	MyProc->lwWaitLink = &proc1;
	proc1.lwWaitLink = &proc3;
	proc3.lwWaitLink = NULL;
	LWLockArray[lwWaitingLockId].lock.tail = &proc3;

	LWLockWaitCancel();

	// Expect head to be changed, MyProc to be out
	assert_false(FindProcInWaitersList(lwWaitingLockId, MyProc));
	assert_true(LWLockArray[lwWaitingLockId].lock.head == &proc1);
	assert_true(LWLockArray[lwWaitingLockId].lock.tail == &proc3);
	assert_false(MyProc->lwWaitLink);

	/*-----------------------------------------------------------------*/
	/* Case 4: proc0->proc1->myProc->proc3->end */

	// Create setup
	MyProc->lwWaiting = true;
	LWLockArray[lwWaitingLockId].lock.head = &proc0;
	proc0.lwWaitLink = &proc1;
	proc1.lwWaitLink = MyProc;
	MyProc->lwWaitLink = &proc3;
	proc3.lwWaitLink = NULL;
	LWLockArray[lwWaitingLockId].lock.tail = &proc3;

	LWLockWaitCancel();

	// Expect head/tail to be un-changed, MyProc to be out
	assert_false(FindProcInWaitersList(lwWaitingLockId, MyProc));
	assert_true(LWLockArray[lwWaitingLockId].lock.head == &proc0);
	assert_true(LWLockArray[lwWaitingLockId].lock.tail == &proc3);
	assert_true(proc0.lwWaitLink == &proc1);
	assert_true(proc1.lwWaitLink == &proc3);
	assert_false(MyProc->lwWaitLink);

	/*-----------------------------------------------------------------*/
	/* Case 5: proc1->proc3->myProc->end */

	// Create setup
	MyProc->lwWaiting = true;
	LWLockArray[lwWaitingLockId].lock.head = &proc1;
	proc1.lwWaitLink = &proc3;
	proc3.lwWaitLink = MyProc;
	MyProc->lwWaitLink = NULL;
	LWLockArray[lwWaitingLockId].lock.tail = MyProc;

	LWLockWaitCancel();

	// Expect head to be un-changed, tail to be changed, MyProc to be out
	assert_false(FindProcInWaitersList(lwWaitingLockId, MyProc));
	assert_true(LWLockArray[lwWaitingLockId].lock.head == &proc1);
	assert_true(LWLockArray[lwWaitingLockId].lock.tail == &proc3);
	assert_false(MyProc->lwWaitLink);

	/*-----------------------------------------------------------------*/
	/* Case 6: proc1->proc3->end My Proc not in waiters list */

	// Create setup
	MyProc->lwWaiting = true;
	LWLockArray[lwWaitingLockId].lock.head = &proc1;
	proc1.lwWaitLink = &proc3;
	proc3.lwWaitLink = NULL;
	LWLockArray[lwWaitingLockId].lock.tail = &proc3;

	LWLockWaitCancel();

	// Expect head/tail to be un-changed, MyProc to be absent
	assert_false(FindProcInWaitersList(lwWaitingLockId, MyProc));
	assert_true(LWLockArray[lwWaitingLockId].lock.head == &proc1);
	assert_true(LWLockArray[lwWaitingLockId].lock.tail == &proc3);
	assert_true(proc1.lwWaitLink == &proc3);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test__LWLockCancelWait)
	};

	return run_tests(tests);
}
