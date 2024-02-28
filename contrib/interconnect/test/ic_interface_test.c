
#include <unistd.h>
#include <sys/wait.h>

#include "postgres.h"
#include "cmockery.h"
#include "ic_modules.h"
#include "postmaster/postmaster.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "storage/latch.h"
#include "ic_test_env.h"

const char *progname = NULL;
static MemoryContext testMemoryContext = NULL;
pid_t		server_ic_proxy_pid = -1;
pid_t		client_ic_proxy_pid = -1;

static void
verify_chunk_tuple_list_serialized_tuple(TupleChunkListItem tc_item)
{
	HeapTupleTableSlot *heap_tuple_slot = NULL;

	char		tc_item_verify_buff[COLUMN_TEXT_LEN];

	heap_tuple_slot = get_tuple_slot_from_chunk_list_serialized_tuple(tc_item);
	assert_true(heap_tuple_slot);

	assert_int_equal(VARSIZE_ANY_EXHDR(heap_tuple_slot->base.tts_values[0]), COLUMN_TEXT_LEN);

	generate_seq_buffer(tc_item_verify_buff, COLUMN_TEXT_LEN);
	/* 1b string header */
	assert_string_equal(tc_item_verify_buff, DatumGetPointer(heap_tuple_slot->base.tts_values[0]) + 1);
	assert_int_equal(DatumGetPointer(heap_tuple_slot->base.tts_values[1]), COLUMN_INT4_VALUE);

	cleanup_heap_tuple_slot(heap_tuple_slot);
}

static void
verify_chunk_tuple_list(TupleChunkListItem tc_item)
{
	char		tc_item_raw_verify_buff[TUPLE_CHUNK_RAW_BUFFER_LEN];

	assert_true(tc_item);
	assert_true(tc_item->p_next);
	assert_false(tc_item->p_next->p_next);

	verify_chunk_tuple_list_serialized_tuple(tc_item);
	generate_seq_buffer(tc_item_raw_verify_buff, TUPLE_CHUNK_RAW_BUFFER_LEN);
	assert_true(verify_chunk_list_raw_data(tc_item->p_next, tc_item_raw_verify_buff, TUPLE_CHUNK_RAW_BUFFER_LEN));
}


static TupleChunkListData *
build_chunk_tuple_slot(EState *estate, bool direct)
{
	TupleChunkListData *tc_list;
	TupleTableSlot *slot;
	int			sent = 0;
	TupleChunkListItem item;
	int32		raw_buffer_len = 0;

	if (direct)
	{
		SerTupInfo	tup_info;
		struct directTransportBuffer direct_buffer;

		tc_list = (TupleChunkListData *) palloc0(sizeof(TupleChunkListData));

		slot = prepare_tuple_slot();
		tup_info.tupdesc = slot->tts_tupleDescriptor;

		CurrentMotionIPCLayer->GetTransportDirectBuffer(estate->interconnect_context, 1, 0, &direct_buffer);
		assert_int_not_equal(direct_buffer.pri, 0);
		assert_int_not_equal(direct_buffer.prilen, 0);

		sent = SerializeTuple(slot, &tup_info, &direct_buffer, tc_list, 0);
		assert_int_not_equal(sent, 0);

		CurrentMotionIPCLayer->PutTransportDirectBuffer(estate->interconnect_context, 1, 0, sent);
		cleanup_tuple_slot(slot);
	}
	else
	{
		/* Don't use InitSerTupInfo */
		/* cause can't call SerializeTuple with system cache */
		/*
		 * just build tc_list without call SerializeTuple when `direct` is
		 * false
		 */

		char		tc_list_raw_buffer[TUPLE_CHUNK_RAW_BUFFER_LEN];

		generate_seq_buffer(tc_list_raw_buffer, TUPLE_CHUNK_RAW_BUFFER_LEN);
		tc_list = prepare_chunk_list_raw_data(tc_list_raw_buffer, TUPLE_CHUNK_RAW_BUFFER_LEN);
	}

	return tc_list;
}

static EState *
client_side_setup(MotionIPCLayer * motion_ipc_layer, int stc[2], int cts[2])
{
	int32		listen_port = 0;
	int32		server_listen_port = 0;
	pid_t		c_pid = 0;
	pid_t		server_pid = 0;
	int8		already_setup = 1;
	EState	   *estate;

	client_side_global_var_init(motion_ipc_layer, &client_ic_proxy_pid);

	CurrentMotionIPCLayer->InitMotionLayerIPC();

	listen_port = CurrentMotionIPCLayer->GetListenPort();
	assert_int_not_equal(listen_port, 0);

	write_data_to_pipe(cts, listen_port, int32);
	read_data_from_pipe(stc, &server_listen_port, int32);

	c_pid = getpid();
	write_data_to_pipe(cts, c_pid, pid_t);
	read_data_from_pipe(stc, &server_pid, pid_t);

	estate = prepare_estate( /* local_slice */ 1, server_listen_port,
							listen_port,
							server_pid,
							c_pid);

	CurrentMotionIPCLayer->SetupInterconnect(estate);
	assert_true(estate->es_interconnect_is_setup);
	assert_true(estate->interconnect_context != NULL);

	write_data_to_pipe(cts, already_setup, int8);
	return estate;
}

static void
forked_client(void **state, int stc[2], int cts[2], MotionIPCLayer * motion_ipc_layer)
{
	EState	   *estate;
	TupleChunkListData *tc_list1;
	TupleChunkListData *tc_list2;
	bool		has_error = false;

	assert_false(CurrentMotionIPCLayer);

	estate = client_side_setup(motion_ipc_layer, stc, cts);
	assert_true(estate);
	assert_true(CurrentMotionIPCLayer);

	tc_list1 = build_chunk_tuple_slot(estate, true);
	tc_list2 = build_chunk_tuple_slot(estate, false);

	CurrentMotionIPCLayer->SendTupleChunkToAMS(estate->interconnect_context, 1, 0, tc_list1->p_first);
	CurrentMotionIPCLayer->SendEOS(estate->interconnect_context, 1, tc_list2->p_first);

	CurrentMotionIPCLayer->TeardownInterconnect(estate->interconnect_context, &has_error);
	assert_false(has_error);

	CurrentMotionIPCLayer->CleanUpMotionLayerIPC();

	shutdown_ic_proxy_if_need(client_ic_proxy_pid);

	cleanup_chunk_list_raw_data(tc_list1);
	cleanup_chunk_list_raw_data(tc_list2);
	cleanup_estate(estate);
}


static EState *
server_side_setup(MotionIPCLayer * motion_ipc_layer, int stc[2], int cts[2])
{
	int32		listen_port = 0;
	int32		client_listen_port = 0;
	pid_t		c_pid = 0;
	pid_t		client_pid = 0;
	int8		already_setup = 1;
	EState	   *estate;

	server_side_global_var_init(motion_ipc_layer, &server_ic_proxy_pid);

	CurrentMotionIPCLayer->InitMotionLayerIPC();

	listen_port = CurrentMotionIPCLayer->GetListenPort();
	assert_int_not_equal(listen_port, 0);

	read_data_from_pipe(cts, &client_listen_port, int32);
	write_data_to_pipe(stc, listen_port, int32);

	c_pid = getpid();
	read_data_from_pipe(cts, &client_pid, pid_t);
	write_data_to_pipe(stc, c_pid, pid_t);

	estate = prepare_estate( /* local_slice */ 0, listen_port,
							client_listen_port,
							c_pid,
							client_pid);

	CurrentMotionIPCLayer->SetupInterconnect(estate);
	assert_true(estate->es_interconnect_is_setup);
	assert_true(estate->interconnect_context != NULL);

	/* waiting for client setup */
	read_data_from_pipe(cts, &already_setup, int8);
	assert_int_equal(1, already_setup);

	return estate;
}

static void
main_server(void **state, int stc[2], int cts[2], MotionIPCLayer * motion_ipc_layer)
{
	int16		src_route = 0;
	TupleChunkListItem tc_item;
	EState	   *estate;
	bool		has_error = false;

	assert_false(CurrentMotionIPCLayer);

	estate = server_side_setup(motion_ipc_layer, stc, cts);
	assert_true(estate);
	assert_true(CurrentMotionIPCLayer);

	tc_item = CurrentMotionIPCLayer->RecvTupleChunkFromAny(estate->interconnect_context, 1, &src_route);
	verify_chunk_tuple_list(tc_item);

	CurrentMotionIPCLayer->TeardownInterconnect(estate->interconnect_context, &has_error);
	assert_false(has_error);

	CurrentMotionIPCLayer->CleanUpMotionLayerIPC();

	shutdown_ic_proxy_if_need(server_ic_proxy_pid);

	pfree(tc_item);
	cleanup_estate(estate);
}

static void
test_ic_interface(void **state, MotionIPCLayer * motion_ipc_layer)
{
	pid_t		f_pid;
	int			stc[2],
				cts[2];

	assert_false(CurrentMotionIPCLayer);

	assert_int_equal(pipe(stc), 0);
	assert_int_equal(pipe(cts), 0);

	f_pid = fork();

	assert_true(f_pid >= 0);
	if (f_pid == 0)
	{
		forked_client(state, stc, cts, motion_ipc_layer);

		close(stc[0]);
		close(stc[1]);
		close(cts[0]);
		close(cts[1]);
		MemoryContextReset(testMemoryContext);
		testMemoryContext = NULL;

		/*
		 * client side should not return assert can be catch in forked process
		 */
		exit(0);
	}

	main_server(state, stc, cts, motion_ipc_layer);
	wait(NULL);

	close(stc[0]);
	close(stc[1]);
	close(cts[0]);
	close(cts[1]);
}

static void
test_ic_interface_tcp(void **state)
{
	test_ic_interface(state, &tcp_ipc_layer);
}

static void
test_ic_interface_udpifc(void **state)
{
	test_ic_interface(state, &udpifc_ipc_layer);
}

static void
test_ic_interface_proxy(void **state)
{
	if (!proxy_ipc_layer.IcProxyServiceMain)
	{
		printf("enable-ic-proxy not set. skiped.\n");
		return;
	}
	test_ic_interface(state, &proxy_ipc_layer);
}

static void
setup_data_structures(void **state)
{
	if (NULL == TopMemoryContext)
	{
		assert_true(NULL == testMemoryContext);
		MemoryContextInit();

		testMemoryContext = AllocSetContextCreate(TopMemoryContext,
												  "Test Context",
												  ALLOCSET_DEFAULT_MINSIZE,
												  ALLOCSET_DEFAULT_INITSIZE,
												  ALLOCSET_DEFAULT_MAXSIZE);

		MemoryContextSwitchTo(testMemoryContext);
	}
}

static void
teardown_data_structures(void **state)
{
	MemoryContextReset(testMemoryContext);
	testMemoryContext = NULL;
	TopMemoryContext = NULL;
	CurrentMotionIPCLayer = NULL;
}

int
main(int argc, char *argv[])
{
	progname = get_progname(argv[0]);

	cmockery_parse_arguments(argc, argv);

	const		UnitTest tests[] = {
		unit_test_setup_teardown(test_ic_interface_tcp, setup_data_structures, teardown_data_structures),
		unit_test_setup_teardown(test_ic_interface_udpifc, setup_data_structures, teardown_data_structures),
		unit_test_setup_teardown(test_ic_interface_proxy, setup_data_structures, teardown_data_structures)
	};

	return run_tests(tests);
	return 0;
}
