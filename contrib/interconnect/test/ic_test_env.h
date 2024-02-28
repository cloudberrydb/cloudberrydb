
#include <unistd.h>

#include "postgres.h"
#include "ic_modules.h"
#include "postmaster/postmaster.h"


#ifndef IC_TEST_ENV_H
#define IC_TEST_ENV_H

#define write_data_to_pipe(fd_pipe, any, type) \
	do { \
		size_t write_size = 0; \
		write_size = write(fd_pipe[1], (void *)&any, sizeof(type)); \
	} while(0);

#define read_data_from_pipe(fd_pipe, any_ptr, type) \
	do { \
		size_t read_size = 0; \
		read_size = read(fd_pipe[0], any_ptr, sizeof(type)); \
	} while(0);


#define COLUMN_NUMS 2
#define COLUMN_TEXT_LEN 100
#define COLUMN_INT4_VALUE 0x123
#define TUPLE_CHUNK_RAW_BUFFER_LEN 200

extern void generate_seq_buffer(char *buffer, const size_t buffer_len);
extern TupleDesc prepare_tuple_desc();
extern TupleTableSlot *prepare_tuple_slot();
extern HeapTupleTableSlot *get_tuple_slot_from_chunk_list_serialized_tuple(TupleChunkListItem tc_item);
extern void cleanup_tuple_desc(TupleDesc desc);
extern void cleanup_tuple_slot(TupleTableSlot *slot);
extern void cleanup_heap_tuple_slot(HeapTupleTableSlot *slot);

extern TupleChunkListData * prepare_chunk_list_raw_data(const char *raw_buffer, const size_t raw_buffer_len);
extern void cleanup_chunk_list_raw_data(TupleChunkListData * tc_list);
extern bool verify_chunk_list_raw_data(const TupleChunkListItem tc_item_raw,
									   const char *verify_buffer, const size_t verify_buffer_len);


extern EState *prepare_estate(const int local_slice,
							  const int32 parent_listen_port,
							  const int32 child_listen_port,
							  const pid_t parent_pid,
							  const pid_t child_pid);
extern void cleanup_estate(EState *estate);

extern void client_side_global_var_init(MotionIPCLayer * motion_ipc_layer, pid_t *ic_proxy_pid);
extern void server_side_global_var_init(MotionIPCLayer * motion_ipc_layer, pid_t *ic_proxy_pid);
extern void shutdown_ic_proxy_if_need(pid_t ic_proxy_pid);

#endif // IC_TEST_ENV_H
