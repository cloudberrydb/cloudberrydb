#include <assert.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <getopt.h>

#include "postgres.h"
#include "ic_modules.h"
#include "ic_internal.h"
#include "postmaster/postmaster.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "storage/latch.h"
#include "ic_test_env.h"

#define MTU_LESS_LEN (sizeof(struct icpkthdr) + TUPLE_CHUNK_HEADER_SIZE) + 1

const char *progname = NULL;
static MemoryContext testMemoryContext = NULL;

volatile bool interrupt_flag = false;
bool		am_client_side = false;
pid_t		server_side_pid = -1;
pid_t		client_side_pid = -1;

pid_t		server_ic_proxy_pid = -1;
pid_t		client_ic_proxy_pid = -1;

static struct option long_options[] = {
	{"help", no_argument, NULL, '?'},
	{"type", required_argument, NULL, 't'},
	{"interval", required_argument, NULL, 'i'},
	{"verify", optional_argument, NULL, 'v'},
	{"mtu", required_argument, NULL, 'm'},
	{"direct", required_argument, NULL, 'd'},
	{NULL, 0, NULL, 0}
};

struct bench_options
{
	int			ic_type;
	int			interval;
	bool		should_verify;
	/* in fact, mtu is currently only valid for the client side */
	int			mtu;
	int			bsize;
	bool		direct_buffer;

	MotionIPCLayer *ipc_layer;
};

static inline void
usage()
{
	printf("%s - Interconntect benchmark \n\n", progname);
	printf("Usage:\n  %s [OPTION]...\n\n", progname);
	printf("Options:\n");

	printf("  -t, --type <ic type>                Specify the interconnection type to run benchmark\n"
		   "                                      The value range is [0-2]:\n"
		   "                                        0 - tcp\n"
		   "                                        1 - udpifc\n"
		   "                                        2 - proxy\n"
		   "                                        default tcp(0)\n");
	printf("  -i, --interval <second>             The duration of the benchmark. default is \"60s\"\n");
	printf("  -v, --verify                        Verify the result in recv side. default is \"false\"\n");
	printf("  -m, --mtu                           The MTU setting. default is \"1500\"\n");
	printf("  -b, --bsize                         The each buffer send size. default is \"200\"\n");
	printf("  -d, --direct                        Use direct buffer in sender. default is \"false\"\n");
}

static void
init_memory_context()
{
	if (NULL == TopMemoryContext)
	{
		assert(NULL == testMemoryContext);
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
destroy_memory_context()
{
	MemoryContextReset(testMemoryContext);
	testMemoryContext = NULL;
	TopMemoryContext = NULL;
	CurrentMotionIPCLayer = NULL;
}

static EState *
client_side_setup(const struct bench_options *options, int stc[2], int cts[2])
{
	int32		listen_port = 0;
	int32		server_listen_port = 0;
	pid_t		c_pid = 0;
	int8		already_setup = 1;
	EState	   *estate;

	am_client_side = true;
	client_side_global_var_init(options->ipc_layer, &client_ic_proxy_pid);
	Gp_max_packet_size = options->mtu;

	CurrentMotionIPCLayer->InitMotionLayerIPC();

	listen_port = CurrentMotionIPCLayer->GetListenPort();
	if (listen_port == 0)
	{
		printf("failed to init motion layer ipc.");
		return NULL;
	}

	write_data_to_pipe(cts, listen_port, int32);
	read_data_from_pipe(stc, &server_listen_port, int32);

	c_pid = getpid();
	write_data_to_pipe(cts, c_pid, pid_t);
	read_data_from_pipe(stc, &server_side_pid, pid_t);

	estate = prepare_estate( /* local_slice */ 1, server_listen_port,
							listen_port,
							server_side_pid,
							c_pid);

	CurrentMotionIPCLayer->SetupInterconnect(estate);
	if (!estate->es_interconnect_is_setup || !estate->interconnect_context)
	{
		cleanup_estate(estate);
		printf("failed to setup motion layer ipc.");
		return NULL;
	}

	write_data_to_pipe(cts, already_setup, int8);
	return estate;
}

EState *
server_side_setup(const struct bench_options *options,
				  int stc[2],
				  int cts[2])
{
	int32		listen_port = 0;
	int32		client_listen_port = 0;
	int8		already_setup = 0;
	pid_t		c_pid = 0;
	EState	   *estate;

	am_client_side = false;
	server_side_global_var_init(options->ipc_layer, &server_ic_proxy_pid);
	Gp_max_packet_size = options->mtu;

	CurrentMotionIPCLayer->InitMotionLayerIPC();

	listen_port = CurrentMotionIPCLayer->GetListenPort();
	if (listen_port == 0)
	{
		printf("failed to init motion layer ipc.");
		return NULL;
	}

	read_data_from_pipe(cts, &client_listen_port, int32);
	write_data_to_pipe(stc, listen_port, int32);

	c_pid = getpid();
	read_data_from_pipe(cts, &client_side_pid, pid_t);
	write_data_to_pipe(stc, c_pid, pid_t);

	estate = prepare_estate( /* local_slice */ 0,
							listen_port,
							client_listen_port,
							c_pid,
							client_side_pid);

	CurrentMotionIPCLayer->SetupInterconnect(estate);
	if (!estate->es_interconnect_is_setup || !estate->interconnect_context)
	{
		cleanup_estate(estate);
		printf("failed to setup motion layer ipc.");
		return NULL;
	}

	/* waiting for client setup */
	read_data_from_pipe(cts, &already_setup, int8);
	if (already_setup != 1)
	{
		printf("failed to recv client setup signal");
		cleanup_estate(estate);
		return NULL;
	}

	return estate;
}

void
sig_handler(int sig_num)
{
	switch (sig_num)
	{
		case SIGALRM:
		case SIGUSR1:
			{
				interrupt_flag = true;
				break;
			}
		default:
			{
				/* do nothing */
				break;
			}
	}
}

void
sig_stop()
{
	interrupt_flag = true;
	if (am_client_side)
	{
		assert(server_side_pid != -1);
		kill(server_side_pid, SIGUSR1);
	}
	else
	{
		assert(client_side_pid != -1);
		kill(client_side_pid, SIGUSR1);
	}
}

static TupleChunkListData *
build_chunk_tuple_slot(EState *estate, size_t size)
{
	TupleChunkListData *tc_list;
	char		tc_list_raw_buffer[size];

	generate_seq_buffer(tc_list_raw_buffer, size);
	tc_list = prepare_chunk_list_raw_data(tc_list_raw_buffer, size);

	return tc_list;
}

static bool
measure_chunk_tuple_list(char *verify_buffer, int verify_buff_len, TupleChunkListItem tc_item,
						 uint64 *total_recv_size, uint64 *total_recv_chunk_item_counts)
{

	TupleChunkListItem p_curr = tc_item;
	TupleChunkListItem p_last;

	if (!p_curr)
	{
		printf("recv got empty TupleChunkListItem.\n");
		return false;
	}

	while (p_curr)
	{
		if (verify_buffer && verify_buff_len != 0 && !verify_chunk_list_raw_data(p_curr, verify_buffer, verify_buff_len))
		{
			printf("recv TupleChunkListItem not matched.\n");
			return false;
		}

		*total_recv_size = *total_recv_size + p_curr->chunk_length;
		(*total_recv_chunk_item_counts)++;
		p_last = p_curr;
		p_curr = p_curr->p_next;
		pfree(p_last);
	}

	return true;
}

static void
print_direct_mode_summary(const struct bench_options *options,
						  const uint64 direct_hit,
						  const uint64 non_direct_hit)
{
	char		pbuff[1024 * 100];
	int			n = 0;

	setbuf(stdout, NULL);
	n = sprintf(pbuff, "+----------------+------------+\n");
	n += sprintf(pbuff + n, "| %-14s | %10ld |\n", "Direct hits", direct_hit);
	n += sprintf(pbuff + n, "| %-14s | %10ld |\n", "Ndirect hits", non_direct_hit);
	/* non-direct hits/direct hits */
	n += sprintf(pbuff + n, "| %-14s | %10.4f |\n", "n/d hits rate", direct_hit == 0
				 ? 0 : (double) non_direct_hit / direct_hit);
	/* buffer size/mtu */
	n += sprintf(pbuff + n, "| %-14s | %10.4f |\n", "b/m ratio", (double) options->bsize / options->mtu);
	sprintf(pbuff + n, "+----------------+------------+\n");

	printf("%s", pbuff);
}


void
client_loop(const struct bench_options *options, int stc[2], int cts[2])
{
	EState	   *estate;
	struct itimerval timeout_val;
	bool		has_error = false;
	TupleChunkListData *tc_list_raw_buffer;
	struct directTransportBuffer direct_buffer;
	int8		already_stop = 0;

	uint64		direct_hit = 0;
	uint64		non_direct_hit = 0;

	init_memory_context();
	estate = client_side_setup(options, stc, cts);
	if (!estate)
	{
		/* can not use signal to notify server side now */
		printf("client side setup failed.\n");
		return;
	}

	signal(SIGALRM, sig_handler);
	signal(SIGUSR1, sig_handler);

	tc_list_raw_buffer = build_chunk_tuple_slot(estate, options->bsize);

	timeout_val.it_value.tv_sec = options->interval;
	timeout_val.it_value.tv_usec = 0;
	timeout_val.it_interval.tv_sec = 0;
	timeout_val.it_interval.tv_usec = 0;
	setitimer(ITIMER_REAL, &timeout_val, NULL);

	while (true)
	{
		if (interrupt_flag)
		{
			kill(server_side_pid, SIGALRM);
			int			n = 0;

			if (fcntl(stc[0], F_SETFL, fcntl(stc[0], F_GETFL) | O_NONBLOCK) != 0)
			{
				printf("client side exit failed.\n");
				return;
			}

			/* waiting for server side stuck in recv or break. */
			sleep(0.1);
			while ((n = read(stc[0], &already_stop, sizeof(int8))) < 0)
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK)
				{
					CurrentMotionIPCLayer->SendTupleChunkToAMS(estate->interconnect_context, 1, 0, tc_list_raw_buffer->p_first);
					continue;
				}
				else
				{
					printf("client side read signal failed failed.\n");
				}
			}
			break;
		}

		if (options->direct_buffer)
		{
			CurrentMotionIPCLayer->GetTransportDirectBuffer(estate->interconnect_context, 1, 0, &direct_buffer);
			if (direct_buffer.prilen < tc_list_raw_buffer->p_first->chunk_length)
			{
				non_direct_hit++;
				CurrentMotionIPCLayer->SendTupleChunkToAMS(estate->interconnect_context, 1, 0, tc_list_raw_buffer->p_first);
			}
			else
			{
				direct_hit++;
				memcpy(direct_buffer.pri, tc_list_raw_buffer->p_first->chunk_data, tc_list_raw_buffer->p_first->chunk_length);
				CurrentMotionIPCLayer->PutTransportDirectBuffer(estate->interconnect_context, 1, 0, tc_list_raw_buffer->p_first->chunk_length);
			}
		}
		else
		{
			CurrentMotionIPCLayer->SendTupleChunkToAMS(estate->interconnect_context, 1, 0, tc_list_raw_buffer->p_first);
		}
	}

	if (options->direct_buffer)
	{
		print_direct_mode_summary(options, direct_hit, non_direct_hit);
	}


	CurrentMotionIPCLayer->TeardownInterconnect(estate->interconnect_context, &has_error);
	assert(!has_error);

	CurrentMotionIPCLayer->CleanUpMotionLayerIPC();

	shutdown_ic_proxy_if_need(client_ic_proxy_pid);

	cleanup_estate(estate);
	destroy_memory_context();
}

static void
print_summary(const double elapsed_time, const uint32 loop_times,
			  const uint64 total_recv_size, const uint64 total_recv_chunk_item_counts)
{
	char		pbuff[1024 * 100];
	int			n = 0;

	setbuf(stdout, NULL);
	n = sprintf(pbuff, "+----------------+------------+\n");
	n += sprintf(pbuff + n, "| %-14s | %10.3f |\n", "Total time(s)", elapsed_time / 1000);
	n += sprintf(pbuff + n, "| %-14s | %10ld |\n", "Loop times", loop_times);
	n += sprintf(pbuff + n, "| %-14s | %10.3f |\n", "LPS(l/ms)", (double) (loop_times / elapsed_time));
	n += sprintf(pbuff + n, "| %-14s | %10ld |\n", "Recv mbs", total_recv_size / 1024 / 1024);
	n += sprintf(pbuff + n, "| %-14s | %10.3f |\n", "TPS(mb/s)", (double) (total_recv_size / (elapsed_time / 1000) / 1024 / 1024));
	n += sprintf(pbuff + n, "| %-14s | %10ld |\n", "Recv counts", total_recv_chunk_item_counts);
	n += sprintf(pbuff + n, "| %-14s | %10.3f |\n", "Items ops/ms", (double) (total_recv_chunk_item_counts / (elapsed_time)));
	sprintf(pbuff + n, "+----------------+------------+\n");
	printf("%s", pbuff);
}

void
server_loop(const struct bench_options *options, int stc[2], int cts[2])
{
	EState	   *estate;
	struct timeval start_time,
				end_time;
	double		elapsed_time;
	TupleChunkListItem tc_item;
	bool		has_error = false;
	uint32		loop_times = 0;
	uint64		total_recv_size = 0;
	uint64		total_recv_chunk_item_counts = 0;
	char	   *tc_item_raw_verify_buff;
	int			tc_item_raw_verify_buff_len = 0;
	int8		already_stop = 1;

	init_memory_context();
	estate = server_side_setup(options, stc, cts);
	if (!estate)
	{
		/* can not use signal to notify client side now */
		printf("server side setup failed.\n");
		return;
	}

	signal(SIGALRM, sig_handler);
	signal(SIGUSR1, sig_handler);

	if (options->should_verify)
	{
		tc_item_raw_verify_buff_len = options->bsize;
		tc_item_raw_verify_buff = palloc(tc_item_raw_verify_buff_len);
		generate_seq_buffer(tc_item_raw_verify_buff, tc_item_raw_verify_buff_len);
	}

	gettimeofday(&start_time, NULL);
	while (true)
	{
		if (interrupt_flag)
		{
			write_data_to_pipe(stc, already_stop, int8);
			break;
		}

		tc_item = CurrentMotionIPCLayer->RecvTupleChunkFrom(estate->interconnect_context, 1, 0);
		if (!measure_chunk_tuple_list(tc_item_raw_verify_buff,
									  tc_item_raw_verify_buff_len,
									  tc_item,
									  &total_recv_size,
									  &total_recv_chunk_item_counts))
		{
			sig_stop();
		}

		if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_UDPIFC)
		{
			CurrentMotionIPCLayer->DirectPutRxBuffer(estate->interconnect_context, 1, 0);
		}

		loop_times++;
	}

	gettimeofday(&end_time, NULL);
	elapsed_time = (double) (end_time.tv_sec - start_time.tv_sec) * 1000 +
		(double) (end_time.tv_usec - start_time.tv_usec) / 1000;

	print_summary(elapsed_time, loop_times, total_recv_size, total_recv_chunk_item_counts);

	if (options->should_verify)
	{
		pfree(tc_item_raw_verify_buff);
	}

	CurrentMotionIPCLayer->TeardownInterconnect(estate->interconnect_context, &has_error);
	assert(!has_error);

	CurrentMotionIPCLayer->CleanUpMotionLayerIPC();
	shutdown_ic_proxy_if_need(server_ic_proxy_pid);

	cleanup_estate(estate);
	destroy_memory_context();
}


int
main(int argc, char *argv[])
{
	int			stc[2],
				cts[2];
	pid_t		f_pid;
	int			c;

	struct bench_options options = {
		.ic_type = 0,
		.interval = 60,
		.should_verify = false,
		/* the default MTU is 8192 in GUC 
		 * but in a production environment, DBA will generally set it to 1500
		 */ 
		.mtu = 1500,
		.bsize = TUPLE_CHUNK_RAW_BUFFER_LEN,
		.ipc_layer = NULL,
		.direct_buffer = false
	};

	progname = get_progname(argv[0]);
	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0
			|| strcmp(argv[1], "-h") == 0)
		{
			usage();
			exit(0);
		}
	}

	optind = 1;
	while (optind < argc)
	{
		while ((c = getopt_long(argc, argv, "t:i:vm:b:d",
								long_options, NULL)) != -1)
		{
			switch (c)
			{
				case 't':
					{
						char	   *type_c;

						type_c = strdup(optarg);
						options.ic_type = atoi(type_c);
						free(type_c);
						break;
					}
				case 'i':
					{
						char	   *interval_c;

						interval_c = strdup(optarg);
						options.interval = atoi(interval_c);
						free(interval_c);
						break;
					}
				case 'v':
					{
						options.should_verify = true;
						break;
					}
				case 'm':
					{
						char	   *mtu_c;

						mtu_c = strdup(optarg);
						options.mtu = atoi(mtu_c);
						free(mtu_c);
						break;
					}
				case 'b':
					{
						char	   *buffer_size_c;

						buffer_size_c = strdup(optarg);
						options.bsize = atoi(buffer_size_c);
						free(buffer_size_c);
						break;
					}
				case 'd':
					{
						options.direct_buffer = true;
						break;
					}
				default:
					{
						/* do nothing */
						break;
					}
			}
		}
	}

	switch (options.ic_type)
	{
		case INTERCONNECT_TYPE_TCP:
			{
				options.ipc_layer = &tcp_ipc_layer;
				break;
			}
		case INTERCONNECT_TYPE_UDPIFC:
			{
				options.ipc_layer = &udpifc_ipc_layer;
				break;
			}
		case INTERCONNECT_TYPE_PROXY:
			{
				options.ipc_layer = &proxy_ipc_layer;
				break;
			}
		default:
			{
				printf("invalid of args -t/--type %d\n", options.ic_type);
				usage();
				return -1;
			}
	}

	if (options.interval <= 1)
	{
		printf("invalid of args -i/--interval %d\n", options.interval);
		usage();
		return -1;
	}

	if (options.mtu < MTU_LESS_LEN)
	{
		printf("invalid of args -m/--mtu %d, should not less than \n", options.mtu, MTU_LESS_LEN);
		usage();
		return -1;
	}

	if (options.ic_type == INTERCONNECT_TYPE_PROXY && options.mtu != 1500)
	{
		printf("invalid of args -m/--mtu %d, proxy not allow setting mtu. \n", options.mtu);
		usage();
		return -1;
	}

	if (options.bsize <= 0 || options.bsize >= (options.mtu - MTU_LESS_LEN))
	{
		printf("invalid of args -b/--bsize %d, should not bigger than (mtu - header).\n", options.bsize);
		usage();
		return -1;
	}

	if (pipe(stc) < 0)
	{
		printf("pipe created failed. errno: %d\n", errno);
		return -1;
	}

	if (pipe(cts) < 0)
	{
		printf("pipe created failed. errno: %d\n", errno);
		close(stc[0]);
		close(stc[1]);
		return -1;
	}

	f_pid = fork();

	if (f_pid < 0)
	{
		printf("fork failed. errno: %d\n", errno);
		close(stc[0]);
		close(stc[1]);
		close(cts[0]);
		close(cts[1]);
		return -1;
	}

	if (f_pid == 0)
	{
		client_loop(&options, stc, cts);
		close(stc[0]);
		close(stc[1]);
		close(cts[0]);
		close(cts[1]);
	}
	else
	{
		server_loop(&options, stc, cts);
	}

	wait(NULL);
	close(stc[0]);
	close(stc[1]);
	close(cts[0]);
	close(cts[1]);
	return 0;
}
