#include "vbf.h"
#include "vfs.h"
#include "vbf_private.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

static char *rand_string(char *str, size_t size)
{
	size_t i;
	int    key;
	char   char_set[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJK";

	if (size) {
		--size;
		for (i = 0; i < size; i++) {
			key = rand() % (int) (sizeof char_set - 1);
			str[i] = char_set[key];
		}
		str[size] = '\n';
	}

	return str;
}

static int rand_integer(int	lower, int upper)
{
	return (rand() % (upper - lower + 1)) + lower;
}

static int64_t get_file_size(char *file_name)
{
	vbf_file_t *file;
	int64_t     result;

	file = vbf_file_open(file_name, vbf_file_get_open_flags(true));
	if (file == NULL) {
		fprintf(stderr, "failed to open file: %s\n", vbf_strerror());
		exit(EXIT_FAILURE);
	}

	result = vbf_file_size(file);
	if (result == -1) {
		fprintf(stderr, "failed to stat file: %s\n", vbf_strerror());
		exit(EXIT_FAILURE);
	}

	vbf_file_close(file);

	return result;
}

static void write_file(char *file_name)
{
	int   i;
	int   size;
	char *data;
	int   result;
	char  test_buffer[4096];
	vbf_writer_t writer;
	
	result = vbf_writer_init(&writer,
							 "none", /* compress_type: none, zlib, zstd */
							 0, /* compress_level: 0-9 */
							 64 * 1024, /* block_size: 8192-2097152 */
							 file_name,
							 true, /* is_create_file */
							 0, /* file_length */
							 0 /* rownum */);
	if (result == -1) {
		fprintf(stderr, "failed to init vbf_writer: %s\n", vbf_strerror());
		exit(EXIT_FAILURE);
	}


	for(i = 0; i < 25; i++) {
		size = rand_integer(512, 4096 - 1);
		data = rand_string(test_buffer, size);

		result = vbf_writer_write(&writer, (uint8_t *) data, size);
		if (result == -1) {
			fprintf(stderr, "failed to write on vbf_writer: %s\n", vbf_strerror());
			exit(EXIT_FAILURE);
		}
	}

	result = vbf_writer_flush(&writer);
	if (result == -1) {
		fprintf(stderr, "failed to flush vbf_writer: %s\n", vbf_strerror());
		exit(EXIT_FAILURE);
	}

	vbf_writer_fini(&writer);
}

static void read_file(char *file_name)
{
	int           result;
	vbf_reader_t  reader;
	uint8_t      *data;
	int           data_length;
	bool          has_next;
	
	result = vbf_reader_init(&reader,
							 "none", /* compress_type: none, zlib, zstd */
							 0, /* compress_level: 0-9 */
							 64 * 1024 /* block_size: 8192-2097152 */);
	if (result == -1) {
		fprintf(stderr, "failed to init vbf_reader: %s\n", vbf_strerror());
		exit(EXIT_FAILURE);
	}

	result = vbf_reader_reset(&reader, file_name, get_file_size(file_name));
	if (result == -1) {
		fprintf(stderr, "failed to reset vbf_reader: %s\n", vbf_strerror());
		exit(EXIT_FAILURE);
	}

	while (true) {
		result = vbf_reader_next(&reader, &data, &data_length, &has_next);
		if (result == -1) {
			fprintf(stderr, "failed to read on vbf_reader: %s\n", vbf_strerror());
			exit(EXIT_FAILURE);
		}

		if (!has_next) {
			break;
		}
	}

	vbf_reader_fini(&reader);
}

static void read_file_with_rownum(char *file_name)
{
	int           result;
	vbf_reader_t  reader;
	uint8_t      *data;
	int           data_length;
	bool          has_next;
	int64_t       rownum;
	
	result = vbf_reader_init(&reader,
							 "none", /* compress_type: none, zlib, zstd */
							 0, /* compress_level: 0-9 */
							 64 * 1024 /* block_size: 8192-2097152 */);
	if (result == -1) {
		fprintf(stderr, "failed to init vbf_reader: %s\n", vbf_strerror());
		exit(EXIT_FAILURE);
	}

	result = vbf_reader_reset(&reader, file_name, get_file_size(file_name));
	if (result == -1) {
		fprintf(stderr, "failed to reset vbf_reader: %s\n", vbf_strerror());
		exit(EXIT_FAILURE);
	}

	while (true) {
		result = vbf_reader_next_with_rownum(&reader, &data, &data_length, &rownum, &has_next);
		if (result == -1) {
			fprintf(stderr, "failed to read on vbf_reader: %s\n", vbf_strerror());
			exit(EXIT_FAILURE);
		}

		if (!has_next) {
			break;
		}
	}

	vbf_reader_fini(&reader);
}


int main(int argc, char *argv[])
{
	VBF_UNUSED(argc);
	VBF_UNUSED(argv);

	srand(time(0));

	vbf_file_delete("segfile-0");

	write_file("segfile-0");
	read_file("segfile-0");
	read_file_with_rownum("segfile-0");

	vbf_file_delete("segfile-0");

	return EXIT_SUCCESS;
}
