#include "vbf.h"
#include "vfs.h"
#include "vbf_private.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "test.h"

#define TEST_BASE_FILE       "vbf_base_file"
#define TEST_RECOVERED_FILE  "vbf_recovered_file"
#define TEST_DATA_FILE       "vbf_data_file"

int  test_cases = 0;

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

	CHECK_NOT_NULL("vbf_file_open", file, vbf_file_open(file_name, vbf_file_get_open_flags(true)));
	CHECK_SUCCESS("vbf_file_size", result, vbf_file_size(file));
	vbf_file_close(file);

	return result;
}

static void create_base_file(char *file_name)
{
	int   i;
	int   ret;
	int   size;
	char *data;
	char  test_buffer[4096];
	off_t offset = 0;
	vbf_file_t *file;

	CHECK_NOT_NULL("vbf_file_open", file , vbf_file_open(file_name, vbf_file_get_create_flags()));

	for(i = 0; i < 80000; i++) {
		size = rand_integer(512, 4096 - 1);
		data = rand_string(test_buffer, size);

		CHECK_SUCCESS("vbf_file_write", ret, vbf_file_write(file, data, size, offset));

		offset += size;
	}

	vbf_file_close(file);
}

static void create_data_file(char *base_file, char *data_file, char *compress_type, int min_record_size, int max_record_size)
{
	int size;
	int result;
	off_t offset = 0;
	char         *data;
	vbf_file_t   *file;
	vbf_writer_t  writer;

	CHECK_NOT_NULL("vbf_file_open", file, vbf_file_open(base_file, vbf_file_get_open_flags(true)));
	CHECK_SUCCESS("vbf_writer_init", result, vbf_writer_init(&writer, compress_type, 0, 64 * 1024, data_file, true, 0, 0));

	while (true) {
		size = rand_integer(min_record_size, max_record_size);
		CHECK_NOT_NULL("vbf_malloc", data, vbf_malloc(size));
		CHECK_SUCCESS("vbf_file_read", result, vbf_file_read(file, data, size, offset));
		if (result == 0) {
			vbf_free(data);
			break;
		}

		CHECK_SUCCESS("vbf_writer_write", result, vbf_writer_write(&writer, (uint8_t *) data, result));
		vbf_free(data);
		offset += size;
	}

	vbf_file_close(file);
	CHECK_SUCCESS("vbf_writer_flush", result, vbf_writer_flush(&writer));
	vbf_writer_fini(&writer);
}

static void recover_base_file(char *data_file, char *base_file, char *compress_type)
{
	int           result;
	vbf_file_t   *file;
	vbf_reader_t  reader;
	int           data_length;
	bool          has_next;
	uint8_t      *data;
	off_t         offset = 0;

	CHECK_SUCCESS("vbf_reader_init", result, vbf_reader_init(&reader, compress_type, 0, 64 * 1024));
	CHECK_SUCCESS("vbf_reader_reset", result, vbf_reader_reset(&reader, data_file, get_file_size(data_file)));

	CHECK_NOT_NULL("vbf_file_open", file, vbf_file_open(base_file, vbf_file_get_create_flags()));

	while (true) {
		CHECK_SUCCESS("vbf_reader_next", result, vbf_reader_next(&reader, &data, &data_length, &has_next));
		if (!has_next) {
			break;
		}

		CHECK_SUCCESS("vbf_file_write", result, vbf_file_write(file, (char *) data, data_length, offset));
		offset += data_length;
	}

	vbf_file_close(file);
	vbf_reader_fini(&reader);
}

static void compare_files(char *base_file, char *recovered_file)
{
	int line = 1;
	int line_pos = 0;
	int source_char;
	int target_char;
	FILE *source_file;
	FILE *target_file;

	CHECK_NOT_NULL("fopen", source_file, fopen(base_file, "r"));
	CHECK_NOT_NULL("fopen", target_file, fopen(recovered_file, "r"));

	source_char = getc(source_file);
	target_char = getc(target_file);
	while (source_char != EOF && target_char != EOF) {
		line_pos++;

		if (source_char == '\n' && target_char == '\n') {
			line++;
			line_pos = 0;
		}

		if (source_char != target_char) {
			fprintf(stderr, "line number: %d \t error position: %d", line, line_pos);
			exit(EXIT_FAILURE);
        }

		source_char = getc(source_file);
		target_char = getc(target_file);
    }

	fclose(source_file);
	fclose(target_file);
}

static void test_create(void)
{
	int result;
	vbf_writer_t writer;

	vbf_file_delete(TEST_DATA_FILE);

	CHECK_SUCCESS("vbf_writer_init",
					result,
					vbf_writer_init(&writer, "none", 0, 64 * 1024, TEST_DATA_FILE, true, 0, 0));
	CHECK_SUCCESS("vbf_writer_flush",
					result,
					vbf_writer_flush(&writer));
	vbf_writer_fini(&writer);

	CHECK_SUCCESS("vbf_writer_init",
					result,
					vbf_writer_init(&writer, "zlib", 0, 64 * 1024, TEST_DATA_FILE, false, 0, 0));
	CHECK_SUCCESS("vbf_writer_flush",
					result,
					vbf_writer_flush(&writer));
	vbf_writer_fini(&writer);

	CHECK_SUCCESS("vbf_writer_init",
					result,
					vbf_writer_init(&writer, "zstd", 0, 64 * 1024, TEST_DATA_FILE, false, 0, 0));
	CHECK_SUCCESS("vbf_writer_flush",
					result,
					vbf_writer_flush(&writer));
	vbf_writer_fini(&writer);

	vbf_file_delete(TEST_DATA_FILE);
	test_cases++;
}

static void test_small_content(char *compress_type)
{
	vbf_file_delete(TEST_RECOVERED_FILE);
	vbf_file_delete(TEST_DATA_FILE);

	create_data_file(TEST_BASE_FILE, TEST_DATA_FILE, compress_type, 256, 4096);
	recover_base_file(TEST_DATA_FILE, TEST_RECOVERED_FILE, compress_type);
	compare_files(TEST_BASE_FILE, TEST_RECOVERED_FILE);

	vbf_file_delete(TEST_RECOVERED_FILE);
	vbf_file_delete(TEST_DATA_FILE);

	test_cases++;
}

static void test_large_content(char *compress_type)
{
	vbf_file_delete(TEST_RECOVERED_FILE);
	vbf_file_delete(TEST_DATA_FILE);

	create_data_file(TEST_BASE_FILE, TEST_DATA_FILE, compress_type, 64 * 1024, 16 * 1024 * 1024);
	recover_base_file(TEST_DATA_FILE, TEST_RECOVERED_FILE, compress_type);
	compare_files(TEST_BASE_FILE, TEST_RECOVERED_FILE);

	vbf_file_delete(TEST_RECOVERED_FILE);
	vbf_file_delete(TEST_DATA_FILE);

	test_cases++;
}

static void test_mixed_content(char *compress_type)
{
	vbf_file_delete(TEST_RECOVERED_FILE);
	vbf_file_delete(TEST_DATA_FILE);

	create_data_file(TEST_BASE_FILE, TEST_DATA_FILE, compress_type, 256, 16 * 1024 * 1024);
	recover_base_file(TEST_DATA_FILE, TEST_RECOVERED_FILE, compress_type);
	compare_files(TEST_BASE_FILE, TEST_RECOVERED_FILE);

	vbf_file_delete(TEST_RECOVERED_FILE);
	vbf_file_delete(TEST_DATA_FILE);

	test_cases++;
}

int main(int argc, char *argv[])
{
	VBF_UNUSED(argc);
	VBF_UNUSED(argv);

	srand(time(0));

	vbf_file_delete(TEST_BASE_FILE);
	create_base_file(TEST_BASE_FILE);

	fprintf(stderr, "*** Running create tests **\n");
	test_create();

	fprintf(stderr, "*** Running uncompressed small content tests **\n");
	test_small_content("none");
	fprintf(stderr, "*** Running uncompressed large content tests **\n");
	test_large_content("none");
	fprintf(stderr, "*** Running uncompressed mixed content tests **\n");
	test_mixed_content("none");

	fprintf(stderr, "*** Running zlib small content tests **\n");
	test_small_content("zlib");
	fprintf(stderr, "*** Running zlib large content tests **\n");
	test_large_content("zlib");
	fprintf(stderr, "*** Running zlib mixed content tests **\n");
	test_mixed_content("zlib");

	fprintf(stderr, "*** Running zstd small content tests **\n");
	test_small_content("zstd");
	fprintf(stderr, "*** Running zstd large content tests **\n");
	test_large_content("zstd");
	fprintf(stderr, "*** Running zstd mixed content tests **\n");
	test_mixed_content("zstd");

	fprintf(stderr, "==================================================\n");
	fprintf(stderr,
		"Finished running %d reader test cases successfully \n",
		test_cases);
	fprintf(stderr, "==================================================\n");

	vbf_file_delete(TEST_BASE_FILE);

	return EXIT_SUCCESS;
}
