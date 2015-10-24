/*-------------------------------------------------------------------------
 *
 * varblocktest.c
 *		Test the VarBlock module (cdbvarbock.c and .h).
 *
 * Usage: varblocktest [options] < input >output
 *
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>
#include <getopt_long.h>

#include "cdb/cdbvarblock.h"

#include "varblocktest.h"

// static int		binaryFd;		/* binary FD for current file */

/* command-line parameters */
static bool 		make = false; /* when true we are doing make */
static bool 		random_flag = false; /* when true we are doing random bit attack */
typedef enum RandomFlavor
{
	RANDOM_NONE,
	RANDOM_HEADER,
	RANDOM_OFFSETS,
} RandomFlavor;
static RandomFlavor random_flavor = RANDOM_NONE;
static char         *random_flavor_str;
static int			blocksize = 320000;
static bool 		detail_flag = false; /* when true display a lot of detail */
static bool 		varblock_flag = false; /* when true display a VarBlock statistics */

#define MAX_LINEBUFFER_LEN 6000 
static uint8		lineBuffer[MAX_LINEBUFFER_LEN+1];
static bool			endOfInput = false;
static int          lineLength = 0;

#define MAX_VARBLOCKTEST_LEN 320000 
static uint8        varBlockBuffer[MAX_VARBLOCKTEST_LEN];

#define MAX_VARBLOCK_TEMPSPACE_LEN 4098 
static uint8 tempSpace [MAX_VARBLOCK_TEMPSPACE_LEN];

static uint8		tempLineBuffer[MAX_LINEBUFFER_LEN+1];


/*
 * Return random integer in the range 0 .. max - 1.
 */
static int32
getRandomInt(int32 max)
{
	long r;

	r = random();

	return r % max;
}

static void
attackRandomBit(
	uint8 *buffer,
	int32 bufferLen,
	bool turnOn)
{
	int32 bitLen;
	int32 randomBit;
	int32 byteOffset;
	int bitInByte;
	uint8 mask;

	bitLen = bufferLen * 8;

	while (true)
	{
		randomBit = getRandomInt(bitLen);

		byteOffset = randomBit / 8;
		bitInByte = randomBit % 8;

		mask = (1 << bitInByte);
		if (turnOn)
		{
			/*
			 * If the bit is already on, skip.
			 */
			if ((buffer[byteOffset] & mask) != 0)
			{
				continue;
			}
			buffer[byteOffset] |= mask;
		}
		else
		{
			/*
			 * If the bit is already off, skip.
			 */
			if ((buffer[byteOffset] & mask) == 0)
			{
				continue;
			}
			buffer[byteOffset] &= ~mask;
		}
		break;
	}
	
	fprintf(stderr,"Attacking bit (turn %s bit %d of byte %d)\n",
		    (turnOn ? "on" : "off"), bitInByte, byteOffset);
}

static void
doRandomBitAttack(
	int32 bufferLen)
{
	int32 onOff;
	VarBlockCheckError varBlockCheckError;

	onOff = getRandomInt(2);
	if (random_flavor == RANDOM_HEADER)
		attackRandomBit(varBlockBuffer, VARBLOCK_HEADER_LEN, (onOff == 1));
	else if (random_flavor == RANDOM_OFFSETS)
	{
		VarBlockHeader *header;
		int32 firstByteOffset;
		int32 range;

		header = (VarBlockHeader*)varBlockBuffer;
		firstByteOffset = VARBLOCK_HEADER_LEN + VarBlockGet_itemLenSum(header);

		range = bufferLen - firstByteOffset;

		attackRandomBit(varBlockBuffer + firstByteOffset, range, (onOff == 1));
	}
	else
		Assert(false);

	varBlockCheckError = VarBlockIsValid(varBlockBuffer, bufferLen);
	if (varBlockCheckError != VarBlockCheckOk)
	{
		fprintf(stderr,"Random bit attack generated error %d '%s'\n",
			    varBlockCheckError, VarBlockGetCheckErrorStr());
	}
	else
		{
		fprintf(stderr,"Random bit attack did not generate a check error\n");
		}
}

static void
doReadLine(void)
{
	int c = '*';
	int n = 0;

	while (true)
	{
		c = getc(stdin);
		if (c == EOF)
		{
			endOfInput = true;
			break;
		}
		if (c == '\n')
			break;
		if (n >= MAX_LINEBUFFER_LEN)
		{
			fprintf(stderr, "Line too long %d",n);
			exit(1);
		}
		lineBuffer[n] = c;
		n++;
	}
	lineBuffer[n] = '\0';
	lineLength = n;
}

static void
printVarBlockHeader(FILE* where, VarBlockReader *varBlockReader)
{
	int multiplier;
	VarBlockByteLen offsetArrayLen;
	
	fprintf(where,"VarBlockReader:\n");
	fprintf(where,"    header: (%p)\n", varBlockReader->header);
	fprintf(where,"        offsetsAreSmall: %s\n", (VarBlockGet_offsetsAreSmall(varBlockReader->header) ? "true" : "false"));
	fprintf(where,"        version: %d\n", VarBlockGet_version(varBlockReader->header));
	fprintf(where,"        itemLenSum: %d\n", VarBlockGet_itemLenSum(varBlockReader->header));
	fprintf(where,"        itemCount: %d\n", VarBlockGet_itemCount(varBlockReader->header));
	fprintf(where,"    bufferLen: %d\n", varBlockReader->bufferLen);
	fprintf(where,"    nextIndex: %d\n", varBlockReader->nextIndex);
	fprintf(where,"    offsetToOffsetArray: %d\n", varBlockReader->offsetToOffsetArray);
	fprintf(where,"    nextItemPtr: %p\n", varBlockReader->nextItemPtr);

	multiplier = (VarBlockGet_offsetsAreSmall(varBlockReader->header) ? 2 : 3);
	offsetArrayLen = VarBlockGet_itemCount(varBlockReader->header) * multiplier;
	fprintf(where,"    offsetArrayLen: %d\n", offsetArrayLen);
}


static void
doPrintVarBlock(int bufferLen)
{
	VarBlockByteLen varBlockLen;
    VarBlockReader  myVarBlockReader;
	int itemCount;
    uint8 *itemPtr;
	int itemLen;
	int readerItemCount;

	varBlockLen = VarBlockLenFromHeader(varBlockBuffer, VARBLOCK_HEADER_LEN);
	if (varBlockLen != bufferLen)
	{
		fprintf(stderr,"VarBlockLenFromHeader returned %d, expected bufferLen %d\n",
			    varBlockLen, bufferLen);
		exit(1);
	}

	Assert(varBlockLen == bufferLen);


    VarBlockReaderInit(
        &myVarBlockReader,
        varBlockBuffer,
        bufferLen);
	
	if (varblock_flag)
	{
		printVarBlockHeader(stderr,&myVarBlockReader);
	}
	
	itemCount = 0;
	readerItemCount = VarBlockReaderItemCount(&myVarBlockReader);

	if (detail_flag)
	{
		printVarBlockHeader(stdout,&myVarBlockReader);
	}

	while (true)
	{
		itemPtr = VarBlockReaderGetNextItemPtr(&myVarBlockReader,&itemLen);
		if (itemPtr == NULL)
		{
			break;
		}
		if (detail_flag)
		{
			fprintf(stdout, "Reader item count %d, item ptr %p, item length %d\n",
				    itemCount, itemPtr, itemLen);
		}
		if (itemLen > 0)
		{
			memcpy(tempLineBuffer,itemPtr,itemLen);
			tempLineBuffer[itemLen] = '\0';
			if (detail_flag)
			{
				fprintf(stdout, "Reader: '%s'\n", tempLineBuffer);
			}
			else
			{
				fprintf(stdout, "%s\n", tempLineBuffer);
			}
		}
		itemCount++;
	}
	if (readerItemCount != itemCount)
	{
		fprintf(stderr, "Reader count %d, found %d items\n",readerItemCount,itemCount);
		exit(1);
	}
}
	
static void
doMake(char* fname)
{
    VarBlockMaker  myVarBlockMaker;
	int itemCount;
    uint8 *itemPtr;
	int actualItemCount;
	int bufferLen;

    VarBlockMakerInit(
        &myVarBlockMaker,
        varBlockBuffer,
        blocksize,
        tempSpace,
        MAX_VARBLOCK_TEMPSPACE_LEN);
	itemCount = 0;
	
	while (true)
	{
		if (detail_flag)
		{
			fprintf(stdout,"VarBlockMaker:\n");
			fprintf(stdout,"    header: (%p)\n", myVarBlockMaker.header);
			fprintf(stdout,"        offsetsAreSmall: %s\n", (VarBlockGet_offsetsAreSmall(myVarBlockMaker.header) ? "true" : "false"));
			fprintf(stdout,"        version: %d\n", VarBlockGet_version(myVarBlockMaker.header));
			fprintf(stdout,"    maxBufferLen: %d\n", myVarBlockMaker.maxBufferLen);
			fprintf(stdout,"    currentItemLenSum: %d\n", myVarBlockMaker.currentItemLenSum);
			fprintf(stdout,"    nextItemPtr: %p\n", myVarBlockMaker.nextItemPtr);
			fprintf(stdout,"    currentItemCount: %d\n", myVarBlockMaker.currentItemCount);
			fprintf(stdout,"    maxItemCount: %d\n", myVarBlockMaker.maxItemCount);
		}
	
		doReadLine();
		if (!endOfInput || lineLength > 0)
		{
			if (detail_flag)
			{
				fprintf(stdout, "Line: '%s', length %d\n", lineBuffer, lineLength);
			}
			itemPtr = VarBlockMakerGetNextItemPtr(&myVarBlockMaker,lineLength);
			if (itemPtr == NULL)
			{
				actualItemCount = VarBlockMakerItemCount(&myVarBlockMaker);
				Assert(actualItemCount == itemCount);
				if (itemCount == 0)
				{
					fprintf(stderr, "Item too long (#1)\n");
					exit(1);
				}
				bufferLen = VarBlockMakerFinish(&myVarBlockMaker);	
				if (detail_flag)
				{
					fprintf(stdout, 
						    "Finished VarBlock (length = %d, item count = %d)\n",
						    bufferLen, itemCount);
				}
				if (random_flag)
					doRandomBitAttack(bufferLen);
				else
					doPrintVarBlock(bufferLen);
				
			    VarBlockMakerReset(&myVarBlockMaker);
				itemCount = 0;
				itemPtr = VarBlockMakerGetNextItemPtr(&myVarBlockMaker,lineLength);
				if (itemPtr == NULL)
				{
					fprintf(stderr, "Item too long (#2)\n");
					exit(1);
				}
			}

			itemCount++;

			Assert(itemPtr != NULL);
			if (detail_flag)
			{
				fprintf(stdout, 
					    "Copy into pointer %p\n",
					    itemPtr);
			}
			
			if (lineLength > 0)
				memcpy(itemPtr,lineBuffer,lineLength);
				
		}

		if (endOfInput)
			break;
	}

	actualItemCount = VarBlockMakerItemCount(&myVarBlockMaker);
	Assert(actualItemCount == itemCount);
	if (itemCount > 0)
	{
		bufferLen = VarBlockMakerFinish(&myVarBlockMaker);	
		if (detail_flag)
		{
			fprintf(stdout, 
				    "Finished final VarBlock (length = %d, item count = %d)\n",
				    bufferLen, itemCount);
		}
		doPrintVarBlock(bufferLen);
	}
}

/* 
 * Exit closing files
 */
static void
exit_gracefuly(int status)
{
//	close(binaryFd);
	exit(status);
}


static void
help(void)
{
	printf("varblocktest ... \n\n");
	printf("Usage:\n");
	printf("  varblocktest [OPTION]... \n");
	printf("\nOptions controlling the output content:\n");
	printf("  -m, --make                Take standard input and create a file with VarBlocks.\n"); 
	printf("  -r, --random              Do random bit attack.\n");
	printf("  -b, --blocksize           The VarBlock size in bytes.\n");
	printf("  -k, --kilobytes           The VarBlock size in kilobytes.\n");
	printf("  -d, --detail              Display detailed information.\n");
	printf("  -v, --varblock            Display VarBlock information.\n");
	exit(0);
}

int
main(int argc, char** argv)
{
	int	c, optindex;

	static struct option long_options[] = {
		{"make", no_argument, NULL, 'm'},
		{"random", required_argument, NULL, 'r'},
		{"blocksize", required_argument, NULL, 'b'},
		{"kilobytes", required_argument, NULL, 'k'},
		{"detail_flag", no_argument, NULL, 'd'},
		{"varblock", no_argument, NULL, 'v'},
		{"help", no_argument, NULL, '?'},
		{NULL, 0, NULL, 0}
	};
	
	if (argc == 1 || !strcmp(argv[1], "--help") || !strcmp(argv[1], "-?"))
		help();

	while ((c = getopt_long(argc, argv, "mr:bkdv",
							long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'm':			/* make */
				make = true;
				break;
			case 'r':			
				random_flag = true;	/* random */
				random_flavor_str = optarg;
				if (random_flavor_str == NULL)
					random_flavor = RANDOM_HEADER;
				else
				{
					int len;
					
					len = strlen(random_flavor_str);
					if (len == strlen("header") &&
						strcmp(random_flavor_str, "header") == 0)
						random_flavor = RANDOM_HEADER;
					else if (len == strlen("offsets") &&
						strcmp(random_flavor_str, "offsets") == 0)
					{
						random_flavor = RANDOM_OFFSETS;
					}
					else
					{
						fprintf(stderr,"Unknown random flavor '%s'\n", random_flavor_str);
						exit(1);
					}
				}
				break;
			case 'b':			/* blocksize */
				blocksize = atoi(optarg);
				break;
			case 'k':			/* blocksize in kilobytes */
				blocksize = 1024 * atoi(optarg);
				break;
			case 'd':			
				detail_flag = true;	/* detail */
				break;
			case 'v':			
				varblock_flag = true;	/* summary */
				break;
			default:
				fprintf(stderr, "Try \"varblocktest --help\" for more information.\n");
				exit(1);
		}
	}

	if (blocksize > MAX_VARBLOCKTEST_LEN)
	{
		fprintf(stderr, "Blocksize %d greater than maximum.\n", blocksize);
		exit(1);
	}
				
	if (make)
	{

/*
		char *fname;
		
		if (optind >= argc)
		{
			fprintf(stderr, "Missing file specification.\n");
			exit(1);
		}
		
		fname = argv[optind];
		binaryFd = open(fname, O_WRONLY | PG_BINARY | O_CREAT, 0);

		if (binaryFd < 0)
		{
			perror(fname);
			exit(1);
		}

*/
		doMake(NULL);
		exit_gracefuly(0);
	}

/*
	if (reader)
	{
		char *fname;
		
		if (optind >= argc)
		{
			fprintf(stderr, "Missing file specification.\n");
			exit(1);
		}
		
		fname = argv[optind];
		binaryFd = open(fname, O_RDONLY | PG_BINARY, 0);

		if (binaryFd < 0)
		{
			perror(fname);
			exit(1);
		}
		doReader(fname);
		exit_gracefuly(0);
	}
*/
	exit_gracefuly(0);
	
	/* just to avoid a warning */
	return 0;
}

/*
 * Routines needed if headers were configured for ASSERT
 */
#ifndef assert_enabled
bool		assert_enabled = true;
#endif

int
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int         lineNumber)
{
	fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
			errorType, conditionName,
			fileName, lineNumber);

	abort();
	return 0;
}
