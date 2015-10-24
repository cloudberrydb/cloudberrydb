/*
 * SAS Receiver - Naive executable to receive binary output from SAS
 *
 * The intended use case for this program is to be the endpoint of an external
 * readable table:
 *
 *    CREATE EXTERNAL WEB TABLE example_in(like example)
 *      EXECUTE '$GPHOME/bin/sasreceiver localhost 2000'
 *      FORMAT 'CUSTOM' (FORMATTER = sas_input_formatter)
 *      DISTRIBUTED BY (id);
 *
 * This program is launched with data is inserted into the external table:
 *   1) Read the host name and port base from the command line
 *   2) Read the segment id from the environment
 *   3) Add the segment id to the base port
 *   4) Open a socket to the hostname on the derived port
 *   5) Read from the socket and write to stdin
 *   6) Exit when the socket is closed
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/errno.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <unistd.h>


#define BUFFER_SIZE 1024

#define DEBUG

FILE *logfile = NULL;

void sighandler(int sig);

int main(int argc, char *argv[])
{
    int sockfd, portno, segno, n, total;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    char buffer[BUFFER_SIZE];
	char *hostname;
	char *env_str;
	char *endptr;

	/* Open file for logging, otherwise log to stderr */
#ifdef DEBUG
	logfile = fopen("/tmp/sasreceiver.log", "a+");
	if (logfile == NULL)
	{
		fprintf(stderr, "%s: error opening logfile\n", argv[0]);
		fprintf(stderr, "%s (errno = %d)\n", strerror(errno), errno);
		exit(0);
	}

	(void) signal(SIGHUP, sighandler);
	(void) signal(SIGINT, sighandler);
	(void) signal(SIGQUIT, sighandler);
	(void) signal(SIGPIPE, sighandler);
#else
	logfile = stderr;
#endif

	/* Verify the correct number of arguments */
    if (argc != 3) 
	{
       fprintf(logfile,"usage %s <host_name> <port_base>\n", argv[0]);
	   fflush(logfile);
       exit(1);
    }
	hostname = argv[1];

	/* Convert the specified port into a number */
	portno = strtol(argv[2], &endptr, 10);
	if (*endptr != '\0')
	{
		fprintf(logfile, "%s: invalid port base: %s\n", argv[0], argv[2]);
		fflush(logfile);
		exit(1);
	}

	/* Read the $GP_SEGMENT_ID from the environment and add to the portno */
	env_str = getenv("GP_SEGMENT_ID");
	if (!env_str)
	{
		fprintf(logfile, "%s: $GP_SEGMENT_ID is not set\n", argv[0]);
		fflush(logfile);
		exit(1);
	}
	segno = strtol(env_str, &endptr, 10);
	if (*endptr != '\0')
	{
		fprintf(logfile, "%s: invalid $GP_SEGMENT_ID: %s\n", argv[0], env_str);
		fflush(logfile);
		exit(1);
	}

	/* Debugging information regarding ports */
	fprintf(logfile, "%s: port base = %d\n", argv[0], portno);
	fprintf(logfile, "%s: segment id = %d\n", argv[0], segno);
	portno += segno;
	fprintf(logfile, "%s: port = %d\n", argv[0], portno);
	fflush(logfile);
	

	/* Open a socket */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
	{
		fprintf(logfile, "%s: error opening socket", argv[0]);
		fflush(logfile);
		exit(1);
	}
    server = gethostbyname(hostname);
    if (server == NULL) 
	{
        fprintf(logfile, "%s: no such host: %s\n", argv[0], hostname);
		fflush(logfile);
        exit(1);
    }
	memset((char *) &serv_addr, '\0', sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, 
		   server->h_length);
    serv_addr.sin_port = htons(portno);
    if (connect(sockfd, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0) 
	{
		fprintf(logfile, "%s: error connecting\n", argv[0]);
		fflush(logfile);
		exit(1);
	}
	fprintf(logfile, "connection established\n");
	fflush(logfile);

	/* Loop, reading from socket, writing to stdin until socket is closed */
	total = 0;
	do 
	{
		n = read(sockfd, buffer, BUFFER_SIZE);
		total += n;

		/* write the read data back out to stdout */
		if (n > 0)
		{
			char *buf       = buffer;
			int   remaining = n;
			while (remaining > 0)
			{
				int written = fwrite(buf, 1, remaining, stdout);
				if (written < 0)
				{
					fprintf(stderr, "%s: error writing to stdout\n", argv[0]);
					exit(1);
				}
				buf       += written;
				remaining -= written;
			}
		}
	}
	while (n > 0);

	if (n < 0)
	{
		fprintf(logfile, "%s: error reading from socket\n", argv[0]);
		fprintf(logfile, "%s (errno = %d)\n", strerror(errno), errno);
		fflush(logfile);
		exit(1);
	}

	fprintf(logfile, "%s: read %d bytes\n", argv[0], total);
	fflush(logfile);

	close(sockfd);
	
    return 0;
}


void sighandler(int sig)
{
	if (logfile)
	{
		fprintf(logfile, "\nInterupped by signal %d\n", sig);
		fclose(logfile);
	}
	exit(sig);
}
