/* 
 * SAS_write - mimicking the "write" side of sas
 *
 * This program is mimicking the actual SAS backend, it opens a socket and 
 * starts listening on that socket to send data to the database.
 *
 * When data a connection is received this starts reading data from stdin
 * and writing data to the socket.
 *
 * The real SAS backend will want to do something more intelligent.
 *
 */
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>

/* Define a 1K buffer, probably should be larger in practice */
#define BUFFER_SIZE 1024
char buffer[BUFFER_SIZE];

void sighandler(int sig);

int main(int argc, char *argv[])
{
	int sockfd, newsockfd, portno;
	struct sockaddr_in serv_addr, cli_addr;
	socklen_t serv_len = sizeof(serv_addr);
	socklen_t cli_len  = sizeof(cli_addr);
	int n, total;

	(void) signal(SIGHUP, sighandler);
	(void) signal(SIGINT, sighandler);
	(void) signal(SIGQUIT, sighandler);
	(void) signal(SIGPIPE, sighandler);

	/* Get the port number from the arguments */
	if (argc != 2) 
	{
		fprintf(stderr,"usage: %s <port-number>\n", argv[0]);
		exit(1);
	}
	portno = atoi(argv[1]);

	/* Open a socket and bind to the port */
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) 
	{
		fprintf(stderr, "Error opening socket\n");
		exit(1);
	}
	bzero((char *) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons( (int16_t) portno);
	serv_len = sizeof(serv_addr);
	if (bind(sockfd, (struct sockaddr *) &serv_addr, serv_len) < 0)
	{
		fprintf(stderr, "ERROR binding socket\n");
		exit(1);
	}

	/* Listen on the socket */
	fprintf(stderr, "Listening on port %d...\n", portno);
	listen(sockfd, 5);
	newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &cli_len);
	if (newsockfd < 0)
	{
		fprintf(stderr, "ERROR on accept\n");
		exit(1);
	}

	fprintf(stderr, "Connection established\n");

	/* Read data until the connection is closed */
	total = 0;
	do 
	{
		n = fread(buffer, sizeof(char), BUFFER_SIZE, stdin);
		fprintf(stderr, "%s: read %d bytes\n", argv[0], n);

		if (n > 0)
		{
			char *buf     = buffer;
			int remaining = n;

			while (remaining > 0)
			{
				int written = write(newsockfd, buf, remaining);
				if (written < 0)
				{
					fprintf(stderr, "%s: error writing to socket\n", argv[0]);
					exit(1);
				}
				total += written;
				buf += written;
				remaining -= written;
			}
		}
	}
	while (n > 0);
	
	fprintf(stderr, "%s: wrote %d bytes\n", argv[0], total);

	/* Close the socket */
	if (shutdown(newsockfd, 2) < 0)
	{
		fprintf(stderr, "%s: error closing socket\n", argv[0]);
		fprintf(stderr, "%s (errno = %d)\n", strerror(errno), errno);
		exit(1);
	}
	
	close(newsockfd);
	close(sockfd);

	return 0; 
}


void sighandler(int sig)
{
	fprintf(stderr, "\nInterupped by signal %d\n", sig);
	exit(sig);
}
